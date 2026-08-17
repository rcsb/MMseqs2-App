package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// An instance is one results directory together with the server and worker
// processes that share it. It is the unit this whole feature is about: with
// per-instance job volumes, the files of a job live on exactly one instance,
// and redis is what remembers which one, so that a server that did not run a
// job can still answer for it.
//
// The identity is deliberately not derived from anything Kubernetes-specific
// (pod name, downward API, hostname): it is a random id stored in the results
// directory itself. That is the correct scope, because the results directory
// is what actually holds the jobs -- two processes sharing the directory are
// one instance, and when the directory is thrown away (an ephemeral volume,
// a wiped work dir, a fresh container) the identity goes with it, which is
// exactly the moment the jobs it owned stopped existing.
const instanceIdFile = "instance.id"

const (
	// How long the registry entry survives without a refresh. Once it is gone,
	// every job owned by the instance is considered lost, so this has to be
	// comfortably longer than the refresh interval to survive a slow redis.
	InstanceTTL = 30 * time.Second
	// How often a server refreshes its registry entry.
	InstanceRefresh = 10 * time.Second
	// How long a looked up instance address is reused before asking redis
	// again. Status polls are the hottest path in the API, and every one of
	// them resolves an owner, so this keeps that from becoming a redis round
	// trip per poll per client.
	instanceCacheTTL = 5 * time.Second
)

func newInstanceId() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// LoadInstanceId returns the id of the results directory at base, creating it
// on first use. Server and worker start concurrently and race here, so the
// file is created exclusively and the loser of the race re-reads it.
func LoadInstanceId(base string) (string, error) {
	path := filepath.Join(filepath.Clean(base), instanceIdFile)

	for i := 0; i < 5; i++ {
		data, err := ioutil.ReadFile(path)
		if err == nil {
			id := strings.TrimSpace(string(data))
			if len(id) > 0 {
				return id, nil
			}
			// A zero length file means another process created it but has not
			// written to it yet. Give it a moment rather than stealing the id.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if os.IsNotExist(err) == false {
			return "", err
		}

		id, err := newInstanceId()
		if err != nil {
			return "", err
		}

		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return "", err
		}
		_, err = f.WriteString(id + "\n")
		if err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
		return id, nil
	}

	return "", errors.New("Could not determine the instance id of " + base)
}

// AdvertisedAddress is the base URL other instances use to reach this one.
//
// It is worked out without asking Kubernetes for anything: unless it is
// configured explicitly, the host is whichever local address the kernel would
// use to reach redis, which is by definition an address that is routable on
// the network the instances share.
func AdvertisedAddress(config ConfigRoot) (string, error) {
	if len(config.Instances.Advertise) > 0 {
		return normalizeAddress(config.Instances.Advertise)
	}

	host, port, err := net.SplitHostPort(config.Instances.Address)
	if err != nil {
		return "", err
	}
	if len(port) == 0 {
		return "", errors.New("instances.address needs a port")
	}

	if isWildcardHost(host) {
		host, err = outboundIP(config.Redis.Network, config.Redis.Address)
		if err != nil {
			return "", err
		}
	}

	return "http://" + net.JoinHostPort(host, port), nil
}

func isWildcardHost(host string) bool {
	if len(host) == 0 {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// outboundIP asks the kernel which of our addresses would be used to talk to
// peer. For UDP this sets up the route without sending anything.
func outboundIP(network string, peer string) (string, error) {
	if network == "unix" {
		return localIP()
	}

	conn, err := net.Dial("udp", peer)
	if err != nil {
		// A redis on a unix socket or an unresolvable name leaves us to guess.
		return localIP()
	}
	defer conn.Close()

	host, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return "", err
	}
	return host, nil
}

func localIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() || ipnet.IP.To4() == nil {
			continue
		}
		return ipnet.IP.String(), nil
	}
	return "", errors.New("Could not determine an address to advertise, set instances.advertise")
}

func normalizeAddress(address string) (string, error) {
	if strings.Contains(address, "://") == false {
		address = "http://" + address
	}
	u, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	if len(u.Host) == 0 {
		return "", errors.New("Invalid instances.advertise: " + address)
	}
	return strings.TrimSuffix(u.Scheme+"://"+u.Host+u.Path, "/"), nil
}

// JobTracker is the part of a job system that knows which instance holds the
// files of a job. Only the redis job system implements it; in local mode
// there is a single instance and nothing to track.
type JobTracker interface {
	// Tracking reports whether per-instance job directories are in use.
	Tracking() bool
	// InstanceId is the id of the results directory of this process.
	InstanceId() string
	// Announce keeps this instance's address in the registry until stop is
	// closed, and removes it on the way out.
	Announce(stop <-chan struct{})
	// InstanceAlive reports whether the given instance is still registered.
	InstanceAlive(instance string) bool
	// InstanceAddress returns the base URL of an instance, or "" if it is gone.
	InstanceAddress(instance string) (string, error)
	// ForgetInstance drops a cached address, so that the next lookup asks
	// redis again instead of waiting out the cache.
	ForgetInstance(instance string)
	// JobOwner returns the instance holding the files of a job, "" if unknown.
	JobOwner(id Id) (string, error)
	// ClaimJob records this instance as the owner of a job.
	ClaimJob(id Id) error
	// LoadJobRequest returns the job input, which is what a worker rebuilds
	// the job directory from.
	LoadJobRequest(id Id) (JobRequest, error)
	// Forget drops every trace of a job: a resubmission then runs it again.
	Forget(id Id) error
}

// Tracker returns the job tracker of a job system, if it has one and it is
// enabled.
func Tracker(jobsystem JobSystem) (JobTracker, bool) {
	tracker, ok := jobsystem.(JobTracker)
	if ok == false || tracker.Tracking() == false {
		return nil, false
	}
	return tracker, true
}

type cachedAddress struct {
	address string
	read    time.Time
}

type addressCache struct {
	mutex   sync.Mutex
	entries map[string]cachedAddress
}

func (c *addressCache) get(instance string) (string, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	entry, ok := c.entries[instance]
	if ok == false || time.Since(entry.read) > instanceCacheTTL {
		return "", false
	}
	return entry.address, true
}

func (c *addressCache) put(instance string, address string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]cachedAddress)
	}
	c.entries[instance] = cachedAddress{address, time.Now()}
}

func (c *addressCache) forget(instance string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.entries, instance)
}

// unannounceOnSignal takes this instance out of the registry when the process
// is asked to stop. The entry would otherwise survive for the rest of its TTL,
// and everything forwarded here in the meantime fails.
func unannounceOnSignal(stop chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	<-sigs
	close(stop)
	// let Announce delete the entry before the process goes away
	time.Sleep(500 * time.Millisecond)
	os.Exit(0)
}

// waitForInstance blocks until this instance is registered, meaning its server
// is up and reachable. A worker that ran jobs before that would put results on
// a volume nobody can read, so it waits instead.
func waitForInstance(tracker JobTracker) {
	warned := false
	for tracker.InstanceAlive(tracker.InstanceId()) == false {
		if warned == false {
			log.Println("Waiting for the server of instance " + tracker.InstanceId() + " to register")
			warned = true
		}
		time.Sleep(time.Second)
	}
}
