package main

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// Set on a request that has already been sent to the instance that was
// supposed to own the job. It stops two servers with disagreeing ideas of who
// owns a job from bouncing a request between them forever.
const forwardedHeader = "X-MMseqs-Forwarded"

// OwnerRouter sends a request for a job to the instance that holds its files.
//
// With one shared jobs volume any server could read any job, and the service
// in front of them could hand a request to any pod. With per-instance volumes
// that is no longer true, so the request goes to the owner instead of the
// files coming to the request.
type OwnerRouter struct {
	tracker   JobTracker
	token     string
	transport http.RoundTripper
}

func MakeOwnerRouter(tracker JobTracker, config ConfigRoot) *OwnerRouter {
	return &OwnerRouter{
		tracker: tracker,
		token:   config.Instances.Token,
		transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
		},
	}
}

// Route wraps a handler that reads job files from the local results directory.
func (r *OwnerRouter) Route(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		id := Id(mux.Vars(req)["ticket"])
		if validId(string(id)) == false {
			http.Error(w, "Invalid ID", http.StatusBadRequest)
			return
		}

		owner, err := r.tracker.JobOwner(id)
		if err != nil {
			// Redis is not answering. The job may well be ours, and failing
			// here would turn a redis hiccup into a failure for every job.
			log.Print(err)
			handler(w, req)
			return
		}

		// No owner recorded means the job predates the tracker or was never
		// claimed by a worker; either way the local directory is the only
		// place it could be.
		if len(owner) == 0 || owner == r.tracker.InstanceId() {
			handler(w, req)
			return
		}

		if len(req.Header.Get(forwardedHeader)) > 0 {
			log.Println("Refusing to forward " + string(id) + " again, owner is " + owner)
			resultsGone(w)
			return
		}

		address, err := r.tracker.InstanceAddress(owner)
		if err != nil || len(address) == 0 {
			if err != nil {
				log.Print(err)
			}
			resultsGone(w)
			return
		}

		proxy, err := r.proxy(owner, address)
		if err != nil {
			log.Print(err)
			resultsGone(w)
			return
		}

		proxy.ServeHTTP(w, req)
	}
}

func (r *OwnerRouter) proxy(owner string, address string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	token := r.token
	proxy := &httputil.ReverseProxy{
		Transport: r.transport,
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			// The path is left alone: the peer listener of the owner serves
			// the same endpoints under the same prefix.
			req.Host = target.Host
			req.Header.Set(forwardedHeader, "1")
			if len(token) > 0 {
				req.Header.Set("Authorization", "Bearer "+token)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			// The registry entry outlives the process by up to its TTL, and a
			// server whose container died while the volume lives on cannot
			// serve either. Stop trusting the cached address right away.
			r.tracker.ForgetInstance(owner)
			log.Println("Forwarding to instance " + owner + " failed: " + err.Error())
			resultsGone(w)
		},
	}
	return proxy, nil
}

func resultsGone(w http.ResponseWriter) {
	// 404 and not 502: from a client's point of view this ticket no longer
	// exists, and submitting the query again is the way to get results.
	http.Error(w, "Job results are no longer available", http.StatusNotFound)
}

// instanceAuth guards the peer listener. It is optional because the listener
// only exposes read only endpoints, but the listener is bound to an address
// other pods can reach, unlike the public API which sits behind a proxy.
func instanceAuth(token string, handler http.Handler) http.Handler {
	if len(token) == 0 {
		return handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		header := req.Header.Get("Authorization")
		if strings.HasPrefix(header, "Bearer ") == false || strings.TrimPrefix(header, "Bearer ") != token {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, req)
	})
}

// ServePeers starts the listener other instances forward job requests to. It
// is separate from the public listener on purpose: the public one binds
// loopback only in most deployments, so that everything from the outside has
// to come through a proxy that enforces upload limits and authentication.
func ServePeers(config ConfigRoot, handler http.Handler) {
	srv := &http.Server{
		Handler: instanceAuth(config.Instances.Token, handler),
		Addr:    config.Instances.Address,

		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute,
	}

	log.Println("Serving instance requests on " + config.Instances.Address)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
