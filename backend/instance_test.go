package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadInstanceIdIsStable(t *testing.T) {
	dir, err := ioutil.TempDir("", "instance")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	first, err := LoadInstanceId(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 {
		t.Fatal("empty instance id")
	}

	// The server and the worker of one instance read the same directory and
	// have to end up with the same identity.
	second, err := LoadInstanceId(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("instance id changed between reads: %q then %q", first, second)
	}

	// A different results directory is a different instance.
	other, err := ioutil.TempDir("", "instance")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(other)

	third, err := LoadInstanceId(other)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("two results directories share an instance id")
	}
}

// Server and worker start at the same time and both create the file.
func TestLoadInstanceIdConcurrent(t *testing.T) {
	dir, err := ioutil.TempDir("", "instance")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var wg sync.WaitGroup
	ids := make([]string, 8)
	errs := make([]error, 8)
	for i := 0; i < len(ids); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = LoadInstanceId(dir)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("reader %d got %q, reader 0 got %q", i, ids[i], ids[0])
		}
	}
}

func TestAdvertisedAddress(t *testing.T) {
	config := ConfigRoot{}
	config.Redis.Network = "tcp"
	config.Redis.Address = "127.0.0.1:6379"
	config.Instances.Address = ":8082"

	address, err := AdvertisedAddress(config)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(address, "http://") == false || strings.HasSuffix(address, ":8082") == false {
		t.Fatalf("advertised address %q is not a base URL on the configured port", address)
	}
	// A wildcard bind must never be advertised as such.
	if strings.Contains(address, "://:") || strings.Contains(address, "0.0.0.0") {
		t.Fatalf("advertised address %q is not reachable from another host", address)
	}

	config.Instances.Advertise = "10.1.2.3:9000"
	address, err = AdvertisedAddress(config)
	if err != nil {
		t.Fatal(err)
	}
	if address != "http://10.1.2.3:9000" {
		t.Fatalf("advertised address %q, want the configured override", address)
	}
}

func TestAddressCacheExpires(t *testing.T) {
	cache := addressCache{}
	cache.put("a", "http://10.0.0.1:8082")

	if address, ok := cache.get("a"); ok == false || address != "http://10.0.0.1:8082" {
		t.Fatalf("cache miss right after put: %q %v", address, ok)
	}
	if _, ok := cache.get("b"); ok {
		t.Fatal("cache hit for an instance that was never put")
	}

	cache.forget("a")
	if _, ok := cache.get("a"); ok {
		t.Fatal("cache hit after forget")
	}

	cache.entries["c"] = cachedAddress{"http://10.0.0.2:8082", time.Now().Add(-2 * instanceCacheTTL)}
	if _, ok := cache.get("c"); ok {
		t.Fatal("cache hit for a stale entry")
	}
}

func TestJanitorSweep(t *testing.T) {
	dir, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	old := testId("old")
	mine := testId("mine")
	theirs := testId("theirs")
	running := testId("running")
	fresh := testId("fresh")

	jobsystem := &fakeJobSystem{
		instance: "self",
		status: map[Id]Status{
			old:     StatusComplete,
			mine:    StatusComplete,
			theirs:  StatusComplete,
			running: StatusRunning,
			fresh:   StatusComplete,
		},
		owners: map[Id]string{
			mine:    "self",
			theirs:  "other",
			running: "self",
		},
	}

	stale := time.Now().Add(-2 * time.Hour)
	for _, id := range []Id{old, mine, theirs, running} {
		makeJobDir(t, dir, id, stale)
	}
	makeJobDir(t, dir, fresh, time.Now())
	// Not a job directory, and the identity of this instance: never touched.
	if err := ioutil.WriteFile(filepath.Join(dir, instanceIdFile), []byte("self\n"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := sweep(jobsystem, dir, time.Hour, 0); err != nil {
		t.Fatal(err)
	}

	for _, id := range []Id{old, mine, theirs} {
		if _, err := os.Stat(filepath.Join(dir, string(id))); os.IsNotExist(err) == false {
			t.Fatalf("%s was not deleted", id)
		}
	}
	for _, id := range []Id{running, fresh} {
		if _, err := os.Stat(filepath.Join(dir, string(id))); err != nil {
			t.Fatalf("%s should have been kept: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, instanceIdFile)); err != nil {
		t.Fatalf("the instance id file was deleted: %v", err)
	}

	// Keys are dropped for the jobs this instance owns, and only those: the
	// directory of a job owned by somebody else is a leftover from a requeue,
	// and the instance running it now still needs its keys.
	if jobsystem.forgotten[mine] == false {
		t.Fatal("keys of an owned job were kept")
	}
	if jobsystem.forgotten[theirs] {
		t.Fatal("keys of a job owned by another instance were dropped")
	}
	if jobsystem.forgotten[running] {
		t.Fatal("keys of a running job were dropped")
	}
}

// An age limit is a retention policy, not a bound on the volume: a busy hour
// writes as much as it writes. The size limit is what keeps the disk from
// filling, so it has to evict jobs that are nowhere near old enough.
func TestJanitorSweepEnforcesSizeLimit(t *testing.T) {
	dir, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	oldest := testId("oldest")
	middle := testId("middle")
	newest := testId("newest")

	jobsystem := &fakeJobSystem{
		instance: "self",
		status: map[Id]Status{
			oldest: StatusComplete,
			middle: StatusComplete,
			newest: StatusComplete,
		},
		owners: map[Id]string{
			oldest: "self",
			middle: "self",
			newest: "self",
		},
	}

	now := time.Now()
	for i, id := range []Id{oldest, middle, newest} {
		makeJobDir(t, dir, id, now.Add(-time.Duration(10-i)*time.Minute))
		padJobDir(t, dir, id, 4096)
	}

	// Everything is minutes old, so only the size limit can bite. Room for
	// two of the three directories: the oldest has to go, the others stay.
	if err := sweep(jobsystem, dir, time.Hour, 9000); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, string(oldest))); os.IsNotExist(err) == false {
		t.Fatal("the oldest job was kept while the directory was over its size limit")
	}
	for _, id := range []Id{middle, newest} {
		if _, err := os.Stat(filepath.Join(dir, string(id))); err != nil {
			t.Fatalf("%s was deleted after the limit was already met: %v", id, err)
		}
	}
	if jobsystem.forgotten[oldest] == false {
		t.Fatal("keys of an evicted job were kept")
	}
}

func makeJobDir(t *testing.T, base string, id Id, modtime time.Time) {
	t.Helper()
	dir := filepath.Join(base, string(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "job.json"), []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(dir, modtime, modtime); err != nil {
		t.Fatal(err)
	}
}

func padJobDir(t *testing.T, base string, id Id, size int) {
	t.Helper()
	dir := filepath.Join(base, string(id))
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "alis_pdb"), make([]byte, size), 0644); err != nil {
		t.Fatal(err)
	}
	// Writing into the directory moved its mtime, and the sweep orders by it.
	if err := os.Chtimes(dir, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
}
