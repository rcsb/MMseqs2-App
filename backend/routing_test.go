package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// Ids have to look like a real ticket or validId rejects them.
func testId(seed string) Id {
	padded := seed + strings.Repeat("x", 38)
	return Id(padded[:38])
}

type fakeJobSystem struct {
	instance  string
	status    map[Id]Status
	owners    map[Id]string
	addresses map[string]string
	forgotten map[Id]bool
	dropped   []string
}

func (f *fakeJobSystem) SetStatus(id Id, status Status) error {
	if f.status == nil {
		f.status = make(map[Id]Status)
	}
	f.status[id] = status
	return nil
}

func (f *fakeJobSystem) Status(id Id) (Status, error) {
	status, ok := f.status[id]
	if ok == false {
		return StatusUnknown, nil
	}
	return status, nil
}

func (f *fakeJobSystem) GetTicket(id Id) (Ticket, error) {
	status, err := f.Status(id)
	return Ticket{id, status}, err
}

func (f *fakeJobSystem) NewJob(request JobRequest, jobsbase string, allowResubmit bool) (Ticket, error) {
	return Ticket{request.Id, StatusPending}, nil
}

func (f *fakeJobSystem) MultiStatus(ids []string) ([]Ticket, error) {
	tickets := make([]Ticket, 0, len(ids))
	for _, id := range ids {
		ticket, _ := f.GetTicket(Id(id))
		tickets = append(tickets, ticket)
	}
	return tickets, nil
}

func (f *fakeJobSystem) Dequeue() (*Ticket, error) {
	return nil, nil
}

func (f *fakeJobSystem) Tracking() bool           { return true }
func (f *fakeJobSystem) InstanceId() string       { return f.instance }
func (f *fakeJobSystem) Announce(<-chan struct{}) {}

func (f *fakeJobSystem) InstanceAddress(instance string) (string, error) {
	address, ok := f.addresses[instance]
	if ok == false {
		return "", nil
	}
	return address, nil
}

func (f *fakeJobSystem) InstanceAlive(instance string) bool {
	address, _ := f.InstanceAddress(instance)
	return len(address) > 0
}

func (f *fakeJobSystem) ForgetInstance(instance string) {
	f.dropped = append(f.dropped, instance)
}

func (f *fakeJobSystem) JobOwner(id Id) (string, error) {
	owner, ok := f.owners[id]
	if ok == false {
		return "", nil
	}
	if owner == "redis is down" {
		return "", errors.New("redis is down")
	}
	return owner, nil
}

func (f *fakeJobSystem) ClaimJob(id Id) error {
	if f.owners == nil {
		f.owners = make(map[Id]string)
	}
	f.owners[id] = f.instance
	return nil
}

func (f *fakeJobSystem) LoadJobRequest(id Id) (JobRequest, error) {
	return JobRequest{}, errors.New("not stored")
}

func (f *fakeJobSystem) Forget(id Id) error {
	if f.forgotten == nil {
		f.forgotten = make(map[Id]bool)
	}
	f.forgotten[id] = true
	return nil
}

// call runs a request for a ticket through a router that serves "local" when
// the job is here.
func call(t *testing.T, jobsystem *fakeJobSystem, id Id, forwarded bool) *httptest.ResponseRecorder {
	t.Helper()

	router := mux.NewRouter()
	handler := MakeOwnerRouter(jobsystem, ConfigRoot{}).Route(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("local"))
	})
	router.HandleFunc("/result/{ticket}/{entry}", handler)

	req := httptest.NewRequest("GET", "/result/"+string(id)+"/0", nil)
	if forwarded {
		req.Header.Set(forwardedHeader, "1")
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestServesOwnJobsLocally(t *testing.T) {
	id := testId("mine")
	jobsystem := &fakeJobSystem{
		instance: "self",
		owners:   map[Id]string{id: "self"},
	}

	res := call(t, jobsystem, id, false)
	if res.Body.String() != "local" {
		t.Fatalf("body %q, want the local handler to have answered", res.Body.String())
	}
}

// A job nobody claimed predates the tracker, or is an index job: the local
// directory is the only place it could be.
func TestServesUnclaimedJobsLocally(t *testing.T) {
	jobsystem := &fakeJobSystem{instance: "self"}

	res := call(t, jobsystem, testId("nobody"), false)
	if res.Body.String() != "local" {
		t.Fatalf("body %q, want the local handler to have answered", res.Body.String())
	}
}

// Redis being unreachable must not take out jobs that are on this instance.
func TestServesLocallyWhenOwnerLookupFails(t *testing.T) {
	id := testId("broken")
	jobsystem := &fakeJobSystem{
		instance: "self",
		owners:   map[Id]string{id: "redis is down"},
	}

	res := call(t, jobsystem, id, false)
	if res.Body.String() != "local" {
		t.Fatalf("body %q, want the local handler to have answered", res.Body.String())
	}
}

func TestForwardsToOwner(t *testing.T) {
	id := testId("theirs")

	var gotPath string
	var gotHeader string
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotPath = req.URL.Path
		gotHeader = req.Header.Get(forwardedHeader)
		w.Write([]byte("remote"))
	}))
	defer owner.Close()

	jobsystem := &fakeJobSystem{
		instance:  "self",
		owners:    map[Id]string{id: "other"},
		addresses: map[string]string{"other": owner.URL},
	}

	res := call(t, jobsystem, id, false)
	if res.Body.String() != "remote" {
		t.Fatalf("body %q, want the owner to have answered", res.Body.String())
	}
	if gotPath != "/result/"+string(id)+"/0" {
		t.Fatalf("owner was asked for %q", gotPath)
	}
	if gotHeader != "1" {
		t.Fatal("forwarded requests must be marked, or two servers can bounce one forever")
	}
}

// The owner it was forwarded to does not think it owns the job either. Rather
// than sending it on, say the results are gone.
func TestDoesNotForwardTwice(t *testing.T) {
	id := testId("bounce")
	jobsystem := &fakeJobSystem{
		instance:  "self",
		owners:    map[Id]string{id: "other"},
		addresses: map[string]string{"other": "http://127.0.0.1:1"},
	}

	res := call(t, jobsystem, id, true)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", res.Code)
	}
}

func TestReportsResultsGoneWhenOwnerIsGone(t *testing.T) {
	id := testId("dead")
	jobsystem := &fakeJobSystem{
		instance: "self",
		owners:   map[Id]string{id: "other"},
	}

	res := call(t, jobsystem, id, false)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 so that the client resubmits", res.Code)
	}
}

// The instance is still registered but its server is not answering.
func TestReportsResultsGoneWhenOwnerIsUnreachable(t *testing.T) {
	id := testId("silent")
	jobsystem := &fakeJobSystem{
		instance:  "self",
		owners:    map[Id]string{id: "other"},
		addresses: map[string]string{"other": "http://127.0.0.1:1"},
	}

	res := call(t, jobsystem, id, false)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", res.Code)
	}
	if len(jobsystem.dropped) == 0 || jobsystem.dropped[0] != "other" {
		t.Fatal("a cached address that did not work must be dropped")
	}
}

func TestInstanceAuth(t *testing.T) {
	handler := instanceAuth("secret", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/result/x/0", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d without a token, want 401", w.Code)
	}

	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Body.String() != "ok" {
		t.Fatalf("body %q with the right token", w.Body.String())
	}
}
