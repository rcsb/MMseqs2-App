package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testId(seed string) Id {
	padded := seed + strings.Repeat("x", 38)
	return Id(padded[:38])
}

// writeDb lays out an mmseqs style database: entries in a data file, and an
// index of key/offset/length pointing into it. Each entry is terminated by a
// NUL, which the length counts and Reader.Data trims.
func writeDb(t *testing.T, base string, entries []string) {
	t.Helper()

	var data bytes.Buffer
	var index bytes.Buffer
	offset := 0
	for i, entry := range entries {
		payload := entry + "\x00"
		data.WriteString(payload)
		fmt.Fprintf(&index, "%d\t%d\t%d\n", i, offset, len(payload))
		offset += len(payload)
	}

	if err := ioutil.WriteFile(base, data.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(base+".index", index.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

const testAlignment = "QUERY\t1abc_A\t99.5\t100\t1\t0\t1\t100\t5\t104\t1e-50\t250\t120\t130\tMKVLAA\tMKVLAT"

// writeJobDir builds what a finished single query search leaves behind, with
// as many query entries as requested.
func writeJobDir(t *testing.T, results string, id Id, queries int) string {
	t.Helper()

	dir := filepath.Join(results, string(id))
	if err := os.MkdirAll(filepath.Join(dir, "tmp", "latest"), 0755); err != nil {
		t.Fatal(err)
	}

	alignments := make([]string, queries)
	sequences := make([]string, queries)
	headers := make([]string, queries)
	for i := 0; i < queries; i++ {
		alignments[i] = testAlignment
		sequences[i] = "MKVLAA"
		headers[i] = fmt.Sprintf("sp|P%d|TEST", i)
	}

	writeDb(t, filepath.Join(dir, "alis_pdb"), alignments)
	writeDb(t, filepath.Join(dir, "tmp", "latest", "query"), sequences)
	writeDb(t, filepath.Join(dir, "tmp", "latest", "query_h"), headers)
	return dir
}

func TestRenderJobDirectory(t *testing.T) {
	results, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(results)

	id := testId("render")
	dir := writeJobDir(t, results, id, 1)

	if n := resultEntries(dir); n != 1 {
		t.Fatalf("resultEntries = %d, want 1", n)
	}

	response, err := Alignments(id, 0, results)
	if err != nil {
		t.Fatal(err)
	}
	if response.Query.Header != "sp|P0|TEST" || response.Query.Sequence != "MKVLAA" {
		t.Fatalf("query = %+v", response.Query)
	}
	if len(response.Results) != 1 || response.Results[0].Database != "pdb" {
		t.Fatalf("results = %+v", response.Results)
	}
	if len(response.Results[0].Alignments) != 1 {
		t.Fatalf("alignments = %+v", response.Results[0].Alignments)
	}
	hit := response.Results[0].Alignments[0]
	if hit.Target != "1abc_A" || hit.Score != 250 || hit.DbAln != "MKVLAT" {
		t.Fatalf("alignment = %+v", hit)
	}
}

// A directory with no alignment database renders nothing, and must say so
// rather than reporting an entry that cannot be read.
func TestRenderEmptyJobDirectory(t *testing.T) {
	results, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(results)

	id := testId("empty")
	dir := filepath.Join(results, string(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	if n := resultEntries(dir); n != 0 {
		t.Fatalf("resultEntries = %d, want 0", n)
	}
	// Reader.Make leaves a nil file handle here; this used to panic in Delete.
	if _, err := Alignments(id, 0, results); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeResultRoundTrip(t *testing.T) {
	response := AlignmentResponse{
		Query: FastaEntry{"sp|P0|TEST", "MKVLAA"},
		Results: []SearchResult{{
			Database:   "pdb",
			Alignments: []AlignmentEntry{{Query: "QUERY", Target: "1abc_A", Score: 250, QueryAln: "MKVLAA"}},
		}},
	}

	payload, err := encodeResult(response)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != 0x1f || payload[1] != 0x8b {
		t.Fatal("results are stored compressed, redis keeps them in memory")
	}

	plain, err := decodeResult(payload)
	if err != nil {
		t.Fatal(err)
	}

	var decoded AlignmentResponse
	if err := json.Unmarshal(plain, &decoded); err != nil {
		t.Fatalf("stored payload is not the JSON the API serves: %v", err)
	}
	if decoded.Query.Header != response.Query.Header ||
		decoded.Results[0].Alignments[0].Target != "1abc_A" {
		t.Fatalf("round trip lost data: %+v", decoded)
	}

	// Anything not gzipped is handed back untouched rather than failing.
	if plain, err := decodeResult([]byte("{}")); err != nil || string(plain) != "{}" {
		t.Fatalf("uncompressed payload: %q %v", plain, err)
	}
}

// storeResults is what makes the job directory disposable, so what it deletes
// and what it keeps is the whole contract.
type fakeStore struct {
	stored int
	err    error
}

func (f *fakeStore) StoreJobRequest(JobRequest) error      { return nil }
func (f *fakeStore) LoadJobRequest(Id) (JobRequest, error) { return JobRequest{}, nil }
func (f *fakeStore) LoadResult(Id, int64) ([]byte, error)  { return nil, nil }
func (f *fakeStore) Heartbeat(Id, <-chan struct{})         {}
func (f *fakeStore) StoreResults(Id, string) (int, error)  { return f.stored, f.err }

func TestStoreResultsDropsRenderedJobs(t *testing.T) {
	results, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(results)

	config := ConfigRoot{}
	config.Paths.Results = results

	cases := []struct {
		name    string
		jobtype JobType
		stored  int
		gone    bool
	}{
		{"search results are in redis, the files are scratch", JobSearch, 1, true},
		{"an index job leaves nothing worth keeping", JobIndex, 0, true},
		{"an msa job's results are files nothing rendered", JobMsa, 0, false},
	}

	for _, c := range cases {
		id := testId(string(c.jobtype))
		dir := writeJobDir(t, results, id, 1)

		err := storeResults(&fakeStore{stored: c.stored}, id, JobRequest{Id: id, Type: c.jobtype}, config)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}

		_, statErr := os.Stat(dir)
		if c.gone && os.IsNotExist(statErr) == false {
			t.Fatalf("%s: directory was kept", c.name)
		}
		if c.gone == false && statErr != nil {
			t.Fatalf("%s: directory was deleted: %v", c.name, statErr)
		}
		os.RemoveAll(dir)
	}
}

// A store that failed must not have its job directory deleted: the results
// would be gone from both places.
func TestStoreResultsKeepsFilesWhenStoringFails(t *testing.T) {
	results, err := ioutil.TempDir("", "results")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(results)

	config := ConfigRoot{}
	config.Paths.Results = results

	id := testId("failed")
	dir := writeJobDir(t, results, id, 1)

	if err := storeResults(&fakeStore{err: fmt.Errorf("redis is down")}, id, JobRequest{Id: id, Type: JobSearch}, config); err == nil {
		t.Fatal("a failed store must be reported")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory was deleted after a failed store: %v", err)
	}
}

func TestResultConfigDefaults(t *testing.T) {
	empty := ConfigResults{}
	if empty.Retention() != DefaultResultTTL || empty.LeaseDuration() != DefaultResultLease {
		t.Fatal("an unset results block must still bound how long redis keeps jobs")
	}

	set := ConfigResults{TTL: 5, Lease: 30}
	if set.Retention().Minutes() != 5 || set.LeaseDuration().Seconds() != 30 {
		t.Fatalf("retention=%v lease=%v", set.Retention(), set.LeaseDuration())
	}
}
