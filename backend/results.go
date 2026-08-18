package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

// Job inputs and finished results live in redis, so that nothing has to be
// shared between the server that accepts a search and the worker that runs it.
//
// A worker still needs real files while a job runs, because mmseqs is a
// command line tool that reads and writes them. What changes is that those
// files stop being the way anyone else reaches the job: once it is done the
// worker renders exactly what the API would have served out of that directory,
// puts it in redis, and deletes the directory. Every server can then answer
// for the job without having seen its files, and no disk accumulates results.
const (
	keyJob    = "mmseqs:job:"
	keyResult = "mmseqs:result:"
	keyStatus = "mmseqs:status:"
)

// ResultStore is the part of a job system that keeps job inputs and rendered
// results where every server can read them. Only the redis job system
// implements it; in local mode there is a single process and the files it
// wrote are all it needs.
type ResultStore interface {
	// StoreJobRequest keeps the input, which is what a worker rebuilds the job
	// directory from.
	StoreJobRequest(request JobRequest) error
	LoadJobRequest(id Id) (JobRequest, error)
	// StoreResults renders every entry of a finished job out of its directory
	// and keeps them. It reports how many entries it stored, which is zero for
	// job types whose results are not alignments.
	StoreResults(id Id, jobsbase string) (int, error)
	// LoadResult returns the rendered response for one entry, or nil when this
	// job has nothing stored.
	LoadResult(id Id, entry int64) ([]byte, error)
	// Heartbeat keeps a RUNNING job from expiring while it is still running.
	Heartbeat(id Id, stop <-chan struct{})
}

// Results returns the result store of a job system, if it has one.
func Results(jobsystem JobSystem) (ResultStore, bool) {
	store, ok := jobsystem.(ResultStore)
	return store, ok
}

func (j *RedisJobSystem) StoreJobRequest(request JobRequest) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return err
	}
	return j.Client.Set(keyJob+string(request.Id), buf.String(), 0).Err()
}

func (j *RedisJobSystem) LoadJobRequest(id Id) (JobRequest, error) {
	data, err := j.Client.Get(keyJob + string(id)).Result()
	if err != nil {
		return JobRequest{}, err
	}

	var request JobRequest
	if err := json.NewDecoder(strings.NewReader(data)).Decode(&request); err != nil {
		return JobRequest{}, err
	}
	return request, nil
}

func (j *RedisJobSystem) StoreResults(id Id, jobsbase string) (int, error) {
	dir := filepath.Join(filepath.Clean(jobsbase), string(id))
	count := resultEntries(dir)
	if count == 0 {
		return 0, nil
	}

	fields := make(map[string]interface{}, count)
	for entry := int64(0); entry < count; entry++ {
		response, err := Alignments(id, entry, jobsbase)
		if err != nil {
			return 0, err
		}
		payload, err := encodeResult(response)
		if err != nil {
			return 0, err
		}
		fields[strconv.FormatInt(entry, 10)] = payload
	}

	if err := j.Client.HMSet(keyResult+string(id), fields).Err(); err != nil {
		return 0, err
	}
	// Results are the only copy once the job directory is gone, so they need
	// an end of their own. SetStatus puts the same expiry on the status when
	// the job finishes, so the two disappear together.
	if j.Retention > 0 {
		j.Client.Expire(keyResult+string(id), j.Retention)
	}

	return int(count), nil
}

func (j *RedisJobSystem) LoadResult(id Id, entry int64) ([]byte, error) {
	payload, err := j.Client.HGet(keyResult+string(id), strconv.FormatInt(entry, 10)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return decodeResult(payload)
}

// Heartbeat keeps the status key of a running job alive. Letting it expire
// instead is what turns a job whose worker was killed mid-run back into an
// unknown ticket, which a client can simply resubmit, rather than one that
// stays RUNNING for as long as redis does.
func (j *RedisJobSystem) Heartbeat(id Id, stop <-chan struct{}) {
	if j.Lease <= 0 {
		return
	}

	// Refresh several times per lease, so that one slow round trip does not
	// declare a healthy job dead.
	ticker := time.NewTicker(j.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := j.Client.Expire(keyStatus+string(id), j.Lease).Err(); err != nil {
				log.Print(err)
			}
		case <-stop:
			return
		}
	}
}

// Forget drops every trace of a job, so that resubmitting the same query runs
// it again instead of resolving to a ticket whose results are gone.
func (j *RedisJobSystem) Forget(id Id) error {
	return j.Client.Del(
		keyStatus+string(id),
		keyJob+string(id),
		keyResult+string(id),
	).Err()
}

// resultEntries is the number of query entries a finished search produced,
// read from the first alignment database: each holds one entry per query.
func resultEntries(dir string) int64 {
	matches, err := filepath.Glob(filepath.Join(filepath.Clean(dir), "alis_*.index"))
	if err != nil || len(matches) == 0 {
		return 0
	}

	reader := Reader{}
	reader.Make(dbpaths(strings.TrimSuffix(matches[0], ".index")))
	size := reader.Size()
	reader.Delete()
	return size
}

// Responses are mostly aligned sequence strings and compress well. They are
// stored compressed because redis holds them in memory, where the difference
// between 200KB and 40KB a job decides whether an hour of traffic fits.
func encodeResult(response AlignmentResponse) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(response); err != nil {
		gz.Close()
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeResult(payload []byte) ([]byte, error) {
	if len(payload) < 2 || payload[0] != 0x1f || payload[1] != 0x8b {
		// Not compressed. Nothing writes uncompressed results, but handing
		// back what is there beats failing a fetch over an encoding guess.
		return payload, nil
	}

	gz, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	return ioutil.ReadAll(gz)
}
