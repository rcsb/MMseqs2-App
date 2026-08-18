package main

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"sort"
	"strings"
)

type SearchJob struct {
	Size     int      `json:"size" valid:"required"`
	Database []string `json:"database" valid:"required"`
	Mode     string   `json:"mode" valid:"required"`
	// Exported so that a JobRequest fully describes its job: the worker that
	// runs it has never seen the job.fasta a server wrote, it only gets this
	// struct back out of redis.
	Query string `json:"query"`
}

func (r SearchJob) Hash() Id {
	h := sha256.New224()
	h.Write([]byte(r.Query))
	h.Write([]byte(r.Mode))

	sort.Strings(r.Database)

	for _, value := range r.Database {
		h.Write([]byte(value))
	}

	bs := h.Sum(nil)
	return Id(base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(bs))
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (r SearchJob) Rank() float64 {
	return float64(r.Size * max(len(r.Database), 1))
}

func (r SearchJob) WriteFasta(path string) error {
	err := ioutil.WriteFile(path, []byte(r.Query), 0644)
	if err != nil {
		return err
	}
	return nil
}

func isIn(num string, params []string) int {
	for i, param := range params {
		if num == param {
			return i
		}
	}

	return -1
}

func NewSearchJobRequest(query string, dbs []string, validDbs []Params, mode string, resultPath string, email string) (JobRequest, error) {
	job := SearchJob{
		max(strings.Count(query, ">"), 1),
		dbs,
		mode,
		query,
	}

	request := JobRequest{
		job.Hash(),
		StatusPending,
		JobSearch,
		job,
		email,
	}

	ids := make([]string, len(validDbs))
	for i, item := range validDbs {
		ids[i] = item.Display.Path
	}

	for _, item := range job.Database {
		idx := isIn(item, ids)
		if idx == -1 {
			return request, errors.New("Selected databases are not valid!")
		}
	}

	return request, nil
}
