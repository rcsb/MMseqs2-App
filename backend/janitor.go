package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// A per-instance results directory is only visible from inside the instance,
// so nothing outside it can prune it: no cron job can reach the disk, and the
// instances that could are not allowed to touch each other's files. Each
// instance therefore collects its own garbage, dropping the redis keys of the
// jobs it deletes so that nothing is left pointing at files that are gone.
//
// There are two bounds and both matter. An age limit is what the retention
// policy is about, how long after a search its results can still be fetched.
// A size limit is what keeps the promise, because an age limit says nothing
// about how much a busy hour writes, and the volume under a per-instance
// results directory is small and fixed.
func janitor(jobsystem JobSystem, config ConfigRoot) {
	maxAge := time.Duration(config.Cleanup.MaxAge) * time.Minute
	maxSize := int64(config.Cleanup.MaxSize) * 1024 * 1024
	if maxAge <= 0 && maxSize <= 0 {
		return
	}

	interval := time.Duration(config.Cleanup.Interval) * time.Minute
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}

	if maxAge > 0 {
		log.Println("Deleting finished jobs older than " + maxAge.String())
	}
	if maxSize > 0 {
		log.Println("Keeping the results directory below " + ByteSize(maxSize))
	}

	for {
		time.Sleep(interval)
		if err := sweep(jobsystem, config.Paths.Results, maxAge, maxSize); err != nil {
			log.Print(err)
		}
	}
}

type jobDir struct {
	id      Id
	modtime time.Time
	size    int64
}

func sweep(jobsystem JobSystem, results string, maxAge time.Duration, maxSize int64) error {
	base := filepath.Clean(results)
	entries, err := ioutil.ReadDir(base)
	if err != nil {
		return err
	}

	jobs := make([]jobDir, 0, len(entries))
	var total int64
	for _, entry := range entries {
		if entry.IsDir() == false || validId(entry.Name()) == false {
			continue
		}
		size := dirSize(filepath.Join(base, entry.Name()))
		jobs = append(jobs, jobDir{Id(entry.Name()), entry.ModTime(), size})
		total += size
	}

	// Oldest first, which is the order both bounds want to delete in.
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].modtime.Before(jobs[j].modtime)
	})

	for _, job := range jobs {
		expired := maxAge > 0 && time.Since(job.modtime) >= maxAge
		tooBig := maxSize > 0 && total > maxSize
		if expired == false && tooBig == false {
			break
		}

		if remove(jobsystem, base, job.id) == false {
			continue
		}
		total -= job.size
	}

	return nil
}

// remove deletes one job directory, and the redis keys of the job if they are
// this instance's to delete. It reports whether the files are gone.
func remove(jobsystem JobSystem, base string, id Id) bool {
	status, err := jobsystem.Status(id)
	if err != nil {
		return false
	}
	// A job still queued or running is not garbage, however old its directory
	// looks: a long index job barely touches it while it works.
	if status == StatusRunning || status == StatusPending {
		return false
	}

	if tracker, tracked := Tracker(jobsystem); tracked {
		owner, err := tracker.JobOwner(id)
		if err != nil {
			return false
		}
		// A directory we do not own is a leftover, from a job that was
		// requeued and ran somewhere else. Its keys belong to whoever has it
		// now, so only the files go.
		if owner == tracker.InstanceId() {
			if err := tracker.Forget(id); err != nil {
				log.Print(err)
				return false
			}
		}
	}

	if err := os.RemoveAll(filepath.Join(base, string(id))); err != nil {
		log.Print(err)
		return false
	}
	return true
}

func dirSize(dir string) int64 {
	var size int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	return size
}

func ByteSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return strconv.FormatInt(bytes, 10) + " B"
	}
	value, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		value *= unit
		exp++
	}
	return strconv.FormatInt(bytes/value, 10) + string("KMGTPE"[exp]) + "iB"
}
