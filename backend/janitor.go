package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

// A per-instance results directory is only visible from inside the instance,
// so nothing outside it can prune it: no cron job can reach the disk, and the
// instances that could are not allowed to touch each other's files. Each
// instance therefore collects its own garbage, dropping the redis keys of the
// jobs it deletes so that nothing is left pointing at files that are gone.
func janitor(jobsystem JobSystem, config ConfigRoot) {
	if config.Cleanup.MaxAge <= 0 {
		return
	}

	interval := time.Duration(config.Cleanup.Interval) * time.Minute
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	maxAge := time.Duration(config.Cleanup.MaxAge) * time.Minute

	log.Println("Deleting finished jobs older than " + maxAge.String())
	for {
		time.Sleep(interval)
		if err := sweep(jobsystem, config.Paths.Results, maxAge); err != nil {
			log.Print(err)
		}
	}
}

func sweep(jobsystem JobSystem, results string, maxAge time.Duration) error {
	base := filepath.Clean(results)
	entries, err := ioutil.ReadDir(base)
	if err != nil {
		return err
	}

	tracker, tracked := Tracker(jobsystem)
	for _, entry := range entries {
		if entry.IsDir() == false || validId(entry.Name()) == false {
			continue
		}
		if time.Since(entry.ModTime()) < maxAge {
			continue
		}

		id := Id(entry.Name())
		status, err := jobsystem.Status(id)
		if err != nil {
			continue
		}
		if status == StatusRunning || status == StatusPending {
			continue
		}

		if tracked {
			owner, err := tracker.JobOwner(id)
			if err != nil {
				continue
			}
			// A directory we do not own is a leftover, from a job that was
			// requeued and ran somewhere else. Its keys belong to whoever has
			// it now, so only the files go.
			if owner == tracker.InstanceId() {
				if err := tracker.Forget(id); err != nil {
					log.Print(err)
					continue
				}
			}
		}

		if err := os.RemoveAll(filepath.Join(base, string(id))); err != nil {
			log.Print(err)
		}
	}

	return nil
}
