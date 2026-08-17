package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/go-redis/redis"
)

// Redis keys backing the instance tracker.
//
// mmseqs:job:<id> is the job input. Keeping it here is what lets any worker
// rebuild the job directory on its own disk, without a shared volume.
//
// mmseqs:owner:<id> is the instance whose disk holds the job directory.
//
// mmseqs:instance:<instance> is where that instance can be reached. It has a
// TTL, so an instance that dies stops being an answer to anything on its own.
const (
	keyJob      = "mmseqs:job:"
	keyOwner    = "mmseqs:owner:"
	keyInstance = "mmseqs:instance:"
)

func (j *RedisJobSystem) Tracking() bool {
	return len(j.Instance) > 0
}

func (j *RedisJobSystem) InstanceId() string {
	return j.Instance
}

// Announce publishes this instance's address and keeps refreshing it. The
// entry expiring is what tells everybody else that the jobs on this instance
// are gone, so the refresh must outlive nothing but the process itself.
func (j *RedisJobSystem) Announce(stop <-chan struct{}) {
	if j.Tracking() == false {
		return
	}

	key := keyInstance + j.Instance
	refresh := func() {
		if err := j.Client.Set(key, j.Address, InstanceTTL).Err(); err != nil {
			log.Print(err)
		}
	}

	refresh()
	log.Println("Instance " + j.Instance + " serving jobs at " + j.Address)

	ticker := time.NewTicker(InstanceRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			refresh()
		case <-stop:
			// Nothing on this volume can be served any more, so do not make
			// everyone else wait out the TTL to find out.
			j.Client.Del(key)
			return
		}
	}
}

func (j *RedisJobSystem) InstanceAddress(instance string) (string, error) {
	if len(instance) == 0 {
		return "", nil
	}
	if address, ok := j.addresses.get(instance); ok {
		return address, nil
	}

	address, err := j.Client.Get(keyInstance + instance).Result()
	if err != nil {
		if err == redis.Nil {
			j.addresses.put(instance, "")
			return "", nil
		}
		return "", err
	}

	j.addresses.put(instance, address)
	return address, nil
}

func (j *RedisJobSystem) ForgetInstance(instance string) {
	j.addresses.forget(instance)
}

func (j *RedisJobSystem) InstanceAlive(instance string) bool {
	address, err := j.InstanceAddress(instance)
	if err != nil {
		// A redis that cannot be reached is not evidence that an instance died,
		// and treating it as such would throw away every job in the cluster.
		return true
	}
	return len(address) > 0
}

func (j *RedisJobSystem) JobOwner(id Id) (string, error) {
	owner, err := j.Client.Get(keyOwner + string(id)).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return owner, nil
}

func (j *RedisJobSystem) ClaimJob(id Id) error {
	return j.Client.Set(keyOwner+string(id), j.Instance, 0).Err()
}

func (j *RedisJobSystem) LoadJobRequest(id Id) (JobRequest, error) {
	data, err := j.Client.Get(keyJob + string(id)).Result()
	if err != nil {
		return JobRequest{}, err
	}

	var request JobRequest
	if err := json.NewDecoder(bytes.NewBufferString(data)).Decode(&request); err != nil {
		return JobRequest{}, err
	}
	return request, nil
}

// Forget removes the job from redis entirely. The files stay on whichever
// instance owned them until its janitor collects them; nothing can look them
// up any more, and a resubmission of the same query starts a fresh job.
func (j *RedisJobSystem) Forget(id Id) error {
	return j.Client.Del(
		"mmseqs:status:"+string(id),
		keyJob+string(id),
		keyOwner+string(id),
	).Err()
}

// checkOwner verifies that the instance holding a job is still alive, and
// resolves the job if it is not. Results that only existed on a volume that no
// longer exists have to stop being advertised as COMPLETE, or clients poll a
// ticket that can never be fetched.
func (j *RedisJobSystem) checkOwner(id Id, status Status) Status {
	if status != StatusComplete && status != StatusRunning {
		return status
	}

	owner, err := j.JobOwner(id)
	if err != nil {
		return status
	}
	if len(owner) == 0 {
		// Claimed by nobody yet: the worker sets the owner before it sets
		// RUNNING, so this is a job from before the upgrade, or an index job.
		return status
	}
	if j.InstanceAlive(owner) {
		return status
	}

	if status == StatusRunning {
		// The worker died with its volume. Nothing was lost that cannot be
		// recomputed, so put the job back in the queue for a live worker.
		if err := j.requeue(id); err != nil {
			log.Print(err)
			return status
		}
		log.Println("Requeued " + string(id) + ", instance " + owner + " is gone")
		return StatusPending
	}

	if err := j.Forget(id); err != nil {
		log.Print(err)
		return status
	}
	log.Println("Dropped " + string(id) + ", instance " + owner + " is gone")
	return StatusUnknown
}

// requeue puts an already stored job back on the pending queue.
func (j *RedisJobSystem) requeue(id Id) error {
	request, err := j.LoadJobRequest(id)
	if err != nil {
		// Without the input there is nothing to run, so the job is lost.
		j.Forget(id)
		return err
	}

	job, ok := request.Job.(Job)
	if ok == false {
		j.Forget(id)
		return errors.New("Invalid Job")
	}

	j.Client.Del(keyOwner + string(id))
	if err := j.SetStatus(id, StatusPending); err != nil {
		return err
	}
	return j.Client.ZAdd("mmseqs:pending", redis.Z{Score: job.Rank(), Member: string(id)}).Err()
}

// newTrackedJob is NewJob when job directories are per instance: the input
// goes to redis instead of a shared disk, and the job directory is created by
// whichever worker picks the job up.
func (j *RedisJobSystem) newTrackedJob(request JobRequest, allowResubmit bool) (Ticket, error) {
	id := request.Id
	res, err := j.Status(id)
	if err != nil {
		return Ticket{id, StatusError}, err
	}

	switch res {
	case StatusComplete:
		if allowResubmit == false {
			return Ticket{id, res}, nil
		}
		j.Forget(id)
	case StatusPending, StatusRunning:
		return Ticket{id, res}, nil
	case StatusError:
		j.Forget(id)
	}

	job, ok := request.Job.(Job)
	if ok == false {
		return Ticket{id, StatusError}, errors.New("Invalid Job")
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return Ticket{id, StatusError}, err
	}

	// The input has to be readable before the job is queued, or a fast worker
	// dequeues a job it cannot run.
	err = j.Client.Watch(func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Set(keyJob+string(id), buf.String(), 0)
			pipe.Set("mmseqs:status:"+string(id), string(StatusPending), 0)
			pipe.ZAdd("mmseqs:pending", redis.Z{Score: job.Rank(), Member: string(id)})
			return nil
		})
		return err
	})

	if err != nil {
		j.SetStatus(id, StatusError)
		return Ticket{id, StatusError}, err
	}

	return Ticket{id, StatusPending}, nil
}
