package main

import (
	"os"
	"os/signal"
	"syscall"
)

type RunType int

const (
	LOCAL RunType = iota
	WORKER
	SERVER
)

func ParseType(args []string) (RunType, []string) {
	resArgs := make([]string, 0)
	t := SERVER
	for _, arg := range args {
		switch arg {
		case "-worker":
			t = WORKER
			continue
		case "-server":
			t = SERVER
			continue
		case "-local":
			t = LOCAL
			continue
		}

		resArgs = append(resArgs, arg)
	}

	return t, resArgs
}

func ParseConfigName(args []string) (string, []string) {
	resArgs := make([]string, 0)
	file := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "-config" {
			file = args[i+1]
			i++
			continue
		}

		resArgs = append(resArgs, args[i])
	}

	return file, resArgs
}

// makeJobSystem builds the redis job system that holds job inputs, statuses
// and results, so that servers and workers share nothing but redis.
func makeJobSystem(config ConfigRoot) *RedisJobSystem {
	jobsystem := MakeRedisJobSystem(config.Redis)
	jobsystem.Retention = config.Results.Retention()
	jobsystem.Lease = config.Results.LeaseDuration()
	return jobsystem
}

func main() {
	t, args := ParseType(os.Args[1:])
	configFile, args := ParseConfigName(args)

	var config ConfigRoot
	var err error
	if len(configFile) > 0 {
		config, err = ReadConfigFromFile(configFile)

	} else {
		config, err = DefaultConfig()
	}
	if err != nil {
		panic(err)
	}

	err = config.ReadParameters(args)
	if err != nil {
		panic(err)
	}

	if err := config.CheckPaths(); err != nil {
		panic(err)
	}

	switch t {
	case WORKER:
		worker(makeJobSystem(config), config)
		break
	case SERVER:
		server(makeJobSystem(config), config)
		break
	case LOCAL:
		jobsystem, err := MakeLocalJobSystem(config.Paths.Results)
		if err != nil {
			panic(err)
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigs
			os.Exit(0)
		}()

		loop := make(chan bool)
		for i := 0; i < config.Local.Workers; i++ {
			go worker(&jobsystem, config)
		}
		go server(&jobsystem, config)
		<-loop
		break
	}
}
