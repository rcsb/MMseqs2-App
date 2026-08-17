package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var defaultFileContent = []byte(`{
    // should mmseqs und webserver output be printed
    "verbose": true,
    "server" : {
        "address"    : "127.0.0.1:8081",
        // prefix for all API endpoints
        "pathprefix" : "/api/",
        // enables additional API endpoints for adding databases
        // WARNING: No additional authentication provided. Enable only within trusted network/for trusted admins.
        "dbmanagment": false,
        /* enable HTTP Basic Auth (optional)
        "auth": {
            "username" : "",
            "password" : ""
        },
        */
        // should CORS headers be set to allow requests from anywhere
        "cors"       : true
    },
    // paths to workfolders and mmseqs, special character ~ is resolved relative to the binary location
    "paths" : {
        // path to mmseqs databases, has to be shared between server/workers
        "databases"    : "~databases",
        // path to job results and scratch directory, has to be shared between server/workers
        "results"      : "~jobs",
        // path to mmseqs binary
        "mmseqs"       : "~mmseqs"
    },
    /* per-instance job directories (optional)
       Set an address and the "results" path above no longer has to be shared
       between servers and workers: redis keeps track of which instance holds
       which job, and servers forward requests for jobs they do not have to
       the instance that ran them. The address has to be reachable by the
       other instances.
    "instances" : {
        "address"   : ":8082",
        // defaults to the local address that reaches redis
        "advertise" : "",
        // shared secret required on forwarded requests
        "token"     : ""
    },
    */
    // what this instance keeps of the jobs it has finished. Required when
    // "instances" is enabled, since nothing outside an instance can prune it.
    "cleanup" : {
        // minutes a finished job stays fetchable, 0 for no age limit
        "maxage"   : 60,
        // megabytes the results directory may use, oldest jobs deleted first,
        // 0 for no limit
        "maxsize"  : 0,
        // minutes between sweeps
        "interval" : 10
    },
    // connection details for redis database, not used in -local mode
    "redis" : {
        "network"  : "tcp",
        "address"  : "localhost:6379",
        "password" : "",
        "index"    : 0
    },
    // options for local/single-binary server
    "local" : {
        "workers"  : 1
    },
    "mail" : {
        "mailer" : {
            // three types available:
            // null: uses NullTransport class, which ignores all sent emails
            "type" : "null"
            /* smtp: Uses SMTP to send emails example for gmail
            "type" : "smtp",
            "transport" : {
                // full host URL with port
                "host" : "smtp.gmail.com:587",
                // RFC 4616  PLAIN authentication
                "auth" : {
                    {
                        // empty for gmail
                        "identity" : "",
                        // gmail user
                        "username" : "user@gmail.com",
                        "password" : "password",
                        "host" : "smtp.gmail.com",
                    }
                }
            }
            */
            /* mailgun: Uses the mailgun API to send emails
            "type"      : "mailgun",
            "transport" : {
                // mailgun domain
                "domain" : "mail.mmseqs.com",
                // mailgun API private key
                "secretkey" : "key-XXXX",
                // mailgun API public key
                "publickey" : "pubkey-XXXX"
            }
            */
        },
        // Email FROM field
        "sender"    : "mail@example.org",
        /* Bracket notation is also possible:
        "sender"    : "Webserver <mail@example.org>",
        */
        // Email templates. First "%s" is resolved to the ticket identifier
        "templates" : {
            "success" : {
                "subject" : "Done -- %s",
                "body"    : "%s"
            },
            "timeout" : {
                "subject" : "Timeout -- %s",
                "body"    : "%s"
            },
            "error"   : {
                "subject" : "Error -- %s",
                "body"    : "%s"
            }
        }
    }
}
`)

type ConfigPaths struct {
	Databases string `json:"databases"`
	Results   string `json:"results"`
	Temporary string `json:"temporary"`
	Mmseqs    string `json:"mmseqs"`
}

type ConfigRedis struct {
	Network  string `json:"network"`
	Address  string `json:"address"`
	Password string `json:"password"`
	DbIndex  int    `json:"index"`
}

type ConfigLocal struct {
	Workers int `json:"workers"`
}

type ConfigMailTemplate struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type ConfigMailTemplates struct {
	Success ConfigMailTemplate `json:"success"`
	Timeout ConfigMailTemplate `json:"timeout"`
	Error   ConfigMailTemplate `json:"error"`
}

type ConfigMail struct {
	Mailer    *ConfigMailtransport `json:"mailer" valid:"optional"`
	Sender    string               `json:"sender"`
	Templates ConfigMailTemplates  `json:"templates"`
}

type ConfigAuth struct {
	Username string `json:"username" valid:"required"`
	Password string `json:"password" valid:"required"`
}

type ConfigServer struct {
	Address     string      `json:"address" valid:"required"`
	PathPrefix  string      `json:"pathprefix" valid:"optional"`
	DbManagment bool        `json:"dbmanagment" valid:"optional"`
	CORS        bool        `json:"cors" valid:"optional"`
	Auth        *ConfigAuth `json:"auth" valid:"optional"`
}

// ConfigInstances turns the results directory from something every server and
// worker has to share into something each of them can keep to itself.
//
// Setting an address enables it: redis then tracks which instance holds which
// job, servers forward requests for jobs they do not have, and each instance
// cleans up after itself. Leaving it empty keeps the shared volume behaviour.
type ConfigInstances struct {
	// Where to listen for requests forwarded by other instances, ":8082" for
	// example. Must be reachable by them, so not a loopback address.
	Address string `json:"address" valid:"optional"`
	// What to tell other instances to connect to. Defaults to the local
	// address that reaches redis, which is the right one in almost every
	// setup; set it when that guess is wrong.
	Advertise string `json:"advertise" valid:"optional"`
	// Shared secret required on forwarded requests. Optional, but the
	// listener is reachable by anything that can route to the pod.
	Token string `json:"token" valid:"optional"`
}

func (c ConfigInstances) Enabled() bool {
	return len(c.Address) > 0
}

// ConfigCleanup bounds what an instance keeps on its own disk. Nothing outside
// the instance can prune a per-instance volume, so this is not optional
// housekeeping: without it a long lived instance fills its volume up.
type ConfigCleanup struct {
	// Minutes a finished job is kept before its directory is deleted and its
	// redis keys are dropped. This is the retention policy: how long after a
	// search its results can still be fetched. Zero means no age limit.
	MaxAge int `json:"maxage" valid:"optional"`
	// Megabytes the results directory may use, oldest jobs deleted first when
	// it is over. This is what actually bounds the volume, since an age limit
	// says nothing about how much a busy hour writes. Zero means no limit.
	MaxSize int `json:"maxsize" valid:"optional"`
	// Minutes between sweeps.
	Interval int `json:"interval" valid:"optional"`
}

const DefaultCleanupInterval = 10 * time.Minute

// Retention is how long the redis keys of a finished job are kept. The janitor
// is what normally deletes a job, keys and files together, so this only has to
// cover the case where it never gets to: an instance that died. It trails the
// janitor by a couple of sweeps so that it is never redis that expires a job
// whose files are still there to be read.
func (c ConfigRoot) Retention() time.Duration {
	if c.Instances.Enabled() == false {
		return 0
	}

	interval := time.Duration(c.Cleanup.Interval) * time.Minute
	if interval <= 0 {
		interval = DefaultCleanupInterval
	}

	if c.Cleanup.MaxAge <= 0 {
		// Only a size limit is set, so a job has no age at which it is due to
		// be deleted. The keys still need an end, or the ones belonging to an
		// instance that died stay in redis for good.
		return 24 * time.Hour
	}
	return time.Duration(c.Cleanup.MaxAge)*time.Minute + 2*interval
}

// CheckCleanup refuses a configuration that would grow without bound. With a
// per-instance results directory there is no cron job that can come along
// later and clean up, so an instance that does not prune itself fills its
// volume and stops serving.
func (c *ConfigRoot) CheckCleanup() error {
	if c.Instances.Enabled() == false {
		return nil
	}
	if c.Cleanup.MaxAge <= 0 && c.Cleanup.MaxSize <= 0 {
		return errors.New("instances.address is set, so nothing outside this instance can prune its results: set cleanup.maxage (minutes), cleanup.maxsize (megabytes), or both")
	}
	return nil
}

type ConfigRoot struct {
	Server    ConfigServer    `json:"server" valid:"required"`
	Paths     ConfigPaths     `json:"paths" valid:"required"`
	Redis     ConfigRedis     `json:"redis" valid:"optional"`
	Local     ConfigLocal     `json:"local" valid:"optional"`
	Mail      ConfigMail      `json:"mail" valid:"optional"`
	Instances ConfigInstances `json:"instances" valid:"optional"`
	Cleanup   ConfigCleanup   `json:"cleanup" valid:"optional"`
	Verbose   bool            `json:"verbose"`
}

func ReadConfigFromFile(name string) (ConfigRoot, error) {
	file, err := os.Open(name)
	if err != nil {
		return ConfigRoot{}, err
	}
	defer file.Close()

	absPath, err := filepath.Abs(name)
	if err != nil {
		return ConfigRoot{}, err
	}

	relativeTo := filepath.Dir(absPath)

	return ReadConfig(file, relativeTo)
}

func DefaultConfig() (ConfigRoot, error) {
	r := bytes.NewReader(defaultFileContent)

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	relativeTo := filepath.Dir(ex)

	return ReadConfig(r, relativeTo)
}

func ReadConfig(r io.Reader, relativeTo string) (ConfigRoot, error) {
	var config ConfigRoot
	if err := DecodeJsonAndValidate(r, &config); err != nil {
		return config, fmt.Errorf("Fatal error for config file: %s\n", err)
	}

	paths := []*string{&config.Paths.Databases, &config.Paths.Results, &config.Paths.Mmseqs}
	for _, path := range paths {
		if strings.HasPrefix(*path, "~") {
			*path = strings.TrimLeft(*path, "~")
			*path = filepath.Join(relativeTo, *path)
		}
	}

	return config, nil
}

func (c *ConfigRoot) CheckPaths() error {
	paths := []string{c.Paths.Databases, c.Paths.Results}
	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			os.MkdirAll(path, 0755)
		}
	}

	if _, err := os.Stat(c.Paths.Mmseqs); err != nil {
		return errors.New("MMseqs2 binary was not found at " + c.Paths.Mmseqs)
	}

	return nil
}

func (c *ConfigRoot) ReadParameters(args []string) error {
	var key string
	inParameter := false
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			if inParameter == true {
				return errors.New("Invalid Parameter String")
			}
			key = strings.TrimLeft(arg, "-")
			inParameter = true
		} else {
			if inParameter == false {
				return errors.New("Invalid Parameter String")
			}
			err := c.setParameter(key, arg)
			if err != nil {
				return err
			}
			inParameter = false
		}
	}

	if inParameter == true {
		return errors.New("Invalid Parameter String")
	}

	return nil
}

func (c *ConfigRoot) setParameter(key string, value string) error {
	path := strings.Split(key, ".")
	return setNodeValue(c, path, value)
}

// DFS in Config Tree to set the new value
func setNodeValue(node interface{}, path []string, value string) error {
	if len(path) == 0 {
		if v, ok := node.(reflect.Value); ok {
			if v.IsValid() == false || v.CanSet() == false {
				return errors.New("Leaf node is not valid")
			}

			switch v.Kind() {
			case reflect.Struct:
				return errors.New("Leaf node is a struct")
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				i, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return err
				}
				v.SetInt(i)
				break
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				i, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					return err
				}
				v.SetUint(i)
				break
			case reflect.Bool:
				b, err := strconv.ParseBool(value)
				if err != nil {
					return err
				}
				v.SetBool(b)
				break
			case reflect.String:
				v.SetString(value)
				break
			default:
				return errors.New("Leaf node type not implemented")
			}
			return nil
		} else {
			return errors.New("Leaf node is not a value")
		}
	}

	v, ok := node.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(node).Elem()
	}

	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			t := v.Type().Elem()
			n := reflect.New(t)
			v.Set(n)
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return errors.New("Node is not a struct")
	}

	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}

		if tag == path[0] {
			f := v.Field(i)
			return setNodeValue(f, path[1:], value)
		}
	}

	return errors.New("Path not found in config")
}
