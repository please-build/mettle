// Package main implements an execution server for the Remote Execution API.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/peterebden/go-cli-init/v4/flags"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/sirupsen/logrus"

	"github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/api"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/mettle/worker"
	"github.com/thought-machine/please-servers/redis"
)

var log = logging.MustGetLogger()

type StorageOpts struct {
	Storage   string `short:"s" long:"url" env:"STORAGE_URL" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
	TLS       bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	TokenFile string `long:"token_file" env:"TOKEN_FILE" description:"File containing a pre-shared token to authenticate to storage server with."`
}

type CacheOpts struct {
	Dir    string         `long:"dir" description:"Directory to cache blobs in"`
	Prefix []string       `long:"prefix" description:"Path prefix for cache items to store"`
	Part   []string       `long:"part" description:"Cache any paths with a component with this name in them"`
	MaxMem flags.ByteSize `long:"max_size" default:"100M" description:"Max size of in-memory blob cache"`
}

var opts = struct {
	Usage        string
	Logging      cli.LoggingOpts `group:"Options controlling logging output"`
	InstanceName string          `short:"i" long:"instance_name" default:"mettle" description:"Name of this execution instance"`
	API          struct {
		API struct {
			URL string `long:"url" description:"URL for communicating with other API servers"`
			TLS bool   `long:"tls" description:"Use TLS for communication between api servers"`
		} `group:"Options controlling communication with other API servers for bootstrapping zero-downtime deployments." namespace:"api"`
		Storage         StorageOpts         `group:"Options controlling communication with the CAS server" namespace:"storage"`
		GRPC            grpcutil.Opts       `group:"Options controlling the gRPC server"`
		Queues          api.PubSubOpts      `group:"Options controlling the pub/sub queues"`
		AllowedPlatform map[string][]string `long:"allowed_platform" description:"Allowed values for platform properties"`
	} `command:"api" description:"Start as an API server"`
	Worker struct {
		Dir               string                  `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		NoClean           bool                    `long:"noclean" description:"Don't clean workdirs after actions complete"`
		PreflightAction   bool                    `long:"preflight_action" description:"Run a pre-flight test action before starting to process work"`
		Name              string                  `short:"n" long:"name" description:"Name of this worker"`
		Browser           string                  `long:"browser" env:"BROWSER_URL" description:"Base URL for browser service (only used to construct informational user messages"`
		Lucidity          string                  `long:"lucidity" description:"URL of Lucidity server to report to"`
		PromGateway       string                  `long:"prom_gateway" default:"" description:"Gateway URL to push Prometheus updates to."`
		Sandbox           string                  `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox        string                  `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout           flags.Duration          `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		MinDiskSpace      flags.ByteSize          `long:"min_disk_space" default:"1G" description:"Don't accept builds unless at least this much disk space is available"`
		MemoryThreshold   float64                 `long:"memory_threshold" default:"100.0" description:"Don't accept builds unless available memory is under this percentage"`
		ConnCheck         string                  `long:"connectivity_check" choice:"gstatic" choice:"firefox" description:"Run an HTTP connectivity check periodically to verify if HTTP access is working"`
		ConnCheckPeriod   flags.Duration          `long:"connectivity_check_period" default:"1h" description:"Periodicity to re-check connectivity at"`
		VersionFile       string                  `long:"version_file" description:"File containing version tag"`
		Costs             map[string]cli.Currency `long:"cost" description:"Per-second costs to associate with each build action."`
		ImmediateShutdown bool                    `long:"immediate_shutdown" description:"True if the worker should shut down immediately on a sigterm."`
		Cache             CacheOpts               `group:"Options controlling caching" namespace:"cache"`
		Storage           StorageOpts             `group:"Options controlling communication with the CAS server" namespace:"storage"`
		Redis             redis.Opts              `group:"Options controlling connection to Redis" namespace:"redis"`
		Queues            struct {
			RequestQueue  string         `short:"q" long:"request_queue" env:"WORKER_REQUEST_QUEUE" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
			ResponseQueue string         `short:"r" long:"response_queue" required:"true" env:"WORKER_RESPONSE_QUEUE" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
			AckExtension  flags.Duration `long:"ack_extension" description:"Period to extend the ack deadline by during execution. Only has any effect on gcppubsub queues."`
		} `group:"Options controlling the pub/sub queues"`
	} `command:"worker" description:"Start as a worker"`
	Dual struct {
		GRPC            grpcutil.Opts           `group:"Options controlling the gRPC server"`
		Dir             string                  `short:"d" long:"dir" default:"plz-out/mettle" description:"Base directory to run actions in"`
		NoClean         bool                    `long:"noclean" env:"METTLE_NO_CLEAN" description:"Don't clean workdirs after actions complete"`
		PreflightAction bool                    `long:"preflight_action" description:"Run a pre-flight test action before starting to process work"`
		NumWorkers      int                     `short:"n" long:"num_workers" env:"METTLE_NUM_WORKERS" description:"Number of workers to run in parallel"`
		Browser         string                  `long:"browser" description:"Base URL for browser service (only used to construct informational user messages"`
		Lucidity        string                  `long:"lucidity" description:"URL of Lucidity server to report to"`
		Sandbox         string                  `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox      string                  `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout         flags.Duration          `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		MinDiskSpace    flags.ByteSize          `long:"min_disk_space" default:"1G" description:"Don't accept builds unless at least this much disk space is available"`
		MemoryThreshold float64                 `long:"memory_threshold" default:"100.0" description:"Don't accept builds unless available memory is under this percentage"`
		ConnCheck       string                  `long:"connectivity_check" choice:"gstatic" choice:"firefox" description:"Run an HTTP connectivity check periodically to verify if HTTP access is working"`
		ConnCheckPeriod flags.Duration          `long:"connectivity_check_period" default:"1h" description:"Periodicity to re-check connectivity at"`
		VersionFile     string                  `long:"version_file" description:"File containing version tag"`
		Costs           map[string]cli.Currency `long:"cost" description:"Per-second costs to associate with each build action."`
		Cache           CacheOpts               `group:"Options controlling caching" namespace:"cache"`
		Storage         struct {
			Storage []string `short:"s" long:"storage_url" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
			TLS     bool     `long:"tls" description:"Use TLS for communication with the storage server"`
		}
		Redis           redis.Opts          `group:"Options controlling connection to Redis" namespace:"redis"`
		AllowedPlatform map[string][]string `long:"allowed_platform" description:"Allowed values for platform properties"`
	} `command:"dual" description:"Start as both API server and worker. For local testing only."`
	One struct {
		Args struct {
			Actions []cli.Action `positional-arg-name:"action" required:"true" description:"The action digest to run"`
		} `positional-args:"true"`
		Dir         string         `short:"d" long:"dir" default:"." description:"Directory to run actions in"`
		Sandbox     string         `long:"sandbox" description:"Location of tool to sandbox build actions with"`
		AltSandbox  string         `long:"alt_sandbox" description:"Location of tool to sandbox build actions with that don't explicitly request it"`
		Timeout     flags.Duration `long:"timeout" hidden:"true" description:"Deprecated, has no effect."`
		ProfileFile string         `long:"profile_file" hidden:"true" description:"Write a CPU profile to this file"`
		MemProfile  string         `long:"mem_profile_file" hidden:"true" description:"Write a memory profile to this file"`
		Cache       CacheOpts      `group:"Options controlling caching" namespace:"cache"`
		Storage     StorageOpts    `group:"Options controlling communication with the CAS server"`
		Redis       redis.Opts     `group:"Options controlling connection to Redis" namespace:"redis"`
	} `command:"one" description:"Executes a single build action, identified by its action digest."`
	Admin cli.AdminOpts `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Mettle is an implementation of the execution service of the Remote Execution API.
It does not implement the storage APIs itself; it must be given the location of another
server to provide that.

It can be configured in one of two modes; either as an API server or a worker. The
API server provides the gRPC API that others contact to request execution of tasks.
Meanwhile the workers perform the actual execution of tasks.

The two server types communicate via a pub/sub queue. The only configuration usefully
supported for this is using GCP Cloud Pub/Sub via a gcppubsub:// URL. The queues and
subscriptions must currently be set up manually.
For testing purposes it can be configured in a dual setup where it performs both
roles and in this setup it can be set up with an in-memory queue using a mem:// URL.
Needless to say, this mode does not synchronise with any other servers.

The specific usage of pubsub bears some note; both worker and api servers have long-running
stateful operations and hence should not be casually restarted. In the case of the worker
it will stop receiving new requests when sent a signal, but should be waited for its
current operation to complete before being terminated. Unfortunately the length of time
required is outside of our control (since timeouts on actions are set by clients) so we
cannot give a hard limit of time required.
For the master, it has no persistent storage, but all servers register to receive all
events. Hence when a server restarts it will not have knowledge of all currently running
jobs until an update is sent for each; the suggestion here is to wait for > 1 minute
(after which each live job should have sent an update, and the new server will know about
it). This is easy to arrange in a managed environment like Kubernetes.

The worker supports a file-based cache; this is populated initially using the 'cache'
command and is not updated by it at runtime. This works off the observation that total
downloaded bytes probably follow a power law distribution with a few relatively rarely
updated blobs dominating much of the data downloaded.
`,
}

func main() {
	cmd, info := cli.ParseFlagsOrDie("Mettle", &opts, &opts.Logging)

	initLogrusFromVerbosity(opts.Logging.Verbosity, opts.Logging.Structured)

	if cmd != "one" {
		go cli.ServeAdmin(fmt.Sprintf("mettle-%s", cmd), opts.Admin, info)
	}

	if cmd == "dual" {
		const requests = "mem://requests"
		const responses = "mem://responses"
		queues := api.PubSubOpts{
			RequestQueue:          requests,
			ResponseQueue:         responses,
			PreResponseQueue:      responses,
			NumPollers:            1,
			SubscriptionBatchSize: 100,
		}
		primaryRedis, readRedis := opts.Dual.Redis.Clients()

		// Must ensure the topics are created ahead of time.
		common.MustOpenTopic(requests)
		common.MustOpenTopic(responses)
		if opts.Dual.NumWorkers == 0 {
			opts.Dual.NumWorkers = runtime.NumCPU()
		}
		for i := 0; i < opts.Dual.NumWorkers; i++ {
			storage := opts.Dual.Storage.Storage[i%len(opts.Dual.Storage.Storage)]
			dir := filepath.Join(opts.Dual.Dir, fmt.Sprintf("worker_%02d", i))
			go worker.RunForever(worker.Opts{
				InstanceName:      opts.InstanceName,
				RequestQueue:      requests + "?ackdeadline=10m",
				ResponseQueue:     responses,
				Name:              fmt.Sprintf("%s-%d", opts.InstanceName, i),
				Storage:           storage,
				SecureStorage:     opts.Dual.Storage.TLS,
				TokenFile:         opts.Dual.GRPC.TokenFile,
				Dir:               dir,
				Clean:             !opts.Dual.NoClean,
				CacheDir:          opts.Dual.Cache.Dir,
				CachePrefix:       opts.Dual.Cache.Prefix,
				CacheParts:        opts.Dual.Cache.Part,
				MaxCacheSize:      int64(opts.Dual.Cache.MaxMem),
				BrowserURL:        opts.Dual.Browser,
				Sandbox:           opts.Dual.Sandbox,
				AltSandbox:        opts.Dual.AltSandbox,
				Lucidity:          opts.Dual.Lucidity,
				PrimaryRedis:      primaryRedis,
				ReadRedis:         readRedis,
				LargeBlobSize:     opts.Worker.Redis.MaxSize,
				MinDiskSpace:      int64(opts.Dual.MinDiskSpace),
				MemoryThreshold:   opts.Dual.MemoryThreshold,
				VersionFile:       opts.Dual.VersionFile,
				Costs:             opts.Dual.Costs,
				PreflightAction:   opts.Dual.PreflightAction,
				ConnCheck:         opts.Dual.ConnCheck,
				ConnCheckPeriod:   time.Duration(opts.Dual.ConnCheckPeriod),
				ImmediateShutdown: opts.Worker.ImmediateShutdown,
			})
		}
		api.ServeForever(api.Opts{
			GRPC:            opts.Dual.GRPC,
			Queues:          queues,
			AllowedPlatform: opts.Dual.AllowedPlatform,
			StorageURL:      opts.Dual.Storage.Storage[0],
			StorageTLS:      opts.Dual.Storage.TLS,
		})
	} else if cmd == "worker" {
		primaryRedis, readRedis := opts.Worker.Redis.Clients()
		worker.RunForever(worker.Opts{
			InstanceName:      opts.InstanceName,
			RequestQueue:      opts.Worker.Queues.RequestQueue,
			ResponseQueue:     opts.Worker.Queues.ResponseQueue,
			Name:              opts.Worker.Name,
			Storage:           opts.Worker.Storage.Storage,
			SecureStorage:     opts.Worker.Storage.TLS,
			TokenFile:         opts.Worker.Storage.TokenFile,
			Dir:               opts.Worker.Dir,
			Clean:             !opts.Worker.NoClean,
			CacheDir:          opts.Worker.Cache.Dir,
			CachePrefix:       opts.Worker.Cache.Prefix,
			CacheParts:        opts.Worker.Cache.Part,
			MaxCacheSize:      int64(opts.Worker.Cache.MaxMem),
			BrowserURL:        opts.Worker.Browser,
			Sandbox:           opts.Worker.Sandbox,
			AltSandbox:        opts.Worker.AltSandbox,
			Lucidity:          opts.Worker.Lucidity,
			PromGatewayURL:    opts.Worker.PromGateway,
			PrimaryRedis:      primaryRedis,
			ReadRedis:         readRedis,
			LargeBlobSize:     opts.Worker.Redis.MaxSize,
			MinDiskSpace:      int64(opts.Worker.MinDiskSpace),
			MemoryThreshold:   opts.Worker.MemoryThreshold,
			VersionFile:       opts.Worker.VersionFile,
			Costs:             opts.Worker.Costs,
			AckExtension:      time.Duration(opts.Worker.Queues.AckExtension),
			PreflightAction:   opts.Worker.PreflightAction,
			ConnCheck:         opts.Worker.ConnCheck,
			ConnCheckPeriod:   time.Duration(opts.Worker.ConnCheckPeriod),
			ImmediateShutdown: opts.Worker.ImmediateShutdown,
		})
	} else if cmd == "api" {
		api.ServeForever(api.Opts{
			GRPC:            opts.API.GRPC,
			Name:            opts.API.Queues.ResponseQueueSuffix,
			Queues:          opts.API.Queues,
			APIURL:          opts.API.API.URL,
			APITLS:          opts.API.API.TLS,
			AllowedPlatform: opts.API.AllowedPlatform,
			StorageURL:      opts.API.Storage.Storage,
			StorageTLS:      opts.API.Storage.TLS,
		})
	} else if err := one(); err != nil {
		log.Fatalf("%s", err)
	}
}

func one() error {
	if opts.One.ProfileFile != "" {
		f, err := os.Create(opts.One.ProfileFile)
		if err != nil {
			log.Fatalf("Failed to open profile file: %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("could not start profiler: %s", err)
		}
		defer f.Close()
		defer pprof.StopCPUProfile()
	}
	if opts.One.MemProfile != "" {
		f, err := os.Create(opts.One.MemProfile)
		if err != nil {
			log.Fatalf("Failed to open memory profile file: %s", err)
		}
		defer f.Close()
		defer pprof.WriteHeapProfile(f)
	}
	primaryRedis, readRedis := opts.One.Redis.Clients()
	for _, action := range opts.One.Args.Actions {
		if err := worker.RunOne(worker.Opts{
			InstanceName:  opts.InstanceName,
			Name:          "mettle-one",
			Storage:       opts.One.Storage.Storage,
			SecureStorage: opts.One.Storage.TLS,
			TokenFile:     opts.One.Storage.TokenFile,
			Dir:           opts.One.Dir,
			CacheDir:      opts.One.Cache.Dir,
			CachePrefix:   opts.One.Cache.Prefix,
			CacheParts:    opts.One.Cache.Part,
			Sandbox:       opts.One.Sandbox,
			AltSandbox:    opts.One.AltSandbox,
			PrimaryRedis:  primaryRedis,
			ReadRedis:     readRedis,
			LargeBlobSize: opts.Worker.Redis.MaxSize,
		}, action.ToProto()); err != nil {
			return err
		}
	}
	return nil
}

// initLogrusFromVerbosity creates instance of Logrus and populates verbosity level
func initLogrusFromVerbosity(v logging.Verbosity, structured bool) {
	var level logrus.Level

	switch {
	case v <= 0:
		level = logrus.ErrorLevel
	case v == 1:
		level = logrus.WarnLevel
	case v == 2 || v == 3:
		// logrus doesn't have notice level, so use info
		level = logrus.InfoLevel
	case v >= 4:
		level = logrus.DebugLevel
	}

	logrus.SetLevel(level)
	logrus.SetOutput(os.Stderr)

	if structured {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "15:04:05.000",
		})
	}

	logrus.WithFields(logrus.Fields{
		"verbosity":  int(v),
		"level":      logrus.GetLevel().String(),
		"structured": structured,
		"out":        fmt.Sprintf("%T", logrus.StandardLogger().Out),
	}).Info("logrus initialised")
}
