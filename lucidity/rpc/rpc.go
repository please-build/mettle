package rpc

import (
	"context"
	"embed"
	"fmt"
	"mime"
	"net/http"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/please-build/mettle/grpcutil"
	pb "github.com/please-build/mettle/proto/lucidity"
)

var log = logging.MustGetLogger()

//go:embed static/*.*
var embeddedData embed.FS

var liveWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "live_workers_total",
})

var deadWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "dead_workers_total",
})

var healthyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "healthy_workers_total",
})

var unhealthyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "unhealthy_workers_total",
})

var busyWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "lucidity",
	Name:      "busy_workers_total",
})

// ServeForever serves on the given port until terminated.
func ServeForever(ctx context.Context, opts grpcutil.Opts, httpPort int, maxAge time.Duration, minProportion float64, audience string, allowedUsers []string) {
	srv := newServer(minProportion)
	prometheus.MustRegister(srv)
	lis, s := grpcutil.NewServer(ctx, opts)
	pb.RegisterLucidityServer(s, srv)

	mux := http.NewServeMux()
	mux.HandleFunc("/workers", srv.ServeWorkers)
	mux.HandleFunc("/", srv.ServeAsset)
	mux.HandleFunc("/disable", srv.ServeDisable)
	authed := maybeAddValidation(ctx, mux, audience, allowedUsers)
	log.Notice("Serving HTTP on %s:%d", opts.Host, httpPort)
	go http.ListenAndServe(fmt.Sprintf("%s:%d", opts.Host, httpPort), authed)
	go srv.Clean(maxAge)
	grpcutil.ServeForever(lis, s)
}

// server implements prometheus.Collector, which allows us to compute metrics
// on the fly.
type server struct {
	pb.UnimplementedLucidityServer
	workers, validVersions sync.Map
	minProportion          float64
}

func newServer(minProportion float64) *server {
	return &server{
		minProportion: minProportion,
	}
}

func (s *server) Update(ctx context.Context, req *pb.Worker) (*pb.UpdateResponse, error) {
	req.UpdateTime = time.Now().Unix()
	v, ok := s.workers.Load(req.Name)
	req.Disabled = ok && v.(*pb.Worker).Disabled
	s.workers.Store(req.Name, req)
	if !ok || v.(*pb.Worker).Version != req.Version {
		s.recalculateValidVersions()
	}
	s.checkWorkerHealth(req)
	return &pb.UpdateResponse{ShouldDisable: req.Disabled || !req.Healthy}, nil
}

func (s *server) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	return s.listWorkers(req), nil
}

func (s *server) listWorkers(req *pb.ListWorkersRequest) *pb.ListWorkersResponse {
	workers := &pb.ListWorkersResponse{}
	s.workers.Range(func(k, v interface{}) bool {
		r := v.(*pb.Worker)
		if (req.Hostname == "" || r.Hostname == req.Hostname) && (req.Name == "" || r.Name == req.Name) {
			s.checkWorkerHealth(r)
			workers.Workers = append(workers.Workers, r)
		}
		return true
	})
	slices.SortFunc(workers.Workers, func(a, b *pb.Worker) int {
		if a.Name < b.Name {
			return -1
		} else if a.Name > b.Name {
			return 1
		}
		return 0
	})
	return workers
}

func (s *server) checkWorkerHealth(req *pb.Worker) {
	if req.UpdateTime < time.Now().Add(-10*time.Minute).Unix() {
		req.Healthy = false
		req.Status = "Too long since last update"
	} else if !s.IsValidVersion(req.Version) {
		req.Healthy = false
		req.Status = "Invalid version"
	}
}

func (s *server) ServeWorkers(w http.ResponseWriter, r *http.Request) {
	workers := s.listWorkers(&pb.ListWorkersRequest{})
	m := jsonpb.Marshaler{
		OrigName: true,
		Indent:   "  ",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	m.Marshal(w, workers)
}

func (s *server) ServeAsset(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "" {
		r.URL.Path = "index.html"
	}
	asset, err := embeddedData.ReadFile(path.Join("static", r.URL.Path))
	if err != nil {
		log.Warning("404 for URL %s", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", mime.TypeByExtension(path.Ext(r.URL.Path)))
	w.WriteHeader(http.StatusOK)
	w.Write(asset)
}

func (s *server) ServeDisable(w http.ResponseWriter, r *http.Request) {
	req := &pb.Disable{}
	if r.Method != http.MethodPost {
		log.Warning("Rejecting request to %s with method %s", r.URL.Path, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
	} else if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		log.Warning("Invalid request: %s", err)
		w.WriteHeader(http.StatusBadRequest)
	} else if v, ok := s.workers.Load(req.Name); !ok {
		log.Warning("Request to disable unknown worker %s", req.Name)
		w.WriteHeader(http.StatusNotFound)
	} else {
		v.(*pb.Worker).Disabled = req.Disable
	}
}

// Clean periodically checks all known workers and discards any older than the given duration.
func (s *server) Clean(maxAge time.Duration) {
	if maxAge <= 0 {
		return
	}
	for range time.NewTicker(maxAge / 10).C {
		min := time.Now().Add(-maxAge).Unix()
		s.workers.Range(func(k, v interface{}) bool {
			if v.(*pb.Worker).UpdateTime < min {
				go s.removeWorker(k.(string))
			}
			return true
		})
	}
}

func (s *server) removeWorker(key string) {
	s.workers.Delete(key)
}

// recalculateValidVersions runs through all the workers and calculates the new set of versions we'll allow to be active.
func (s *server) recalculateValidVersions() {
	// Collect counts of all known versions
	counts := map[string]int{}
	n := 0
	s.workers.Range(func(k, v interface{}) bool {
		counts[v.(*pb.Worker).Version]++
		n++
		return true
	})
	// Now add any of those over the threshold to the set of valid versions
	min := int(s.minProportion * float64(n))
	log.Notice("Updating valid versions, total workers: %d, min threshold: %d", n, min)
	for k, v := range counts {
		if v < min {
			log.Info("  %s: %d (invalid)", k, v)
		} else {
			log.Info("  %s: %d", k, v)
			s.validVersions.Store(k, v)
		}
	}
	// Delete any old versions that were valid but are no longer.
	s.validVersions.Range(func(k, v interface{}) bool {
		if counts[k.(string)] < min {
			log.Notice("Disabling version %s", k)
			s.validVersions.Delete(k)
		}
		return true
	})
}

// IsValidVersion returns true if this version is currently live.
func (s *server) IsValidVersion(v string) bool {
	_, valid := s.validVersions.Load(v)
	return valid
}

// Describe sends prometheus description of all metrics collected in Collect.
func (s *server) Describe(out chan<- *prometheus.Desc) {
	liveWorkers.Describe(out)
	deadWorkers.Describe(out)
	healthyWorkers.Describe(out)
	unhealthyWorkers.Describe(out)
	busyWorkers.Describe(out)
}

// Collect counts number of unhealthy, dead and busy workers and send the
// corresponding metrics. It also checks the worker health on the fly, in case
// the version or the updateTime is no longer valid.
func (s *server) Collect(out chan<- prometheus.Metric) {
	var total, unhealthy, dead, busy float64
	s.workers.Range(func(_, v interface{}) bool {
		r := v.(*pb.Worker)
		s.checkWorkerHealth(r)
		if !r.Healthy {
			unhealthy++
		}
		if !r.Alive {
			dead++
		}
		if r.Busy {
			busy++
		}
		total++
		return true
	})
	liveWorkers.Set(total - dead)
	deadWorkers.Set(dead)
	healthyWorkers.Set(total - unhealthy)
	unhealthyWorkers.Set(unhealthy)
	busyWorkers.Set(busy)

	liveWorkers.Collect(out)
	deadWorkers.Collect(out)
	healthyWorkers.Collect(out)
	unhealthyWorkers.Collect(out)
	busyWorkers.Collect(out)
}
