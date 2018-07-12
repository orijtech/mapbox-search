package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/groupcache"
	"github.com/orijtech/mapbox"
	"github.com/orijtech/otils"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

func main() {
	addr := flag.String("addr", ":0", "the address on which to run the HTTPPool")
	peersCSV := flag.String("peers-csv", "", "the peers' full HTTP addresses separated by commas e.g http://localhost:9877,http://localhost:localhost:9878")
	flag.Parse()

	if err := enableOpenCensus(); err != nil {
		log.Fatalf("Failed to enable OpenCensus: %v", err)
	}
	peersList := strings.Split(*peersCSV, ",")

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to bind to address: %q error: %v", *addr, err)
	}
	defer ln.Close()

	// Register groups
	registerGroups()

	httpAddr := fmt.Sprintf("http://%s", ln.Addr().String())
	pool = groupcache.NewHTTPPoolOpts(httpAddr, &groupcache.HTTPPoolOptions{BasePath: "/"})
	if len(peersList) > 0 {
		pool.Set(peersList...)
	}

	log.Printf("HTTP address: %s", httpAddr)
	mux := http.NewServeMux()
	mux.Handle("/", pool)
	mux.HandleFunc("/latlon", byLatLon)
	mux.HandleFunc("/name", byName)
	mux.HandleFunc("/setpeers", setPeers)

	h := &ochttp.Handler{Handler: mux}
	if err := http.Serve(ln, h); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

type lookupQuery struct {
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
	Name string  `json:"name"`
}

func byLatLon(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "byLatLon")
	defer span.End()

	lookup(ctx, latLonLookupName, w, r)
}

func byName(w http.ResponseWriter, r *http.Request) {
	ctx, span := trace.StartSpan(r.Context(), "byName")
	defer span.End()

	lookup(ctx, addressLookupName, w, r)
}

func lookup(ctx context.Context, groupName string, w http.ResponseWriter, r *http.Request) {
	ld := new(lookupQuery)
	if err := parseJSON(r.Body, ld); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var group *groupcache.Group
	var key string
	switch strings.ToLower(groupName) {
	case addressLookupName:
		group = byNameGroup
		key = ld.Name
	case latLonLookupName:
		group = byLatLonGroup
		key = fmt.Sprintf("%.6f,%.6f", ld.Lat, ld.Lon)
	default:
		http.Error(w, fmt.Sprintf("unknown group %q", groupName), http.StatusBadRequest)
		return
	}

	var data []byte
	if err := group.Get(ctx, key, groupcache.AllocatingByteSliceSink(&data)); err != nil {
		log.Printf("Lookup error: %v key: %q\n", err, key)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

var poolMu sync.Mutex
var pool *groupcache.HTTPPool

type peers struct {
	GroupName string   `json:"group_name"`
	Peers     []string `json:"peers"`
}

func setPeers(w http.ResponseWriter, r *http.Request) {
	ps := new(peers)
	if err := parseJSON(r.Body, ps); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	poolMu.Lock()
	pool.Set(ps.Peers...)
	poolMu.Unlock()
}

func parseJSON(rc io.ReadCloser, save interface{}) error {
	blob, err := ioutil.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, save)
}

const (
	addressLookupName = "address_lookup"
	latLonLookupName  = "lat_lon_lookup"
)

var mapboxClient *mapbox.Client

func init() {
	var err error
	mapboxClient, err = mapbox.NewClient()
	if err != nil {
		log.Fatalf("Failed to create a mapbox client: %v", err)
	}
}

var (
	byNameGroup, byLatLonGroup *groupcache.Group
)

func registerGroups() {
	// For address lookup
	byNameGroup = groupcache.NewGroup(addressLookupName, 1<<31, groupcache.GetterFunc(func(ctx context.Context, placeName string, sink groupcache.Sink) error {
		ctx, span := trace.StartSpan(ctx, "name_lookup")
		defer span.End()

		// The key is a string descriptive of a place for example "Olduvai Gorge" or "Palo Alto"
		match, err := mapboxClient.LookupPlace(ctx, placeName)
		if err != nil {
			return err
		}
		blob, err := json.Marshal(match)
		if err != nil {
			return err
		}
		sink.SetBytes(blob)
		return nil
	}))

	// For lat_lon lookup
	byLatLonGroup = groupcache.NewGroup(latLonLookupName, 1<<31, groupcache.GetterFunc(func(ctx context.Context, latLonCSV string, sink groupcache.Sink) error {
		ctx, span := trace.StartSpan(ctx, "lat_lon_lookup")
		defer span.End()

		// latLonCSV is a latitude longitude pair separated by a comma, in the form: <lat>,<lon>
		splits := strings.Split(latLonCSV, ",")
		if len(splits) < 2 {
			return errors.New("expecting <lat>,<lon>")
		}
		lat, err := strconv.ParseFloat(splits[0], 32)
		if err != nil {
			return fmt.Errorf("failed to parse latitude: %v", err)
		}
		lon, err := strconv.ParseFloat(splits[1], 32)
		if err != nil {
			return fmt.Errorf("failed to parse longitude: %v", err)
		}
		match, err := mapboxClient.LookupLatLon(ctx, lat, lon)
		if err != nil {
			return err
		}
		blob, err := json.Marshal(match)
		if err != nil {
			return err
		}
		sink.SetBytes(blob)
		return nil
	}))
}

func enableOpenCensus() error {
	sd, err := stackdriver.NewExporter(stackdriver.Options{
		MetricPrefix: "mapboxsearch",
		ProjectID:    otils.EnvOrAlternates("MAPBOXSEARCH_GCP_PROJECTID", "census-demos"),
	})
	if err != nil {
		return err
	}
	if err := view.Register(ochttp.DefaultServerViews...); err != nil {
		return fmt.Errorf("failed to register default server views: %v", err)
	}
	if err := view.Register(ochttp.DefaultClientViews...); err != nil {
		return fmt.Errorf("failed to register default client views: %v", err)
	}
	// Enable groupcache metrics
	if err := view.Register(groupcache.AllViews...); err != nil {
		return fmt.Errorf("failed to register groupcache views: %v", err)
	}
	view.RegisterExporter(sd)
	trace.RegisterExporter(sd)
	return nil
}
