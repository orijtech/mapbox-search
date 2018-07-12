package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/orijtech/mapbox"
	"github.com/orijtech/otils"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

var httpClient = &http.Client{Transport: &ochttp.Transport{}}

func main() {
	serverAddr := flag.String("server_url", "http://localhost:8777", "the server of the mapbox search cluster")
	flag.Parse()

	if err := enableOpenCensus(); err != nil {
		log.Fatalf("Failed to enable OpenCensus: %v", err)
	}
	mc := &client{addr: *serverAddr}

	br := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		line, _, err := br.ReadLine()
		if err != nil {
			log.Fatalf("Failed to read a line in: %v", err)
		}
		mres, err := mc.LookupByName(context.Background(), string(line))
		if err != nil {
			log.Fatalf("Failed to search for place: %v", err)
		}
		prettyPrintResults(mres)
	}
}

type client struct {
	addr string
}

type query struct {
	Name string  `json:"name"`
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
}

func (c *client) LookupByLatLon(ctx context.Context, lat, lon float64) (*mapbox.GeocodeResponse, error) {
	ctx, span := trace.StartSpan(ctx, "(*client).LookupByLatLon")
	defer span.End()

	return c.doRequest(ctx, "latlon", &query{Lat: lat, Lon: lon})
}

func (c *client) LookupByName(ctx context.Context, name string) (*mapbox.GeocodeResponse, error) {
	ctx, span := trace.StartSpan(ctx, "(*client).LookupByName")
	defer span.End()

	return c.doRequest(ctx, "name", &query{Name: name})
}

func (c *client) doRequest(ctx context.Context, relativePath string, q *query) (*mapbox.GeocodeResponse, error) {
	blob, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/%s", c.addr, relativePath)
	req, err := http.NewRequest("POST", url, bytes.NewReader(blob))
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	if !otils.StatusOK(res.StatusCode) {
		return nil, fmt.Errorf("%s", res.Status)
	}
	jsonBlob, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	gres := new(mapbox.GeocodeResponse)
	if err := json.Unmarshal(jsonBlob, gres); err != nil {
		return nil, err
	}
	return gres, nil
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
	view.RegisterExporter(sd)
	trace.RegisterExporter(sd)

	// For demo purposes, we are always sampling
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	return nil
}

func prettyPrintResults(mres *mapbox.GeocodeResponse) {
	if len(mres.Features) == 0 {
		fmt.Println("No results found!")
                return
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Relevance (%)", "Latitude", "Longitude"})
        table.SetRowLine(true)

	for _, feat := range mres.Features {
		lat, lon := feat.Center[0], feat.Center[1]
		table.Append([]string{
			feat.PlaceName,
			fmt.Sprintf("%.2f", feat.Relevance*100),
			fmt.Sprintf("%.4f", lat),
			fmt.Sprintf("%.4f", lon),
		})
	}
	table.Render()
}
