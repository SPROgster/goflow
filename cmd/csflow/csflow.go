package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"

	"github.com/cloudflare/goflow/v3/producer"
	"github.com/cloudflare/goflow/v3/transport"
	"github.com/cloudflare/goflow/v3/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	version    = ""
	buildinfos = ""
	AppVersion = "GoFlow sFlow " + version + " " + buildinfos

	Addr  = flag.String("addr", "", "sFlow listening address")
	Port  = flag.Int("port", 6343, "sFlow listening port")
	Reuse = flag.Bool("reuse", false, "Enable so_reuseport for sFlow listening port")

	Workers  = flag.Int("workers", 1, "Number of sFlow workers")
	LogLevel = flag.String("loglevel", "info", "Log level")

	MetricsAddr = flag.String("metrics.addr", ":8080", "Metrics address")
	MetricsPath = flag.String("metrics.path", "/metrics", "Metrics path")

	SFlowSample = flag.Bool("sflow.raw.sample", false, "Copy sFlow sample header if present")

	Version = flag.Bool("v", false, "Print version")
)

func init() {
	transport.RegisterFlags()
}

func httpServer() {
	http.Handle(*MetricsPath, promhttp.Handler())
	log.Fatal(http.ListenAndServe(*MetricsAddr, nil))
}

func main() {
	flag.Parse()

	if *Version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	lvl, _ := log.ParseLevel(*LogLevel)
	log.SetLevel(lvl)

	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Info("Starting GoFlow")

	s := &utils.StateSFlow{
		Logger: log.StandardLogger(),
	}

	t, err := transport.CreateTransport()
	if err != nil {
		log.Fatal(err)
	}
	s.Transport = *t

	if *SFlowSample {
		s.Config = &producer.SFlowProducerConfig{
			RawSample: true,
		}
	}

	go httpServer()

	log.WithFields(log.Fields{
		"Type": "sFlow"}).
		Infof("Listening on UDP %v:%v", *Addr, *Port)

	err = s.FlowRoutine(*Workers, *Addr, *Port, *Reuse)
	if err != nil {
		log.Fatalf("Fatal error: could not listen to UDP (%v)", err)
	}
}
