package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"pakeloss/internal/controller"
	"pakeloss/internal/model"
)

func main() {
	configPath := flag.String("config", "", "optional controller config path")
	grpcAddr := flag.String("grpc-addr", "", "controller gRPC address")
	httpAddr := flag.String("http-addr", "", "controller HTTP address")
	resultCSV := flag.String("result-csv", "", "controller CSV result path")
	resultJSONL := flag.String("result-jsonl", "", "controller JSONL result path")
	resultDebugJSONL := flag.String("result-debug-jsonl", "", "controller JSONL seq debug path")
	resultSummaryCSV := flag.String("result-summary-csv", "", "controller CSV session summary path")
	resultSummaryJSONL := flag.String("result-summary-jsonl", "", "controller JSONL session summary path")
	outageEventCSV := flag.String("result-outage-event-csv", "", "controller CSV outage event path")
	outageEventJSONL := flag.String("result-outage-event-jsonl", "", "controller JSONL outage event path")
	resultFlushInterval := flag.String("result-flush-interval", "", "controller result log flush interval")
	logTimezone := flag.String("log-timezone", "", "controller log timezone")
	token := flag.String("token", "", "controller token")
	flowIntervalMs := flag.String("flow-interval-ms", "", "default flow interval in ms")
	flowPacketSize := flag.String("flow-packet-size", "", "default flow packet size in bytes")
	flowSourcePortCount := flag.String("flow-source-port-count", "", "default flow source port count")
	flowLossConfirmWindowMs := flag.String("flow-loss-confirm-window-ms", "", "default flow loss confirm window in ms")
	flowState := flag.String("flow-state", "", "default flow state")
	outageThresholdMs := flag.String("outage-threshold-ms", "", "consecutive loss threshold in ms")
	flag.Parse()

	var cfg model.ControllerConfig
	var err error
	if *configPath != "" {
		cfg, err = controller.LoadControllerConfig(*configPath)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *grpcAddr != "" {
		cfg.GRPCAddr = *grpcAddr
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}
	if *resultCSV != "" {
		cfg.ResultCSV = *resultCSV
	}
	if *resultJSONL != "" {
		cfg.ResultJSONL = *resultJSONL
	}
	if *resultDebugJSONL != "" {
		cfg.ResultDebugJSONL = *resultDebugJSONL
	}
	if *resultSummaryCSV != "" {
		cfg.ResultSummaryCSV = *resultSummaryCSV
	}
	if *resultSummaryJSONL != "" {
		cfg.ResultSummaryJSONL = *resultSummaryJSONL
	}
	if *outageEventCSV != "" {
		cfg.OutageEventCSV = *outageEventCSV
	}
	if *outageEventJSONL != "" {
		cfg.OutageEventJSONL = *outageEventJSONL
	}
	if *resultFlushInterval != "" {
		cfg.ResultFlushInterval = *resultFlushInterval
	}
	if *logTimezone != "" {
		cfg.LogTimezone = *logTimezone
	}
	if *token != "" {
		cfg.Token = *token
	}
	if *flowIntervalMs != "" {
		cfg.FlowDefaults.IntervalMs = mustParseUint32("flow-interval-ms", *flowIntervalMs)
	}
	if *flowPacketSize != "" {
		cfg.FlowDefaults.PacketSize = mustParseUint32("flow-packet-size", *flowPacketSize)
	}
	if *flowSourcePortCount != "" {
		cfg.FlowDefaults.SourcePortCount = mustParseUint32("flow-source-port-count", *flowSourcePortCount)
	}
	if *flowLossConfirmWindowMs != "" {
		cfg.FlowDefaults.LossConfirmWindowMs = mustParseUint32("flow-loss-confirm-window-ms", *flowLossConfirmWindowMs)
	}
	if *flowState != "" {
		cfg.FlowDefaults.State = *flowState
	}
	if *outageThresholdMs != "" {
		cfg.OutageThresholdMs = mustParseUint32("outage-threshold-ms", *outageThresholdMs)
	}
	controller.FinalizeControllerConfig(&cfg)
	if err := validateControllerConfig(cfg); err != nil {
		log.Fatal(err)
	}
	mesh := controller.NewRuntimeMesh(cfg.FlowDefaults)
	mesh.OutageThresholdMs = cfg.OutageThresholdMs
	results, err := controller.NewResultStore(cfg.ResultCSV, cfg.ResultJSONL, cfg.ResultSummaryCSV, cfg.ResultSummaryJSONL, cfg.LogTimezone, cfg.ResultFlushInterval)
	if err != nil {
		log.Fatal(err)
	}
	results.SetDebugJSONLPath(cfg.ResultDebugJSONL)
	results.SetOutageEventPaths(cfg.OutageEventCSV, cfg.OutageEventJSONL)
	results.SetReportFinalizeDelay(cfg.ReportFinalizeDelayDuration())
	defer results.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := controller.NewServer(cfg, mesh, results)
	defer srv.CloseActiveOutages("session_stopped")
	errCh := make(chan error, 2)
	go srv.RunRuntimeMonitor(ctx)
	go results.Run(ctx)
	go func() { errCh <- srv.ListenAndServeGRPC(ctx) }()
	go func() { errCh <- srv.ListenAndServeHTTP(ctx) }()

	if err := <-errCh; err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func validateControllerConfig(cfg model.ControllerConfig) error {
	if strings.TrimSpace(cfg.Token) == "" {
		return fmt.Errorf("controller token is required")
	}
	return nil
}

func mustParseUint32(name, value string) uint32 {
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		log.Fatalf("invalid --%s: %v", name, err)
	}
	return uint32(n)
}
