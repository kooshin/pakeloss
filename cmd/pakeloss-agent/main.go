package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"pakeloss/internal/agent"
	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

func main() {
	configPath := flag.String("config", "", "optional agent config path")
	agentID := flag.String("agent-id", "", "agent ID")
	controllerAddr := flag.String("controller-addr", "", "controller gRPC address")
	controllerVRF := flag.String("controller-vrf", "", "controller VRF")
	token := flag.String("token", "", "controller token")
	udpListen := flag.String("udp-listen", "", "UDP listen address")
	udpAdvertise := flag.String("udp-advertise", "", "UDP advertise address")
	udpListenVRF := flag.String("udp-listen-vrf", "", "UDP listen VRF")
	onControllerDisconnect := flag.String("on-controller-disconnect", "", "continue or stop")
	flag.Parse()

	var cfg model.AgentConfig
	var err error
	if *configPath != "" {
		cfg, err = agent.LoadConfig(*configPath)
		if err != nil {
			log.Fatal(err)
		}
	}
	if *agentID != "" {
		cfg.AgentID = *agentID
	}
	if *controllerAddr != "" {
		cfg.ControllerAddr = *controllerAddr
	}
	if *controllerVRF != "" {
		cfg.ControllerVRF = *controllerVRF
	}
	if *token != "" {
		cfg.Token = *token
	}
	if *udpListen != "" {
		cfg.ListenAddr = *udpListen
	}
	if *udpAdvertise != "" {
		cfg.AdvertiseAddr = *udpAdvertise
	}
	if *udpListenVRF != "" {
		cfg.ListenVRF = *udpListenVRF
	}
	if *onControllerDisconnect != "" {
		cfg.OnControllerDisconnect = *onControllerDisconnect
	}
	agent.FinalizeConfig(&cfg)
	if err := validateAgentConfig(cfg); err != nil {
		log.Fatal(err)
	}
	results := make(chan *pb.ResultReport, 1024)
	manager := agent.NewFlowManager(cfg.AgentID, cfg.ListenVRF)
	manager.SetResultReports(results)
	receiver := agent.NewUDPReceiver(cfg.AgentID, cfg.ListenAddr, results, cfg.ListenVRF)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := receiver.Run(ctx); err != nil && ctx.Err() == nil {
			log.Fatal(err)
		}
	}()
	client := agent.NewControlClient(cfg, manager, receiver, results)
	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func validateAgentConfig(cfg model.AgentConfig) error {
	if cfg.AgentID == "" || cfg.ControllerAddr == "" || cfg.ListenAddr == "" {
		return fmt.Errorf("agent-id, controller-addr, and udp-listen are required")
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return fmt.Errorf("controller token is required")
	}
	return nil
}
