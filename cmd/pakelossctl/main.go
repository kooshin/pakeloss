package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"pakeloss/internal/controller"
	"pakeloss/internal/tui"
)

const defaultRefreshInterval = time.Second

type cliConfig struct {
	api             string
	token           string
	graphMode       string
	refreshInterval time.Duration
}

func main() {
	configPath := flag.String("config", "", "optional controller config path")
	api := flag.String("api", "", "controller HTTP API address")
	token := flag.String("token", "", "controller HTTP API token")
	graphMode := flag.String("graph-mode", "", "tui graph mode: unicode or ascii")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	cfg, err := resolveCLIConfig(*configPath, *api, *token, *graphMode)
	if err != nil {
		log.Fatal(err)
	}
	client := tui.NewClient(cfg.api, cfg.token)
	switch args[0] {
	case "agents":
		agents, err := client.Agents()
		if err != nil {
			log.Fatal(err)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "AGENT\tSTATUS\tENABLED\tUDP\tCONFIG\tFLOWS\tLAST_HEARTBEAT")
		for _, a := range agents {
			fmt.Fprintf(w, "%s\t%s\t%t\t%s\t%d/%d\t%d\t%s\n", a.AgentID, a.Status, a.Enabled, a.UDPAddr, a.ActiveConfigVersion, a.DesiredConfigVersion, a.ActiveFlows, a.LastHeartbeat.Format("15:04:05"))
		}
		_ = w.Flush()
	case "flows":
		flows, err := client.Flows()
		if err != nil {
			log.Fatal(err)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, strings.Join(tui.FlowTableHeaders(), "\t"))
		for _, f := range flows {
			fmt.Fprintln(w, strings.Join(tui.FlowTableValues(f), "\t"))
		}
		_ = w.Flush()
	case "tui":
		if err := tui.NewApp(client, cfg.graphMode, cfg.refreshInterval).Run(); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func resolveCLIConfig(configPath, apiFlag, tokenFlag, graphModeFlag string) (cliConfig, error) {
	cfg := cliConfig{
		api:             "127.0.0.1:8080",
		graphMode:       "unicode",
		refreshInterval: defaultRefreshInterval,
	}
	if configPath != "" {
		controllerCfg, err := controller.LoadControllerConfig(configPath)
		if err != nil {
			return cliConfig{}, err
		}
		if controllerCfg.HTTPAddr != "" {
			cfg.api = controllerCfg.HTTPAddr
		}
		if controllerCfg.Token != "" {
			cfg.token = controllerCfg.Token
		}
		if controllerCfg.TUI.GraphMode != "" {
			cfg.graphMode = controllerCfg.TUI.GraphMode
		}
		if controllerCfg.TUI.RefreshInterval != "" {
			d, err := time.ParseDuration(controllerCfg.TUI.RefreshInterval)
			if err != nil {
				return cliConfig{}, fmt.Errorf("invalid [tui].refresh_interval: %w", err)
			}
			if d <= 0 {
				return cliConfig{}, fmt.Errorf("invalid [tui].refresh_interval: must be > 0")
			}
			cfg.refreshInterval = d
		}
	}
	if apiFlag != "" {
		cfg.api = apiFlag
	}
	if tokenFlag != "" {
		cfg.token = tokenFlag
	}
	if graphModeFlag != "" {
		cfg.graphMode = graphModeFlag
	}
	if strings.TrimSpace(cfg.token) == "" {
		return cliConfig{}, fmt.Errorf("controller token is required")
	}
	return cfg, nil
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  pakelossctl [--config configs/pakeloss-controller.toml] [--api 127.0.0.1:8080] [--token TOKEN] agents")
	fmt.Println("  pakelossctl [--config configs/pakeloss-controller.toml] [--api 127.0.0.1:8080] [--token TOKEN] flows")
	fmt.Println("  pakelossctl [--config configs/pakeloss-controller.toml] [--api 127.0.0.1:8080] [--token TOKEN] [--graph-mode unicode] tui")
}
