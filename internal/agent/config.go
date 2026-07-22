package agent

import (
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"

	"pakeloss/internal/model"
)

type agentConfigFile struct {
	Agent      agentIdentityConfig   `toml:"agent"`
	Controller agentControllerConfig `toml:"controller"`
	UDP        agentUDPConfig        `toml:"udp"`
}

type agentIdentityConfig struct {
	ID string `toml:"id"`
}

type agentControllerConfig struct {
	Addr  string `toml:"addr"`
	VRF   string `toml:"vrf"`
	Token string `toml:"token"`
}

type agentUDPConfig struct {
	ListenAddr             string `toml:"listen_addr"`
	AdvertiseAddr          string `toml:"advertise_addr"`
	ListenVRF              string `toml:"listen_vrf"`
	OnControllerDisconnect string `toml:"on_controller_disconnect"`
}

func LoadConfig(path string) (model.AgentConfig, error) {
	var raw agentConfigFile
	b, err := os.ReadFile(path)
	if err != nil {
		return model.AgentConfig{}, err
	}
	md, err := toml.Decode(string(b), &raw)
	if err != nil {
		return model.AgentConfig{}, err
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		parts := make([]string, 0, len(undecoded))
		for _, key := range undecoded {
			parts = append(parts, key.String())
		}
		return model.AgentConfig{}, fmt.Errorf("unsupported agent config keys: %s", strings.Join(parts, ", "))
	}
	cfg := model.AgentConfig{
		AgentID:                raw.Agent.ID,
		ControllerAddr:         raw.Controller.Addr,
		ControllerVRF:          raw.Controller.VRF,
		Token:                  raw.Controller.Token,
		ListenAddr:             raw.UDP.ListenAddr,
		AdvertiseAddr:          raw.UDP.AdvertiseAddr,
		ListenVRF:              raw.UDP.ListenVRF,
		OnControllerDisconnect: raw.UDP.OnControllerDisconnect,
	}
	FinalizeConfig(&cfg)
	return cfg, nil
}

func FinalizeConfig(cfg *model.AgentConfig) {
	if cfg.OnControllerDisconnect == "" {
		cfg.OnControllerDisconnect = "continue"
	}
}
