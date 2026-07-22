package tui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"pakeloss/internal/model"
)

type Client struct {
	base  string
	token string
	http  *http.Client
}

func NewClient(addr, token string) *Client {
	addr = strings.TrimPrefix(addr, "http://")
	return &Client{base: "http://" + addr, token: token, http: &http.Client{Timeout: 2 * time.Second}}
}

func (c *Client) Agents() ([]model.AgentSnapshot, error) {
	var out []model.AgentSnapshot
	return out, c.get("/api/v1/agents", &out)
}

func (c *Client) Status() (model.StatusSnapshot, error) {
	var out model.StatusSnapshot
	return out, c.get("/api/v1/status", &out)
}

func (c *Client) Flows() ([]model.FlowSnapshot, error) {
	var out []model.FlowSnapshot
	return out, c.get("/api/v1/flows", &out)
}

func (c *Client) PauseFlow(id string) error  { return c.post("/api/v1/flows/" + id + "/pause") }
func (c *Client) ResumeFlow(id string) error { return c.post("/api/v1/flows/" + id + "/resume") }
func (c *Client) EnableAgent(id string) error {
	return c.post("/api/v1/agents/" + id + "/enable")
}
func (c *Client) DisableAgent(id string) error {
	return c.post("/api/v1/agents/" + id + "/disable")
}
func (c *Client) StartAll() error   { return c.post("/api/v1/flows/start") }
func (c *Client) StopAll() error    { return c.post("/api/v1/flows/stop") }
func (c *Client) RestartAll() error { return c.post("/api/v1/flows/restart") }

func (c *Client) get(path string, v any) error {
	req, err := c.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return decodeHTTPError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func (c *Client) post(path string) error {
	req, err := c.newRequest(http.MethodPost, path, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return decodeHTTPError(resp)
	}
	return nil
}

func (c *Client) newRequest(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

func decodeHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		return fmt.Errorf("http status %s", resp.Status)
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Message != "" {
		return fmt.Errorf("%s: %s", resp.Status, payload.Message)
	}
	return fmt.Errorf("http status %s", resp.Status)
}
