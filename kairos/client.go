package kairos

import (
	"encoding/json"
	"fmt"
	"time"

	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
)

const addPointsPath = "/api/v1/datapoints"

type Point struct {
	Name      string                 `json:"name"`
	Timestamp int64                  `json:"timestamp"`
	Value     int64                  `json:"value"`
	Tags      map[string]interface{} `json:"tags"`
	TTL       int                    `json:"ttl"`
}

type HTTPConfig struct {
	Addr       string `mapstructure:"addr"`
	User       string `masptructure:"user"`
	Pass       string `mapstructure:"pass"`
	UserAgent  string `mapstructure:"user_agent"`
	TimeoutSec int    `mapstructure:"timeout"`
}

type HTTPClient struct {
	config *HTTPConfig
	client *http.Client
}

func NewHTTPClient(config *HTTPConfig) *HTTPClient {
	if config.UserAgent == "" {
		config.UserAgent = "golang-client"
	}

	client := new(http.Client)
	if config.TimeoutSec > 0 {
		client.Timeout = time.Duration(config.TimeoutSec) * time.Second
	}

	if strings.HasSuffix(config.Addr, "/") {
		config.Addr = config.Addr[:len(config.Addr)-1]
	}

	return &HTTPClient{
		config: config,
		client: client,
	}
}

func (client HTTPClient) AddPoints(points []Point) error {
	body, err := json.Marshal(&points)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, client.config.Addr+addPointsPath, bytes.NewReader(body))
	if err != nil {
		return err
	}

	if client.config.User != "" && client.config.Pass != "" {
		req.SetBasicAuth(client.config.User, client.config.Pass)
	}

	req.Header.Set("User-Agent", client.config.UserAgent)
	req.Header.Set("Content-Type", "application/json")

	rsp, err := client.client.Do(req)
	if err != nil {
		return nil
	}

	if rsp.StatusCode >= 300 {
		defer rsp.Body.Close()

		msg, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("Expected 200 or 204, not %d. Original error: %s", rsp.StatusCode, string(msg))
	}

	return nil
}
