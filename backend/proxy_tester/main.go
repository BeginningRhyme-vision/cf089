package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/unbound-future-admin/backend/pkg/proxypool"
	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	Worker struct {
		ProxyList string `yaml:"proxy_list"`
	} `yaml:"worker"`
}

func main() {
	// 1. Configure Proxies
	var proxyList []string

	// Try loading from config.yaml first
	configPaths := []string{"config.yaml", "../config.yaml", "../../config.yaml"}
	var configData []byte
	var err error
	for _, p := range configPaths {
		configData, err = os.ReadFile(p)
		if err == nil {
			log.Printf("Loading config from %s", p)
			break
		}
	}

	if configData != nil {
		var appCfg AppConfig
		if err := yaml.Unmarshal(configData, &appCfg); err == nil {
			if appCfg.Worker.ProxyList != "" {
				proxyList = strings.Split(appCfg.Worker.ProxyList, ",")
				for i := range proxyList {
					proxyList[i] = strings.TrimSpace(proxyList[i])
				}
			}
		} else {
			log.Printf("Failed to parse config: %v", err)
		}
	}

	// Check if env var provided (Override)
	if envProxies := os.Getenv("PROXY_LIST"); envProxies != "" {
		proxyList = strings.Split(envProxies, ",")
		for i := range proxyList {
			proxyList[i] = strings.TrimSpace(proxyList[i])
		}
		log.Println("Using proxy list from environment variable PROXY_LIST")
	}

	// Fallback to defaults if still empty
	if len(proxyList) == 0 {
		proxyList = []string{
			"http://127.0.0.1:9090",
			"http://127.0.0.1:9091",
		}
		log.Println("Using default dummy proxy list")
	}

	cfg := proxypool.Config{
		ProxyURLs:     proxyList,
		ProbeURL:      "https://worker.unboundfuture.ai",
		ProbeInterval: 5 * time.Second,
		Timeout:       2 * time.Second,
	}

	// 2. Initialize Manager
	manager, err := proxypool.NewManager(cfg)
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Stop()

	fmt.Println("Proxy Pool Manager Started")
	fmt.Printf("Monitoring %d proxies...\n", len(proxyList))

	// 3. Start a reporter loop
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			snaps := manager.Snapshot()
			fmt.Println("\n--- Proxy Status Report ---")
			for _, s := range snaps {
				statusStr := "ONLINE"
				if s.Status == proxypool.StatusOffline {
					statusStr = "OFFLINE"
				}
				fmt.Printf("[%s] %s (Fails: %d)\n", statusStr, s.URL, s.FailCount)
			}
			fmt.Println("---------------------------")
		}
	}()

	// 4. Simulate Traffic
	client := &http.Client{
		Transport: manager, // Manager implements RoundTripper
		Timeout:   5 * time.Second,
	}

	for i := 0; ; i++ {
		time.Sleep(1 * time.Second)
		start := time.Now()
		resp, err := client.Get("http://httpbin.org/ip") // Returns origin IP
		duration := time.Since(start)

		if err != nil {
			log.Printf("Request #%d FAILED in %v: %v", i+1, duration, err)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		
		fmt.Printf("Request #%d SUCCESS in %v. IP info: %s\n", i+1, duration, string(body))
	}
}
