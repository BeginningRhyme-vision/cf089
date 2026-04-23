package main

import (
	"log"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage StorageConfig `yaml:"storage"`
}

type StorageConfig struct {
	Src                SrcConfig `yaml:"src"`
	TransferServiceURL string    `yaml:"transfer_service_url"`
}

type SrcConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type TransferMetadata struct {
	ID          uint   `json:"id"`
	Endpoint    string `json:"endpoint"`
	AK          string `json:"ak"`
	SKEncrypted string `json:"sk_encrypted"`
}

var (
	cfg        *Config
	apiBaseURL string
)

func loadConfig() {
	paths := []string{"../../config.yaml", "../config.yaml", "config.yaml"}
	var data []byte
	var err error
	for _, p := range paths {
		data, err = os.ReadFile(p)
		if err == nil {
			break
		}
	}
	if data == nil {
		log.Fatal("Could not find config.yaml")
	}

	cfg = &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
}

func getBucketFromEndpoint(endpoint string) string {
	if strings.HasPrefix(endpoint, "s3://") {
		host := strings.TrimPrefix(endpoint, "s3://")
		parts := strings.Split(host, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}

	return strings.Trim(u.Path, "/")
}
