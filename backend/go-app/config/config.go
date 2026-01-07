package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	Database DatabaseConfig `yaml:"database"`
	Redis    RedisConfig    `yaml:"redis"`
	Feishu   FeishuConfig   `yaml:"feishu"`
	Security SecurityConfig `yaml:"security"`
	Storage  StorageConfig  `yaml:"storage"`
	Worker   WorkerConfig   `yaml:"worker"`
}

type AppConfig struct {
	Name string `yaml:"name"`
}

type DatabaseConfig struct {
	URL string `yaml:"url"`
}

type RedisConfig struct {
	URL string `yaml:"url"`
}

type FeishuConfig struct {
	AppID       string `yaml:"app_id"`
	AppSecret   string `yaml:"app_secret"`
	RedirectURI string `yaml:"redirect_uri"`
}

type SecurityConfig struct {
	JWTSecret              string `yaml:"jwt_secret"`
	JWTAlgorithm           string `yaml:"jwt_algorithm"`
	AccessTokenExpireMins  int    `yaml:"access_token_expire_minutes"`
}

type StorageConfig struct {
	TransferServiceURL string    `yaml:"transfer_service_url"`
	Src                SrcConfig `yaml:"src"`
}

type SrcConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Region    string `yaml:"region"`
}

type WorkerConfig struct {
	MaxThreads int `yaml:"max_threads"`
}

func LoadConfig() (*Config, error) {
	// Assuming running from backend/go-app or similar depth
	// Try to find config.yaml in logical places
    // We are at backend/go-app, config is at root
    
    // Check current dir, up one, up two
    paths := []string{
        "config.yaml",
        "../config.yaml",
        "../../config.yaml",
        "../../../config.yaml",
    }

    var configPath string
    for _, p := range paths {
        if _, err := os.Stat(p); err == nil {
            configPath = p
            break
        }
    }

    if configPath == "" {
        return nil, fmt.Errorf("config.yaml not found")
    }

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
