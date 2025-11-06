// config/config.go
package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	Storage struct {
		Type     string `json:"type"`     // "memory" 或 "disk"
		DataPath string `json:"dataPath"` // 磁盘存储的数据路径
	} `json:"storage"`
	Server struct {
		Port string `json:"port"`
	} `json:"server"`
}

func LoadConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
