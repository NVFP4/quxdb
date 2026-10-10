package server

import (
	"fmt"
	"net"
)

const (
	DefaultHost = "0.0.0.0"
	DefaultPort = 7129
)

// Config holds the http listener address.
type Config struct {
	Host string `yaml:"host"`
	Port uint16 `yaml:"port"`
}

// DefaultConfig returns the default listener address.
func DefaultConfig() Config {
	return Config{
		Host: DefaultHost,
		Port: DefaultPort,
	}
}

func (config Config) Addr() string {
	if config.Host == "" {
		config.Host = DefaultHost
	}
	if config.Port == 0 {
		config.Port = DefaultPort
	}
	return net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port))
}
