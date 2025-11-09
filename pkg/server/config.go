package server

import (
	"fmt"
	"net"
)

const (
	DefaultHost    = "0.0.0.0"
	DefaultPort    = 7129
	DefaultDataDir = "quxdata"
)

type QuxServerConfig struct {
	Host    string
	Port    uint16
	DataDir string
}

func NewConfig() QuxServerConfig {
	return QuxServerConfig{
		Host:    DefaultHost,
		Port:    DefaultPort,
		DataDir: DefaultDataDir,
	}
}

func (c QuxServerConfig) WithHost(host string) QuxServerConfig {
	c.Host = host
	return c
}

func (c QuxServerConfig) WithPort(port uint16) QuxServerConfig {
	c.Port = port
	return c
}

func (c QuxServerConfig) WithDataDir(dataDir string) QuxServerConfig {
	c.DataDir = dataDir
	return c
}

func (config QuxServerConfig) Addr() string {
	if config.Host == "" {
		config.Host = DefaultHost
	}
	if config.Port == 0 {
		config.Port = DefaultPort
	}
	return net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port))
}
