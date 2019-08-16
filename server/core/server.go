/*
 * Copyright 2019 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package core

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats-replicator/server/conf"
	"github.com/nats-io/nats-replicator/server/logging"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

var version = "0.0-dev"

// NATSReplicator is the core structure for the server.
type NATSReplicator struct {
	sync.Mutex
	running bool

	startTime time.Time

	logger logging.Logger
	config conf.NATSReplicatorConfig

	natsLock sync.RWMutex
	nats     map[string]*nats.Conn
	stan     map[string]stan.Conn

	connectors []Connector

	reconnectLock  sync.Mutex
	reconnect      map[string]Connector
	reconnectTimer *reconnectTimer

	statsLock     sync.Mutex
	httpReqStats  map[string]int64
	listener      net.Listener
	http          *http.Server
	httpHandler   *http.ServeMux
	monitoringURL string
}

// NewNATSReplicator creates a new account server with a default logger
func NewNATSReplicator() *NATSReplicator {
	return &NATSReplicator{
		logger: logging.NewNATSLogger(logging.Config{
			Colors: true,
			Time:   true,
			Debug:  true,
			Trace:  true,
		}),
		nats: map[string]*nats.Conn{},
		stan: map[string]stan.Conn{},
	}
}

// Logger hosts a shared logger
func (server *NATSReplicator) Logger() logging.Logger {
	return server.logger
}

func (server *NATSReplicator) checkRunning() bool {
	server.Lock()
	defer server.Unlock()
	return server.running
}

// InitializeFromFlags is called from main to configure the server, the server
// will decide what needs to happen based on the flags. On reload the same flags are
// passed
func (server *NATSReplicator) InitializeFromFlags(flags Flags) error {
	server.config = conf.DefaultConfig()

	// Always try to apply a config file, we can't run without one
	err := server.ApplyConfigFile(flags.ConfigFile)

	if err != nil {
		return err
	}

	if flags.Debug || flags.DebugAndVerbose {
		server.config.Logging.Debug = true
	}

	if flags.Verbose || flags.DebugAndVerbose {
		server.config.Logging.Trace = true
	}

	return nil
}

// ApplyConfigFile applies the config file to the server's config
func (server *NATSReplicator) ApplyConfigFile(configFile string) error {
	if configFile == "" {
		configFile = os.Getenv("NATS_REPLICATOR_CONFIG")
		if configFile != "" {
			server.logger.Noticef("using config specified in $NATS_REPLICATOR_CONFIG %q", configFile)
		}
	} else {
		server.logger.Noticef("loading configuration from %q", configFile)
	}

	if configFile == "" {
		return fmt.Errorf("no config file specified")
	}

	if err := conf.LoadConfigFromFile(configFile, &server.config, false); err != nil {
		return err
	}

	return nil
}

// InitializeFromConfig initialize the server's configuration to an existing config object, useful for tests
// Does not change the config at all, use DefaultServerConfig() to create a default config
func (server *NATSReplicator) InitializeFromConfig(config conf.NATSReplicatorConfig) error {
	server.config = config
	return nil
}

// Start the server, will lock the server, assumes the config is loaded
func (server *NATSReplicator) Start() error {
	server.Lock()
	defer server.Unlock()

	if server.logger != nil {
		server.logger.Close()
	}

	server.running = true
	server.startTime = time.Now()
	server.logger = logging.NewNATSLogger(server.config.Logging)
	server.connectors = []Connector{}
	server.reconnect = map[string]Connector{}

	server.logger.Noticef("starting NATS-Replicator, version %s", version)
	server.logger.Noticef("server time is %s", server.startTime.Format(time.UnixDate))

	if err := server.connectToNATS(); err != nil {
		return err
	}

	if err := server.connectToSTAN(); err != nil {
		return err
	}

	if err := server.initializeConnectors(); err != nil {
		return err
	}

	if err := server.startConnectors(); err != nil {
		return err
	}

	if err := server.startMonitoring(); err != nil {
		return err
	}

	return nil
}

// Stop the account server
func (server *NATSReplicator) Stop() {
	server.Lock()
	defer server.Unlock()

	if !server.running {
		return // already stopped
	}

	server.logger.Noticef("stopping bridge")

	server.running = false
	server.stopReconnectTimer()
	server.reconnect = map[string]Connector{} // clear the map

	for _, c := range server.connectors {
		err := c.Shutdown()

		if err != nil {
			server.logger.Noticef("error shutting down connector %s", err.Error())
		}
	}

	for name, nc := range server.nats {
		nc.Close()
		server.logger.Noticef("disconnected from NATS connection named %s", name)
	}

	for name, sc := range server.stan {
		sc.Close()
		server.logger.Noticef("disconnected from NATS streaming connection named %s", name)
	}

	err := server.StopMonitoring()
	if err != nil {
		server.logger.Noticef("error shutting down monitoring server %s", err.Error())
	}
}

// assumes the lock is held by the caller
func (server *NATSReplicator) initializeConnectors() error {
	connectorConfigs := server.config.Connect

	for _, c := range connectorConfigs {
		connector, err := CreateConnector(c, server)

		if err != nil {
			return err
		}

		server.connectors = append(server.connectors, connector)
	}
	return nil
}

// assumes the lock is held by the caller
func (server *NATSReplicator) startConnectors() error {
	for _, c := range server.connectors {
		if err := c.Start(); err != nil {
			server.logger.Noticef("error starting %s, %s", c.String(), err.Error())
			return err
		}
	}
	return nil
}

// FatalError stops the server, prints the messages and exits
func (server *NATSReplicator) FatalError(format string, args ...interface{}) {
	server.Stop()
	log.Fatalf(format, args...)
	os.Exit(-1)
}

// NATS hosts a shared nats connection for the connectors
func (server *NATSReplicator) NATS(name string) *nats.Conn {
	server.natsLock.RLock()
	nc := server.nats[name]
	server.natsLock.RUnlock()
	return nc
}

// Stan hosts a shared streaming connection for the connectors
func (server *NATSReplicator) Stan(name string) stan.Conn {
	server.natsLock.RLock()
	sc := server.stan[name]
	server.natsLock.RUnlock()
	return sc
}

// CheckNATS returns true if the bridge is connected to nats
func (server *NATSReplicator) CheckNATS(name string) bool {
	server.natsLock.Lock()
	defer server.natsLock.Unlock()

	nc, ok := server.nats[name]

	if ok && nc != nil {
		return nc.ConnectedUrl() != ""
	}

	return false
}

// CheckStan returns true if the bridge is connected to stan
func (server *NATSReplicator) CheckStan(name string) bool {
	server.natsLock.Lock()
	defer server.natsLock.Unlock()

	var stanConfig conf.NATSStreamingConfig
	ok := false

	for _, c := range server.config.STAN {
		if c.Name == name {
			stanConfig = c
			ok = true
			break
		}
	}

	if !ok {
		return false
	}

	natsName := stanConfig.NATSConnection
	nc, ok := server.nats[natsName]

	if !ok || nc == nil || nc.ConnectedUrl() == "" {
		return false
	}

	sc, ok := server.stan[name]
	return ok && sc != nil
}

// ConnectorError is called by a connector if it has a failure that requires a reconnect
func (server *NATSReplicator) ConnectorError(connector Connector, err error) {
	if !server.checkRunning() {
		return
	}

	server.reconnectLock.Lock()
	defer server.reconnectLock.Unlock()

	_, check := server.reconnect[connector.ID()]

	if check {
		return // we already have that connector, no need to stop or pring any messages
	}

	description := connector.String()
	server.logger.Errorf("a connector error has occurred, replicator will try to restart %s, %s", description, err.Error())

	err = connector.Shutdown()

	if err != nil {
		server.logger.Warnf("error shutting down connector %s, replicator will try to restart, %s", description, err.Error())
	}

	server.reconnect[connector.ID()] = connector

	server.ensureReconnectTimer()
}

// checkConnections loops over the connections and has them each check check their requirements
func (server *NATSReplicator) checkConnections() {
	server.logger.Warnf("checking connector requirements and will restart as needed.")

	if !server.checkRunning() {
		return
	}

	server.reconnectLock.Lock()
	defer server.reconnectLock.Unlock()

	for _, connector := range server.connectors {
		_, check := server.reconnect[connector.ID()]

		if check {
			continue // we already have that connector, no need to stop or pring any messages
		}

		err := connector.CheckConnections()

		if err == nil {
			continue // connector is happy
		}

		description := connector.String()
		server.logger.Errorf("a connector error has occurred, trying to restart %s, %s", description, err.Error())

		err = connector.Shutdown()

		if err != nil {
			server.logger.Warnf("error shutting down connector %s, trying to restart, %s", description, err.Error())
		}

		server.reconnect[connector.ID()] = connector
	}

	server.ensureReconnectTimer()
}

// requires the reconnect lock be held by the caller
// spawns a go routine that will acquire the lock for handling reconnect tasks
func (server *NATSReplicator) ensureReconnectTimer() {
	if server.reconnectTimer != nil {
		return
	}

	timer := newReconnectTimer()
	server.reconnectTimer = timer

	go func() {
		interval := server.config.ReconnectInterval
		doReconnect := <-timer.After(time.Duration(interval) * time.Millisecond) // This is the reconnect timer
		if !doReconnect {
			return
		}

		server.reconnectLock.Lock()
		defer server.reconnectLock.Unlock()

		server.reconnectTimer = nil

		// Wait for nats to be reconnected
		for _, c := range server.config.NATS {
			if !server.CheckNATS(c.Name) {
				server.logger.Noticef("nats connection %s is down, will try reconnecting to streaming and restarting connectors in %d milliseconds", c.Name, interval)
				server.ensureReconnectTimer()
				return
			}
		}

		// Make sure stan is up, if it should be
		server.logger.Noticef("trying to reconnect to nats streaming")
		err := server.connectToSTAN() // this may be a no-op if server.stan == nil was true but is not true once we get the lock in the connect

		if err != nil {
			server.logger.Noticef("error restarting streaming connection, will retry in %d milliseconds", interval, err.Error())
			server.ensureReconnectTimer()
			return
		}

		// Do all the reconnects
		for id, connector := range server.reconnect {
			server.logger.Noticef("trying to restart connector %s", connector.String())
			err := connector.Start()

			if err != nil {
				server.logger.Noticef("error restarting connector %s, will retry in %d milliseconds, %s", connector.String(), interval, err.Error())
				continue
			}

			delete(server.reconnect, id)
		}

		if len(server.reconnect) > 0 {
			server.ensureReconnectTimer()
		}
	}()
}

// locks the reconnect lock
func (server *NATSReplicator) stopReconnectTimer() {
	server.reconnectLock.Lock()
	defer server.reconnectLock.Unlock()

	if server.reconnectTimer != nil {
		server.reconnectTimer.Cancel()
	}

	server.reconnectTimer = nil
}
