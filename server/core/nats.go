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
	"strings"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

func (server *NATSReplicator) natsError(nc *nats.Conn, sub *nats.Subscription, err error) {
	server.logger.Warnf("nats error %s", err.Error())
}

func (server *NATSReplicator) stanConnectionLost(sc stan.Conn, err error) {
	if !server.checkRunning() {
		return
	}
	server.logger.Warnf("nats streaming disconnected")

	server.natsLock.Lock()
	server.stan = nil // we lost stan
	server.natsLock.Unlock()

	server.checkConnections()
}

func (server *NATSReplicator) natsDisconnected(nc *nats.Conn) {
	if !server.checkRunning() {
		return
	}
	server.logger.Warnf("nats disconnected")
	server.checkConnections()
}

func (server *NATSReplicator) natsReconnected(nc *nats.Conn) {
	server.logger.Warnf("nats reconnected")
}

func (server *NATSReplicator) natsClosed(nc *nats.Conn) {
	if server.checkRunning() {
		server.logger.Errorf("nats connection closed, shutting down bridge")
		go server.Stop()
	}
}

func (server *NATSReplicator) natsDiscoveredServers(nc *nats.Conn) {
	server.logger.Debugf("discovered servers: %v\n", nc.DiscoveredServers())
	server.logger.Debugf("known servers: %v\n", nc.Servers())
}

// assumes the lock is held by the caller
func (server *NATSReplicator) connectToNATS() error {
	server.natsLock.Lock()
	defer server.natsLock.Unlock()

	if !server.running {
		return nil // already stopped
	}

	for _, config := range server.config.NATS {
		name := config.Name
		server.logger.Noticef("connecting to NATS with configuration %s", name)

		maxReconnects := nats.DefaultMaxReconnect
		reconnectWait := nats.DefaultReconnectWait
		connectTimeout := nats.DefaultTimeout

		if config.MaxReconnects > 0 {
			maxReconnects = config.MaxReconnects
		}

		if config.ReconnectWait > 0 {
			reconnectWait = time.Duration(config.ReconnectWait) * time.Millisecond
		}

		if config.ConnectTimeout > 0 {
			connectTimeout = time.Duration(config.ConnectTimeout) * time.Millisecond
		}

		options := []nats.Option{nats.MaxReconnects(maxReconnects),
			nats.ReconnectWait(reconnectWait),
			nats.Timeout(connectTimeout),
			nats.ErrorHandler(server.natsError),
			nats.DiscoveredServersHandler(server.natsDiscoveredServers),
			nats.DisconnectHandler(server.natsDisconnected),
			nats.ReconnectHandler(server.natsReconnected),
			nats.ClosedHandler(server.natsClosed),
		}

		if config.TLS.Root != "" {
			options = append(options, nats.RootCAs(config.TLS.Root))
		}

		if config.TLS.Cert != "" {
			options = append(options, nats.ClientCert(config.TLS.Cert, config.TLS.Key))
		}

		if config.UserCredentials != "" {
			options = append(options, nats.UserCredentials(config.UserCredentials))
		}

		nc, err := nats.Connect(strings.Join(config.Servers, ","),
			options...,
		)

		if err != nil {
			return err
		}

		server.nats[name] = nc
	}
	return nil
}

// assumes the lock is held by the caller
func (server *NATSReplicator) connectToSTAN() error {
	server.natsLock.Lock()
	defer server.natsLock.Unlock()

	for _, config := range server.config.STAN {
		name := config.Name
		sc, ok := server.stan[name]

		if ok && sc != nil {
			continue // that one is already connected
		}

		if config.ClusterID == "" {
			server.logger.Noticef("skipping NATS streaming connection %s, not configured", name)
			continue
		}

		server.logger.Noticef("connecting to NATS streaming with configuration %s, cluster id is %s", name, config.ClusterID)

		nc, ok := server.nats[config.NATSConnection]

		if !ok || nc == nil {
			return fmt.Errorf("stan connection %s requires NATS connection %s", name, config.NATSConnection)
		}

		pubAckWait := stan.DefaultAckWait

		if config.PubAckWait != 0 {
			pubAckWait = time.Duration(config.PubAckWait) * time.Millisecond
		}

		maxPubInFlight := stan.DefaultMaxPubAcksInflight

		if config.MaxPubAcksInflight > 0 {
			maxPubInFlight = config.MaxPubAcksInflight
		}

		connectWait := stan.DefaultConnectWait

		if config.ConnectWait > 0 {
			pubAckWait = time.Duration(config.ConnectWait) * time.Millisecond
		}

		sc, err := stan.Connect(config.ClusterID, config.ClientID,
			stan.NatsConn(nc),
			stan.PubAckWait(pubAckWait),
			stan.MaxPubAcksInflight(maxPubInFlight),
			stan.ConnectWait(connectWait),
			stan.SetConnectionLostHandler(server.stanConnectionLost),
			func(o *stan.Options) error {
				if config.DiscoverPrefix != "" {
					o.DiscoverPrefix = config.DiscoverPrefix
				} else {
					o.DiscoverPrefix = "_STAN.discover"
				}
				return nil
			})

		if err != nil {
			return err
		}

		server.stan[name] = sc
	}

	return nil
}
