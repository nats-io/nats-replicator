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

package conf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	"github.com/nats-io/nats-replicator/server/logging"
)

const (
	// NATSToNATS specifies a connector from NATS to NATS
	NATSToNATS = "NATSToNATS"
	// NATSToStan specifies a connector from NATS to NATS streaming
	NATSToStan = "NATSToStan"
	// StanToNATS specifies a connector from NATS streaming to NATS
	StanToNATS = "StanToNATS"
	// StanToStan specifies a connector from NATS streaming to NATS Streaming
	StanToStan = "StanToStan"
)

// NATSReplicatorConfig is the root structure for a bridge configuration file.
// NATS and STAN connections are specified in a map, where the key is a name used by
// the connector to reference a connection.
type NATSReplicatorConfig struct {
	ReconnectInterval int // milliseconds

	Logging    logging.Config
	NATS       []NATSConfig
	STAN       []NATSStreamingConfig
	Monitoring HTTPConfig
	Connect    []ConnectorConfig
}

// TLSConf holds the configuration for a TLS connection/server
type TLSConf struct {
	Key  string
	Cert string
	Root string
}

// MakeTLSConfig creates a tls.Config from a TLSConf, setting up the key pairs and certs
func (tlsConf *TLSConf) MakeTLSConfig() (*tls.Config, error) {
	if tlsConf.Cert == "" || tlsConf.Key == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(tlsConf.Cert, tlsConf.Key)
	if err != nil {
		return nil, fmt.Errorf("error loading X509 certificate/key pair: %v", err)
	}

	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate: %v", err)
	}

	config := tls.Config{
		Certificates:             []tls.Certificate{cert},
		MinVersion:               tls.VersionTLS12,
		ClientAuth:               tls.NoClientCert,
		PreferServerCipherSuites: true,
	}

	if tlsConf.Root != "" {
		// Load CA cert
		caCert, err := ioutil.ReadFile(tlsConf.Root)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.RootCAs = caCertPool
	}

	return &config, nil
}

// HTTPConfig is used to specify the host/port/tls for an HTTP server
type HTTPConfig struct {
	HTTPHost  string
	HTTPPort  int
	HTTPSPort int
	TLS       TLSConf

	ReadTimeout  int //milliseconds
	WriteTimeout int //milliseconds
}

// NATSConfig configuration for a NATS connection
type NATSConfig struct {
	Name    string
	Servers []string

	ConnectTimeout int //milliseconds
	ReconnectWait  int //milliseconds
	MaxReconnects  int

	TLS             TLSConf
	UserCredentials string
}

// NATSStreamingConfig configuration for a STAN connection
type NATSStreamingConfig struct {
	Name      string
	ClusterID string
	ClientID  string

	PubAckWait         int //milliseconds
	DiscoverPrefix     string
	MaxPubAcksInflight int
	ConnectWait        int // milliseconds

	NATSConnection string //name of the nats connection for this streaming connection
}

// DefaultConfig generates a default configuration with
// logging set to colors, time, debug and trace
func DefaultConfig() NATSReplicatorConfig {
	return NATSReplicatorConfig{
		ReconnectInterval: 5000,
		Logging: logging.Config{
			Colors: true,
			Time:   true,
			Debug:  false,
			Trace:  false,
		},
		Monitoring: HTTPConfig{
			ReadTimeout:  5000,
			WriteTimeout: 5000,
		},
	}
}

// ConnectorConfig configuration for a single connection (of any type)
// Properties are available for any type, but only the ones necessary for the
// connector type are used
type ConnectorConfig struct {
	ID   string // user specified id for a connector, will be defaulted if none is provided
	Type string // Can be any of the type constants (NATSToStan, ...)

	IncomingConnection string // Name of the incoming connection (of either type), can be the same as outgoingConnection
	OutgoingConnection string // Name of the outgoing connection (of either type), can be the same as incomingConnection

	IncomingChannel         string // Used for stan connections
	IncomingDurableName     string // Optional, used for stan connections
	IncomingStartAtSequence int64  // Start position for stan connection, -1 means StartWithLastReceived, 0 means DeliverAllAvailable (default)
	IncomingStartAtTime     int64  // Start time, as Unix, time takes precedence over sequence

	IncomingSubject   string // Used for nats connections
	IncomingQueueName string // Optional, used for nats connections

	OutgoingChannel string // Used for stan connections
	OutgoingSubject string // Used for nats connections
}
