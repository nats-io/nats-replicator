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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	require.Equal(t, false, config.Logging.Trace)
	require.Equal(t, 5000, config.Monitoring.ReadTimeout)
}

func TestMakeTLSConfig(t *testing.T) {
	tlsC := &TLSConf{
		Cert: "../../resources/certs/client-cert.pem",
		Key:  "../../resources/certs/client-key.pem",
		Root: "../../resources/certs/truststore.pem",
	}
	_, err := tlsC.MakeTLSConfig()
	require.NoError(t, err)
}

func TestNATSConfig(t *testing.T) {
	config := DefaultConfig()
	configString := `
	{
		connectors: [],
		nats: [
			{
				name: "one"
				servers: ["nats://localhost:4222"]
			}
		]
		monitoring: {
			HTTPPort: -1,
			ReadTimeout: 2000,
		}
	}
	`

	err := LoadConfigFromString(configString, &config, false)
	require.NoError(t, err)

	require.Equal(t, 1, len(config.NATS))
	require.Equal(t, 1, len(config.NATS[0].Servers))
	require.Equal(t, config.NATS[0].Servers[0], "nats://localhost:4222")
	require.Equal(t, config.Monitoring.ReadTimeout, 2000)
	require.Equal(t, config.Logging.Trace, false)
	require.Equal(t, config.Logging.Debug, false)
}

func TestNATSConfigWithTags(t *testing.T) {
	config := DefaultConfig()
	configString := `
	{
		reconnect_interval: 1000
		connect: [
			{
				incoming_connection: "one"
				outgoing_connection: "one"
				incoming_subject: "test"
				outgoing_subject: "hello"
			}
		],
		nats: [
			{
				name: "one"
				servers: ["nats://localhost:4222"]
				connect_timeout: 5000
			}
		]
		monitoring: {
			http_port: -1,
			read_timeout: 2000,
		}
	}
	`

	err := LoadConfigFromString(configString, &config, false)
	require.NoError(t, err)

	require.Equal(t, config.ReconnectInterval, 1000)
	require.Equal(t, 1, len(config.NATS))
	require.Equal(t, 1, len(config.NATS[0].Servers))
	require.Equal(t, config.NATS[0].Servers[0], "nats://localhost:4222")
	require.Equal(t, config.NATS[0].ConnectTimeout, 5000)
	require.Equal(t, config.Monitoring.ReadTimeout, 2000)
	require.Equal(t, config.Logging.Trace, false)
	require.Equal(t, config.Logging.Debug, false)
	require.Len(t, config.Connect, 1)
	require.Equal(t, config.Connect[0].IncomingConnection, "one")
	require.Equal(t, config.Connect[0].OutgoingConnection, "one")
	require.Equal(t, config.Connect[0].IncomingSubject, "test")
	require.Equal(t, config.Connect[0].OutgoingSubject, "hello")
}
