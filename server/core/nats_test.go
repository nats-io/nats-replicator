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
 */

package core

import (
	"testing"
	"time"

	"github.com/nats-io/nats-replicator/server/conf"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
)

func TestDisconnectReconnect(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()

	connect := []conf.ConnectorConfig{
		{
			Type:               "StanToStan",
			IncomingChannel:    incoming,
			OutgoingChannel:    outgoing,
			IncomingConnection: "stan",
			OutgoingConnection: "stan",
		},
	}

	// Default pings are 2 seconds, with a max of 3 for tests
	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	tbs.Bridge.checkConnections()

	require.NotNil(t, tbs.Bridge.nats["nats"])
	require.NotNil(t, tbs.Bridge.stan["stan"])
	require.True(t, tbs.Bridge.CheckNATS("nats"))
	require.True(t, tbs.Bridge.CheckStan("stan"))

	tbs.StopNATS()

	tbs.Bridge.nats["nats"].FlushTimeout(time.Second * 5)

	now := time.Now()

	// wait 5 seconds for connection lost
	// ping interval is 1sec for tests and max pings is 3
	for time.Since(now) < time.Second*5 {
		if tbs.Bridge.Stan("stan") == nil {
			break
		}

		time.Sleep(time.Millisecond * 500)
	}

	require.NotNil(t, tbs.Bridge.nats["nats"])
	require.Nil(t, tbs.Bridge.stan["stan"])
	require.False(t, tbs.Bridge.CheckNATS("nats"))
	require.False(t, tbs.Bridge.CheckStan("stan"))

	tbs.RestartNATS()

	now = time.Now()

	// Wait for a reconnect
	for time.Since(now) < time.Second*20 && !tbs.Bridge.CheckStan("stan") {
		time.Sleep(time.Millisecond * 100)
	}

	require.NotNil(t, tbs.Bridge.nats["nats"])
	require.NotNil(t, tbs.Bridge.stan["stan"])
	require.True(t, tbs.Bridge.CheckNATS("nats"))
	require.True(t, tbs.Bridge.CheckStan("stan"))
}
