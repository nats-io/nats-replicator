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
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
)

func TestSimpleSendOnNATSReceiveOnNATS(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "NATSToNATS",
			IncomingSubject:    incoming,
			OutgoingSubject:    outgoing,
			IncomingConnection: "nats",
			OutgoingConnection: "nats",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	tbs.Bridge.checkConnections()

	done := make(chan string)
	sub, err := tbs.NC.Subscribe(outgoing, func(msg *nats.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.NC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesIn)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesOut)
	require.Equal(t, int64(1), connStats.Connects)
	require.Equal(t, int64(0), connStats.Disconnects)
	require.True(t, connStats.Connected)
}

func TestWildcardSendOnNATSReceiveOnNATS(t *testing.T) {
	incoming := nuid.Next() + ".*"
	outgoing := nuid.Next() + "." + nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "NATSToNATS",
			IncomingSubject:    incoming,
			OutgoingSubject:    outgoing,
			IncomingConnection: "nats",
			OutgoingConnection: "nats",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	tbs.Bridge.checkConnections()

	done := make(chan string)
	sub, err := tbs.NC.Subscribe(outgoing, func(msg *nats.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.NC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesIn)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesOut)
	require.Equal(t, int64(1), connStats.Connects)
	require.Equal(t, int64(0), connStats.Disconnects)
	require.True(t, connStats.Connected)
}

func TestSimpleSendOnNATSReceiveOnNATSWithQueue(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	group := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "NATSToNATS",
			IncomingSubject:    incoming,
			IncomingQueueName:  group,
			IncomingConnection: "nats",
			OutgoingSubject:    outgoing,
			OutgoingConnection: "nats",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.NC.Subscribe(outgoing, func(msg *nats.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.NC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesIn)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesOut)
	require.Equal(t, int64(1), connStats.Connects)
	require.Equal(t, int64(0), connStats.Disconnects)
	require.True(t, connStats.Connected)
}

func TestSimpleSendOnQueueReceiveOnNatsWithTLS(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "natstonats", // test with different casing
			IncomingSubject:    incoming,
			IncomingConnection: "nats",
			OutgoingConnection: "nats",
			OutgoingSubject:    outgoing,
		},
	}

	tbs, err := StartTLSTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.NC.Subscribe(outgoing, func(msg *nats.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.NC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)
}
