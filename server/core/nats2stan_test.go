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

	"github.com/nats-io/nats-replicator/server/conf"
	"github.com/nats-io/nuid"
	stan "github.com/nats-io/stan.go"
	"github.com/stretchr/testify/require"
)

func TestSimpleSendOnNATSReceiveOnStan(t *testing.T) {
	channel := nuid.Next()
	subject := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "NATSToStan",
			OutgoingChannel:    channel,
			IncomingSubject:    subject,
			IncomingConnection: "nats",
			OutgoingConnection: "stan",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	tbs.Bridge.checkConnections()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(channel, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = tbs.NC.Publish(subject, []byte(msg))
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

func TestSimpleSendOnNATSReceiveOnStanWithGroup(t *testing.T) {
	channel := nuid.Next()
	subject := nuid.Next()
	group := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "NATSToStan",
			IncomingSubject:    subject,
			IncomingQueueName:  group,
			IncomingConnection: "nats",
			OutgoingChannel:    channel,
			OutgoingConnection: "stan",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(channel, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = tbs.NC.Publish(subject, []byte(msg))
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

func TestSimpleSendOnQueueReceiveOnStanWithTLS(t *testing.T) {
	channel := nuid.Next()
	subject := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "natstostan", // test with different casing
			OutgoingChannel:    channel,
			IncomingSubject:    subject,
			IncomingConnection: "nats",
			OutgoingConnection: "stan",
		},
	}

	tbs, err := StartTLSTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(channel, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = tbs.NC.Publish(subject, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)
}
