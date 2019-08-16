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
	stan "github.com/nats-io/stan.go"
	"github.com/stretchr/testify/require"
)

func TestSimpleSendOnStanReceiveOnStan(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "StanToStan",
			IncomingChannel:    incoming,
			OutgoingChannel:    outgoing,
			IncomingConnection: "stan",
			OutgoingConnection: "stan",
		},
	}

	tbs, err := StartTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	tbs.Bridge.checkConnections()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
	require.Equal(t, int64(len([]byte(msg))), connStats.BytesIn)
	require.Equal(t, int64(1), connStats.Connects)
	require.Equal(t, int64(0), connStats.Disconnects)
	require.True(t, connStats.Connected)
}

func TestQueueStartAtPositionToStan(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"
	msg2 := "goodbye world"

	connect := []conf.ConnectorConfig{
		{
			Type:                    "StanToStan",
			IncomingChannel:         incoming,
			OutgoingChannel:         outgoing,
			IncomingConnection:      "stan",
			OutgoingConnection:      "stan",
			IncomingStartAtSequence: 2,
		},
	}

	tbs, err := StartTestEnvironmentInfrastructure(false)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Send 2 messages, should only get 2nd
	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)
	err = tbs.SC.Publish(incoming, []byte(msg2))
	require.NoError(t, err)

	err = tbs.StartReplicator(connect)
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg2, received)

	received = tbs.WaitForIt(2, done)
	require.Empty(t, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
}

func TestQueueDeliverLatestToStan(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()

	connect := []conf.ConnectorConfig{
		{
			Type:                    "StanToStan",
			IncomingChannel:         incoming,
			OutgoingChannel:         outgoing,
			IncomingConnection:      "stan",
			OutgoingConnection:      "stan",
			IncomingStartAtSequence: -1,
		},
	}

	tbs, err := StartTestEnvironmentInfrastructure(false)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Send 2 messages, should only get 2nd
	err = tbs.SC.Publish(incoming, []byte("one"))
	require.NoError(t, err)
	err = tbs.SC.Publish(incoming, []byte("two"))
	require.NoError(t, err)

	err = tbs.StartReplicator(connect)
	require.NoError(t, err)

	err = tbs.SC.Publish(incoming, []byte("three"))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, "two", received)

	received = tbs.WaitForIt(2, done)
	require.Equal(t, "three", received)

	received = tbs.WaitForIt(3, done)
	require.Empty(t, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(2), connStats.MessagesIn)
	require.Equal(t, int64(2), connStats.MessagesOut)
}

func TestQueueStartAtTimeToStan(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"

	tbs, err := StartTestEnvironmentInfrastructure(false)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	// Send 2 messages, should only get 2nd
	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)
	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	time.Sleep(2 * time.Second) // move the time along

	connect := []conf.ConnectorConfig{
		{
			Type:                "StanToStan",
			IncomingChannel:     incoming,
			IncomingConnection:  "stan",
			IncomingStartAtTime: time.Now().Unix(),
			OutgoingChannel:     outgoing,
			OutgoingConnection:  "stan",
		},
	}

	err = tbs.StartReplicator(connect)
	require.NoError(t, err)

	time.Sleep(1 * time.Second) // move the time along

	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)

	received = tbs.WaitForIt(2, done)
	require.Empty(t, received)

	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(1), connStats.MessagesIn)
	require.Equal(t, int64(1), connStats.MessagesOut)
}

func TestDurableSubscriberToStan(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()

	tbs, err := StartTestEnvironmentInfrastructure(false)
	require.NoError(t, err)
	defer tbs.Close()

	connect := []conf.ConnectorConfig{
		{
			Type:                "StanToStan",
			IncomingChannel:     incoming,
			IncomingConnection:  "stan",
			IncomingDurableName: nuid.Next(),

			OutgoingChannel:    outgoing,
			OutgoingConnection: "stan",
		},
	}

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	err = tbs.StartReplicator(connect)
	require.NoError(t, err)

	err = tbs.SC.Publish(incoming, []byte("one"))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, "one", received)

	tbs.StopReplicator()

	err = tbs.SC.Publish(incoming, []byte("two"))
	require.NoError(t, err)

	err = tbs.SC.Publish(incoming, []byte("three"))
	require.NoError(t, err)

	err = tbs.StartReplicator(connect)
	require.NoError(t, err)

	received = tbs.WaitForIt(1, done) // reset counter, we restarted replicator
	require.Equal(t, "two", received)

	received = tbs.WaitForIt(2, done)
	require.Equal(t, "three", received)

	received = tbs.WaitForIt(4, done)
	require.Empty(t, received)

	// Should have 2 messages since the relaunch
	stats := tbs.Bridge.SafeStats()
	connStats := stats.Connections[0]
	require.Equal(t, int64(2), connStats.MessagesIn)
	require.Equal(t, int64(2), connStats.MessagesOut)
}

func TestSimpleSendOnStanReceiveOnStanWithTLS(t *testing.T) {
	incoming := nuid.Next()
	outgoing := nuid.Next()
	msg := "hello world"

	connect := []conf.ConnectorConfig{
		{
			Type:               "stantostan", // test with different casing
			IncomingChannel:    incoming,
			IncomingConnection: "stan",
			OutgoingChannel:    outgoing,
			OutgoingConnection: "stan",
		},
	}

	tbs, err := StartTLSTestEnvironment(connect)
	require.NoError(t, err)
	defer tbs.Close()

	done := make(chan string)
	sub, err := tbs.SC.Subscribe(outgoing, func(msg *stan.Msg) {
		done <- string(msg.Data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, tbs.NC.FlushTimeout(time.Second*5))

	err = tbs.SC.Publish(incoming, []byte(msg))
	require.NoError(t, err)

	received := tbs.WaitForIt(1, done)
	require.Equal(t, msg, received)
}
