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

package main

import (
	"encoding/json"
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats-replicator/server/conf"
	"github.com/nats-io/nats-replicator/server/core"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	stan "github.com/nats-io/stan.go"
)

var iterations int
var natsURL string
var stanClusterID string
var maxPubAcks int

func startReplicator(connections []conf.ConnectorConfig) (*core.NATSReplicator, error) {
	config := conf.DefaultConfig()
	config.Logging.Debug = false
	config.Logging.Trace = false
	config.Logging.Colors = false
	config.Monitoring = conf.HTTPConfig{
		HTTPPort: -1,
	}

	config.NATS = []conf.NATSConfig{}
	config.NATS = append(config.NATS, conf.NATSConfig{
		Servers:        []string{natsURL},
		ConnectTimeout: 2000,
		ReconnectWait:  2000,
		MaxReconnects:  5,
		Name:           "nats",
	})
	config.NATS = append(config.NATS, conf.NATSConfig{
		Servers:        []string{natsURL},
		ConnectTimeout: 2000,
		ReconnectWait:  2000,
		MaxReconnects:  5,
		Name:           "nats2",
	})

	config.STAN = []conf.NATSStreamingConfig{}
	config.STAN = append(config.STAN, conf.NATSStreamingConfig{
		ClusterID:          stanClusterID,
		ClientID:           "perf_test_" + nuid.Next(),
		Name:               "stan",
		NATSConnection:     "nats",
		MaxPubAcksInflight: maxPubAcks,
	})
	config.STAN = append(config.STAN, conf.NATSStreamingConfig{
		ClusterID:          stanClusterID,
		ClientID:           "perf_test_" + nuid.Next(),
		Name:               "stan2",
		NATSConnection:     "nats2",
		MaxPubAcksInflight: maxPubAcks,
	})

	config.Connect = connections

	replicator := core.NewNATSReplicator()
	err := replicator.InitializeFromConfig(config)
	if err != nil {
		return nil, err
	}
	err = replicator.Start()
	if err != nil {
		replicator.Stop()
		return nil, err
	}

	return replicator, nil
}

func main() {
	flag.IntVar(&iterations, "i", 1000, "iterations, defaults to 1000")
	flag.IntVar(&maxPubAcks, "mpa", 1, "maximum pub acks, defaults to 1")
	flag.StringVar(&natsURL, "nats", "nats://localhost:4222", "nats url, defaults to nats://localhost:4222")
	flag.StringVar(&stanClusterID, "stan", "test-cluster", "stan cluster id")
	flag.Parse()

	incoming := nuid.Next()
	outgoing := nuid.Next()
	msgString := strings.Repeat("stannats", 128) // 1024 bytes
	msg := []byte(msgString)
	msgLen := len(msg)
	wg := sync.WaitGroup{}

	connect := []conf.ConnectorConfig{
		{
			Type:               "StanToStan",
			IncomingConnection: "stan",
			OutgoingConnection: "stan2",
			IncomingChannel:    incoming,
			OutgoingChannel:    outgoing,
		},
	}

	replicator, err := startReplicator(connect)
	if err != nil {
		log.Fatalf("error starting replicator, %s", err.Error())
	}

	done := make(chan bool)
	count := 0
	interval := int(iterations / 10)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("error connecting to nats, %s", err.Error())
	}
	defer nc.Close()

	sc, err := stan.Connect(stanClusterID, nuid.Next(), stan.NatsConn(nc), stan.MaxPubAcksInflight(maxPubAcks))
	if err != nil {
		log.Fatalf("error connecting to stan, %s", err.Error())
	}

	sc.Subscribe(outgoing, func(msg *stan.Msg) {
		count++
		if count%interval == 0 {
			log.Printf("received count = %d", count)
		}

		if len(msg.Data) != msgLen {
			log.Fatalf("received message that is the wrong size %d != %d", len(msg.Data), msgLen)
		}

		if count == iterations {
			done <- true
		}
	})

	log.Printf("sending %d messages through stan to stan...", iterations)

	wg.Add(iterations)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, err := sc.PublishAsync(incoming, msg, func(aguid string, err error) {
			if err != nil {
				log.Fatalf("error in ack handler, %s", err.Error())
			}
			wg.Done()
		})

		if err != nil {
			log.Fatalf("error publishing message, %s", err.Error())
		}

		if i%interval == 0 && i != 0 {
			log.Printf("async send count = %d", i)
		}
	}
	wg.Wait()
	<-done
	end := time.Now()

	log.Printf("trying to wait for acks to return to replicator before we shut it down")
	timeout := time.Duration(5000) * time.Millisecond // 5 second timeout
	stop := time.Now().Add(timeout)
	requestsOk := make(chan bool)

	ticker := time.NewTicker(50 * time.Millisecond)
	go func() {
		for t := range ticker.C {
			if t.After(stop) {
				requestsOk <- false
				break
			}

			if replicator.SafeStats().RequestCount >= int64(iterations) {
				requestsOk <- true
				break
			}
		}
		ticker.Stop()
	}()

	<-requestsOk
	stats := replicator.SafeStats()
	statsJSON, _ := json.MarshalIndent(stats, "", "    ")

	replicator.Stop()

	diff := end.Sub(start)
	rate := float64(iterations) / float64(diff.Seconds())
	log.Printf("Replicator Stats:\n\n%s\n", statsJSON)
	log.Printf("Sent %d messages through a streaming channel to a streaming subscriber in %s, or %.2f msgs/sec", iterations, diff, rate)
}
