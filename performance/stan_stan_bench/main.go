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
var messageSize int
var natsURL string
var stanClusterID string
var natsURL2 string
var stanClusterID2 string
var maxPubAcks int
var direct bool
var pubOnly bool
var subOnly bool
var showStats bool
var hideReplicatorLog bool
var showProgress bool

func startReplicator(connections []conf.ConnectorConfig) (*core.NATSReplicator, error) {
	config := conf.DefaultConfig()
	config.Logging.Debug = false
	config.Logging.Trace = false
	config.Logging.Colors = false
	config.Logging.Hide = hideReplicatorLog
	config.Monitoring = conf.HTTPConfig{
		HTTPPort: -1,
	}

	config.NATS = []conf.NATSConfig{}
	config.NATS = append(config.NATS, conf.NATSConfig{
		Servers:        []string{natsURL},
		ConnectTimeout: 5000,
		ReconnectWait:  5000,
		MaxReconnects:  5,
		Name:           "nats",
	})
	config.NATS = append(config.NATS, conf.NATSConfig{
		Servers:        []string{natsURL2},
		ConnectTimeout: 5000,
		ReconnectWait:  5000,
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
		ClusterID:          stanClusterID2,
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
	flag.IntVar(&iterations, "n", 1000, "messages to send, defaults to 1000")
	flag.IntVar(&messageSize, "ms", 1024, "message size, defaults to 1024")
	flag.IntVar(&maxPubAcks, "mpa", 1, "maximum pub acks, defaults to 1")
	flag.StringVar(&natsURL, "nats", "nats://localhost:4222", "nats url, defaults to nats://localhost:4222")
	flag.StringVar(&stanClusterID, "stan", "test-cluster", "stan cluster id")
	flag.StringVar(&natsURL2, "nats2", "", "nats url for the subscriber side, defaults to nats://localhost:4222")
	flag.StringVar(&stanClusterID2, "stan2", "", "stan cluster id for the subscriber side, defaults to test-cluster")
	flag.BoolVar(&direct, "direct", false, "skip the replicator and just")
	flag.BoolVar(&pubOnly, "pub", false, "only publish, don't subscribe, useful for testing send times across a long connection")
	flag.BoolVar(&subOnly, "sub", false, "only time the reads, useful for testing read times across a long connection, timer starts with first receive")
	flag.BoolVar(&showStats, "stats", false, "print replicator stats, if not direct")
	flag.BoolVar(&hideReplicatorLog, "hide", false, "hide the replicator log")
	flag.BoolVar(&showProgress, "progress", true, "show progress")
	flag.Parse()

	var replicator *core.NATSReplicator

	incoming := nuid.Next()
	outgoing := nuid.Next()
	msgString := strings.Repeat("a", messageSize)
	msg := []byte(msgString)
	msgLen := len(msg)
	wg := sync.WaitGroup{}

	if natsURL2 == "" {
		natsURL2 = natsURL
	}

	if stanClusterID2 == "" {
		stanClusterID2 = stanClusterID
	}

	if pubOnly || subOnly {
		direct = true
		log.Printf("Pub and sub only mode always run with direct mode, no replicator is used")
	}

	if !direct {
		connect := []conf.ConnectorConfig{
			{
				Type:               "StanToStan",
				IncomingConnection: "stan",
				OutgoingConnection: "stan2",
				IncomingChannel:    incoming,
				OutgoingChannel:    outgoing,
			},
		}

		var err error
		replicator, err = startReplicator(connect)
		if err != nil {
			log.Fatalf("error starting replicator, %s", err.Error())
		}
	} else {
		log.Printf("Direct mode uses the same nats url and stan cluster id for both connections")
		outgoing = incoming
		stanClusterID2 = stanClusterID
		natsURL2 = natsURL
	}

	done := make(chan bool, 1)
	count := 0
	interval := int(iterations / 10)

	nc, err := nats.Connect(natsURL, nats.Timeout(time.Second*5), nats.MaxReconnects(5), nats.ReconnectWait(time.Second*5))
	if err != nil {
		log.Fatalf("error connecting to nats, %s", err.Error())
	}
	defer nc.Close()

	sc, err := stan.Connect(stanClusterID, nuid.Next(), stan.NatsConn(nc), stan.MaxPubAcksInflight(maxPubAcks))
	if err != nil {
		log.Fatalf("error connecting to stan, %s", err.Error())
	}

	nc2, err := nats.Connect(natsURL2, nats.Timeout(time.Second*5), nats.MaxReconnects(5), nats.ReconnectWait(time.Second*5))
	if err != nil {
		log.Fatalf("error connecting to nats, %s", err.Error())
	}
	defer nc.Close()

	sc2, err := stan.Connect(stanClusterID2, nuid.Next(), stan.NatsConn(nc2), stan.MaxPubAcksInflight(maxPubAcks))
	if err != nil {
		log.Fatalf("error connecting to stan, %s", err.Error())
	}

	var start time.Time

	if !pubOnly {
		sc2.Subscribe(outgoing, func(msg *stan.Msg) {
			if subOnly && count == 0 {
				start = time.Now()
			}
			count++
			if count%interval == 0 && showProgress {
				log.Printf("received count = %d", count)
			}

			if len(msg.Data) != msgLen {
				log.Fatalf("received message that is the wrong size %d != %d", len(msg.Data), msgLen)
			}

			if count == iterations {
				done <- true
			}
		})
	} else {
		done <- true
	}

	log.Printf("Sending %d messages of size %d bytes...", iterations, messageSize)

	wg.Add(iterations)

	start = time.Now()
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

		if i%interval == 0 && i != 0 && showProgress {
			log.Printf("async send count = %d", i)
		}
	}
	wg.Wait()
	<-done
	end := time.Now()

	if replicator != nil {
		log.Printf("Trying to wait for acks to return to replicator before we shut it down")
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

		if showStats {
			log.Printf("Replicator Stats:\n\n%s\n", statsJSON)
		}
	}

	sc2.Close()
	sc.Close()
	nc2.Close()
	nc.Close()

	diff := end.Sub(start)
	rate := float64(iterations) / float64(diff.Seconds())
	sizeRate := float64(messageSize) * rate / (1024 * 1024)

	if pubOnly {
		log.Printf("Sent %d messages to a streaming channel %s", iterations, diff)
	} else if subOnly {
		log.Printf("Read %d messages from a streaming channel in %s", iterations, diff)
	} else if direct {
		log.Printf("Sent %d messages through a streaming channel to a streaming subscriber in %s", iterations, diff)
	} else {
		log.Printf("Sent %d messages through a channel to the replicator and read from another channel in %s", iterations, diff)
	}
	log.Printf("%.2f msgs/sec ~ %.2f MB/sec", rate, sizeRate)
}
