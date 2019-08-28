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
var maxOut int
var direct bool
var pubOnly bool
var subOnly bool
var repOnly bool
var showStats bool
var hideReplicatorLog bool
var showProgress bool
var in string
var out string
var pubFirst bool

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
	flag.IntVar(&maxOut, "maxout", 100, "maximum message out on the subscriber side, defaults to 1000")
	flag.StringVar(&natsURL, "nats", "nats://localhost:4222", "nats url, defaults to nats://localhost:4222")
	flag.StringVar(&stanClusterID, "stan", "test-cluster", "stan cluster id")
	flag.StringVar(&natsURL2, "nats2", "", "nats url for the subscriber side, defaults to nats://localhost:4222")
	flag.StringVar(&stanClusterID2, "stan2", "", "stan cluster id for the subscriber side, defaults to test-cluster")
	flag.BoolVar(&direct, "direct", false, "skip the replicator and just use streaming")
	flag.BoolVar(&repOnly, "rep", false, "only run the replicator")
	flag.BoolVar(&pubFirst, "pubFirst", false, "pre-run the publiser, then start the replicator and/or subscriber")
	flag.BoolVar(&pubOnly, "pub", false, "only publish, don't subscribe, useful for testing send times across a long connection")
	flag.BoolVar(&subOnly, "sub", false, "only time the reads, useful for testing read times across a long connection, timer starts with first receive")
	flag.BoolVar(&showStats, "stats", false, "print replicator stats, if not direct")
	flag.BoolVar(&hideReplicatorLog, "hide", false, "hide the replicator log")
	flag.BoolVar(&showProgress, "progress", true, "show progress")
	flag.StringVar(&in, "in", "", "channel to publish to, and replicate, defaults to a random string")
	flag.StringVar(&out, "out", "", "channel to read from and replicate to, defaults to a random string")
	flag.Parse()

	var replicator *core.NATSReplicator
	var startPub time.Time
	var endPub time.Time
	var startSub time.Time
	var endSub time.Time
	var startRep time.Time
	var endRep time.Time

	incoming := nuid.Next()
	outgoing := nuid.Next()
	msgString := strings.Repeat("a", messageSize)
	msg := []byte(msgString)
	msgLen := len(msg)
	pubwg := sync.WaitGroup{}
	repwg := sync.WaitGroup{}
	subwg := sync.WaitGroup{}
	interval := int(iterations / 10)
	repTimeout := make(chan bool, 1)

	if natsURL2 == "" {
		natsURL2 = natsURL
	}

	if stanClusterID2 == "" {
		stanClusterID2 = stanClusterID
	}

	if in != "" {
		incoming = in
	}

	if out != "" {
		outgoing = out
	}

	if pubOnly || subOnly {
		direct = true
		log.Printf("Pub and sub only mode runs with direct mode, no replicator is used")
	}

	if repOnly {
		direct = false
		log.Printf("Replicator only mode sets direct mode to false")
	}

	if direct {
		log.Printf("Direct mode uses the same nats url and stan cluster id for both connections")
		if in == "" && out == "" {
			log.Printf("Unless custom channels are set, the same channel is used for read/write in direct mode")
			outgoing = incoming
		}
		stanClusterID2 = stanClusterID
		natsURL2 = natsURL
	}

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

	log.Printf("Incoming/Replicated channel %s : Outgoing/Subscribed channel: %s", incoming, outgoing)

	if pubFirst || pubOnly {
		log.Printf("Sending %d messages of size %d bytes...", iterations, messageSize)
		pubwg.Add(iterations)
		pubCount := 0
		startPub = time.Now()
		for i := 0; i < iterations; i++ {
			_, err := sc.PublishAsync(incoming, msg, func(aguid string, err error) {
				pubCount++
				if err != nil {
					log.Fatalf("error in ack handler, %s", err.Error())
				}
				if (pubCount%interval == 0 || pubCount == iterations) && showProgress {
					log.Printf("async send count = %d", pubCount)
				}
				pubwg.Done()
			})

			if err != nil {
				log.Fatalf("error publishing message, %s", err.Error())
			}
		}
		pubwg.Wait()
		endPub = time.Now()
	}

	if !pubOnly && !repOnly {
		subwg.Add(iterations)
		subCount := 0
		_, err := sc2.Subscribe(outgoing, func(msg *stan.Msg) {
			if subCount == 0 {
				startSub = time.Now() // start timing on the first message
			}
			subCount++
			if (subCount%interval == 0 || subCount == iterations) && showProgress {
				log.Printf("received count = %d", subCount)
			}

			if len(msg.Data) != msgLen {
				log.Fatalf("received message that is the wrong size %d != %d", len(msg.Data), msgLen)
			}

			if subCount <= iterations {
				subwg.Done()
			}
		}, stan.DeliverAllAvailable(), stan.MaxInflight(maxOut))

		if err != nil {
			log.Fatalf("error subscribing to %s, %s", outgoing, err.Error())
		}
	}

	if !direct {
		connect := []conf.ConnectorConfig{
			{
				Type:                    "StanToStan",
				IncomingConnection:      "stan",
				OutgoingConnection:      "stan2",
				IncomingChannel:         incoming,
				OutgoingChannel:         outgoing,
				IncomingStartAtSequence: 0,
				IncomingMaxInflight:     int64(maxOut),
			},
		}

		var err error
		replicator, err = startReplicator(connect)
		if err != nil {
			log.Fatalf("error starting replicator, %s", err.Error())
		}

		// Start trying to capture the replicator ack activity
		repwg.Add(1)
		go func() {
			lastRepInterval := int64(0)
			repTicker := time.NewTicker(100 * time.Millisecond)
		loop:
			for {
				select {
				case t := <-repTicker.C:
					reqcount := replicator.SafeStats().RequestCount
					if reqcount >= 0 && startRep.IsZero() {
						startRep = t
					}
					if reqcount >= int64(iterations) {
						endRep = t
						break loop
					}

					curInterval := reqcount / int64(interval)

					if curInterval > lastRepInterval && showProgress {
						lastRepInterval = curInterval
						log.Printf("replicated count = %d", reqcount)
					}
				case <-repTimeout:
					break loop
				}
			}
			repwg.Done()
			repTicker.Stop()
		}()
	}

	if !pubFirst && !pubOnly && !subOnly && !repOnly {
		log.Printf("Sending %d messages of size %d bytes...", iterations, messageSize)
		pubwg.Add(iterations)
		pubCount := 0
		startPub = time.Now()
		for i := 0; i < iterations; i++ {
			_, err := sc.PublishAsync(incoming, msg, func(aguid string, err error) {
				pubCount++
				if err != nil {
					log.Fatalf("error in ack handler, %s", err.Error())
				}
				if (pubCount%interval == 0 || pubCount == iterations) && showProgress {
					log.Printf("async send count = %d", pubCount)
				}
				pubwg.Done()
			})

			if err != nil {
				log.Fatalf("error publishing message, %s", err.Error())
			}
		}
		pubwg.Wait()
		endPub = time.Now()
	}

	if !pubOnly && !repOnly {
		subwg.Wait()
		endSub = time.Now()
	}

	if !direct {

		if !repOnly {
			log.Printf("Waiting for acks to return to replicator before we shut it down, timeout of %d seconds", 10)
			go func() {
				stop := time.Now().Add(10 * time.Second)
				ticker := time.NewTicker(500 * time.Millisecond)
				for t := range ticker.C {
					if t.After(stop) {
						repTimeout <- true
						break
					}
				}
				ticker.Stop()
			}()
		} else {
			log.Printf("Waiting for acks to replicator without timeout")
		}

		repwg.Wait()

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

	if !direct && endRep.IsZero() {
		log.Printf("Test Failed, replicator did not receive all of the acks within the timeout")
		return
	}

	var totalDiff time.Duration

	if pubOnly {
		totalDiff = endPub.Sub(startPub)
		log.Printf("Sent %d messages to a streaming channel %s", iterations, totalDiff)
	} else if subOnly {
		totalDiff = endSub.Sub(startSub)
		log.Printf("Read %d messages from a streaming channel in %s", iterations, totalDiff)
	} else if direct {
		totalDiff = endSub.Sub(startPub)
		log.Printf("Sent %d messages through a streaming channel to a streaming subscriber in %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 2*iterations)
	} else if repOnly {
		totalDiff = endRep.Sub(startRep)
		log.Printf("Replicated %d messages %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 2*iterations)
	} else {
		totalDiff = endSub.Sub(startPub)
		log.Printf("Sent %d messages through a channel to the replicator and read from another channel in %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 4*iterations)
	}

	if pubFirst {
		log.Printf("Messages were pushed to streaming before a replicator or subscriber was created")
	}

	totalRate := float64(iterations) / float64(totalDiff.Seconds())
	totalSizeRate := float64(messageSize) * totalRate / (1024 * 1024)
	log.Printf("Total stats - %.2f msgs/sec ~ %.2f MB/sec (using %d full paths)", totalRate, totalSizeRate, iterations)

	if !repOnly {
		pubDiff := endPub.Sub(startPub)
		pubRate := float64(iterations) / float64(pubDiff.Seconds())
		pubSizeRate := float64(messageSize) * pubRate / (1024 * 1024)
		log.Printf("  Pub stats - %.2f msgs/sec ~ %.2f MB/sec", pubRate, pubSizeRate)
	}

	if !pubOnly && !repOnly {
		subDiff := endSub.Sub(startSub)
		subRate := float64(iterations) / float64(subDiff.Seconds())
		subSizeRate := float64(messageSize) * subRate / (1024 * 1024)
		log.Printf("  Sub stats - %.2f msgs/sec ~ %.2f MB/sec", subRate, subSizeRate)
	}

	if !direct {
		repDiff := endRep.Sub(startRep)
		repRate := float64(iterations) / float64(repDiff.Seconds())
		repSizeRate := float64(messageSize) * repRate / (1024 * 1024)
		log.Printf("  Rep stats - %.2f msgs/sec ~ %.2f MB/sec", repRate, repSizeRate)
	}
}
