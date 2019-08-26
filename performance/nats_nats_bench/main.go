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
)

var iterations int
var messageSize int
var natsURL string
var natsURL2 string
var direct bool
var pubOnly bool
var subOnly bool
var repOnly bool
var showStats bool
var hideReplicatorLog bool
var showProgress bool
var in string
var out string

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
	flag.StringVar(&natsURL, "nats", "nats://localhost:4222", "nats url, defaults to nats://localhost:4222")
	flag.StringVar(&natsURL2, "nats2", "", "nats url for the subscriber side, defaults to nats://localhost:4222")
	flag.BoolVar(&direct, "direct", false, "skip the replicator and just use nats")
	flag.BoolVar(&repOnly, "rep", false, "only run the replicator")
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

	if direct {
		log.Printf("Direct mode uses the same nats url for both connections")
		if in == "" && out == "" {
			log.Printf("Unless custom subjects are set, the same subject is used for read/write in direct mode")
			outgoing = incoming
		}
		natsURL2 = natsURL
	}

	nc, err := nats.Connect(natsURL, nats.Timeout(time.Second*5), nats.MaxReconnects(5), nats.ReconnectWait(time.Second*5))
	if err != nil {
		log.Fatalf("error connecting to nats, %s", err.Error())
	}
	defer nc.Close()

	nc2, err := nats.Connect(natsURL2, nats.Timeout(time.Second*5), nats.MaxReconnects(5), nats.ReconnectWait(time.Second*5))
	if err != nil {
		log.Fatalf("error connecting to nats, %s", err.Error())
	}
	defer nc.Close()

	log.Printf("Incoming/Replicated subject %s : Outgoing/Subscribed subject: %s", incoming, outgoing)

	if !pubOnly && !repOnly {
		subwg.Add(iterations)
		subCount := 0
		_, err := nc.Subscribe(outgoing, func(msg *nats.Msg) {
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
		})

		if err != nil {
			log.Fatalf("error subscribing to %s, %s", outgoing, err.Error())
		}
	}

	if !direct {
		connect := []conf.ConnectorConfig{
			{
				Type:               "NATSToNATS",
				IncomingConnection: "nats",
				OutgoingConnection: "nats2",
				IncomingSubject:    incoming,
				OutgoingSubject:    outgoing,
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
			repTicker := time.NewTicker(50 * time.Millisecond)
		loop:
			for {
				select {
				case t := <-repTicker.C:
					reqcount := replicator.SafeStats().RequestCount
					if reqcount >= 0 && startRep.IsZero() {
						startRep = t
					}

					curInterval := reqcount / int64(interval)

					if curInterval > lastRepInterval && showProgress {
						lastRepInterval = curInterval
						log.Printf("replicated count = %d", reqcount)
					}

					if reqcount >= int64(iterations) {
						endRep = t
						break loop
					}
				case <-repTimeout:
					break loop
				}
			}
			repwg.Done()
			repTicker.Stop()
		}()
	}

	if !subOnly && !repOnly {
		log.Printf("Sending %d messages of size %d bytes...", iterations, messageSize)
		pubwg.Add(iterations)
		startPub = time.Now()
		for i := 0; i < iterations; i++ {
			err := nc.Publish(incoming, msg)
			if err != nil {
				log.Fatalf("error publishing message, %s", err.Error())
			}
			if (i%interval == 0) && showProgress {
				log.Printf("pub count = %d", i)
			}
		}
		if showProgress {
			log.Printf("pub count = %d", iterations)
		}
		endPub = time.Now()
	}

	if !pubOnly && !repOnly {
		subwg.Wait()
		endSub = time.Now()
	}

	if !direct {

		if !repOnly {
			log.Printf("Waiting for replicator before we shut it down, timeout of %d seconds", 10)
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
			log.Printf("Waiting for replicator without timeout")
		}

		repwg.Wait()

		stats := replicator.SafeStats()
		statsJSON, _ := json.MarshalIndent(stats, "", "    ")

		replicator.Stop()

		if showStats {
			log.Printf("Replicator Stats:\n\n%s\n", statsJSON)
		}
	}

	nc2.Close()
	nc.Close()

	if !direct && endRep.IsZero() {
		log.Printf("Test Failed, replicator did replicate all the messages within the timeout")
		return
	}

	var totalDiff time.Duration

	if pubOnly {
		totalDiff = endPub.Sub(startPub)
		log.Printf("Sent %d messages to a subject %s", iterations, totalDiff)
	} else if subOnly {
		totalDiff = endSub.Sub(startSub)
		log.Printf("Read %d messages from a subject in %s", iterations, totalDiff)
	} else if direct {
		totalDiff = endSub.Sub(startPub)
		log.Printf("Sent %d messages through a subject to a subscriber in %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 2*iterations)
	} else if repOnly {
		totalDiff = endRep.Sub(startRep)
		log.Printf("Replicated %d messages %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 2*iterations)
	} else {
		totalDiff = endSub.Sub(startPub)
		log.Printf("Sent %d messages through a subject to the replicator and read from another subject in %s", iterations, totalDiff)
		log.Printf("Total messages moved were %d", 4*iterations)
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
