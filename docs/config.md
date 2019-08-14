# NATS-Replicator Configuration

The replicator uses a single configuration file passed on the command line or environment variable. Configuration is organized into a root section and several blocks.

* [Specifying the Configuration File](#specify)
* [Shared](#root)
* [TLS](#tls)
* [Logging](#logging)
* [Monitoring](#monitoring)
* [NATS](#nats)
* [NATS Streaming](#stan)
* [Connectors](#connectors)

The configuration file format matches the NATS server and supports file includes of the form:

```yaml
include "./includes/connectors.conf"
```

<a name="specify"></a>

## Specifying the Configuration File

To set the configuration on the command line, use:

```bash
% nats-replicator -c <path to config file>
```

To set the configuration file using an environment variable, export `NATS_REPLICATOR_CONFIG` with the path to the configuration.

<a name="root"></a>

## Root Section

The root section:

```yaml
reconnectinterval: 5000,
```

can currently contain settings for:

* `reconnectinterval` - this value, in milliseconds, is the time used in between reconnection attempts for a connector when it fails. For example, if a connector loses access to NATS, the replicator will try to restart it every `reconnectinterval` milliseconds.

## TLS <a name="tls"></a>

NATS, streaming and HTTP configurations all take an optional TLS setting. The TLS configuration takes three possible settings:

* `root` - file path to a CA root certificate store, used for NATS connections
* `cert` - file path to a server certificate, used for HTTPS monitoring and optionally for client side certificates with NATS
* `key` - key for the certificate store specified in cert

<a name="logging"></a>

### Logging

Logging is configured in a manner similar to the nats-server:

```yaml
logging: {
  time: true,
  debug: false,
  trace: false,
  colors: true,
  pid: false,
}
```

These properties are configured for:

* `time` - include the time in logging statements
* `debug` - include debug logging
* `trace` - include verbose, or trace, logging
* `colors` - colorize the logging statements
* `pid` - include the process id in logging statements

<a name="monitoring"></a>

## Monitoring

The monitoring section:

```yaml
monitoring: {
  httpsport: -1,
  tls: {
      cert: /a/server-cert.pem,
      key: /a/server-key.pem,
  }
}
```

Is used to configure an HTTP or HTTPS port, as well as TLS settings when HTTPS is used.

* `httphost` - the network interface to publish monitoring on, valid for HTTP or HTTPS. An empty value will tell the replicator to use all available network interfaces.
* `httpport` - the port for HTTP monitoring, no TLS configuration is expected, a value of -1 will tell the replicator to use an ephemeral port, the port will be logged on startup.

`2019/03/20 12:06:38.027822 [INF] starting http monitor on :59744`

* `httpsport` - the port for HTTPS monitoring, a TLS configuration is expected, a value of -1 will tell the server to use an ephemeral port, the port will be logged on startup.
* `tls` - a [TLS configuration](#tls).

The `httpport` and `httpsport` settings are mutually exclusive, if both are set to a non-zero value the replicator will not start.

<a name="nats"></a>

## NATS

The replicator can make multiple connections to NATS. Each connection has a name so that connectors can refer to it. Streaming connections also use the name of a NATS connection to refer to it. Configuration is through the `nats` section of the config file, which defines an array of connection parameters:

```yaml
nats: [
  {
    Name: "connection_one",
    Servers: ["localhost:4222"],
    ConnectTimeout: 5000,
    MaxReconnects: 5,
    ReconnectWait: 5000,
  }
]
```

NATS can be configured with the following properties:

* `name` - the unique name used to refer to this configuration/connection
* `servers` - an array of server URLS
* `connecttimeout` - the time, in milliseconds, to wait before failing to connect to the NATS server
* `reconnectwait` - the time, in milliseconds, to wait between reconnect attempts
* `maxreconnects` - the maximum number of reconnects to try before exiting the replicator with an error.
* `tls` - (optional) [TLS configuration](#tls). If the NATS server uses unverified TLS with a valid certificate, this setting isn't required.
* `usercredentials` - (optional) the path to a credentials file for connecting to NATs.

<a name="stan"></a>

## NATS Streaming

The replicator can make multiple connections to streaming servers. Each streaming connection must refer to a NATS connection by name. This reduces the configuration clutter by keeping NATS parameters in the NATS section. Configuration is through the `stan` section of the config file, which defines an array:

```yaml
stan: [
  {
    Name: "stan_one",
    NATSConnection: "connection_one",
    ClusterID: "test-cluster"
    ClientID: "replicator_one"
  }
]
```

Multiple streaming connections could use the same NATS connection.

NATS streaming can be configured with the following properties:

* `name` - the unique name used to refer to this configuration/connection
* `natsconnection` - the unique name of the nats connection to use for this streaming connection
* `clusterid` - the cluster id for the NATS streaming server.
* `clientid` - the client id for the connection.
* `pubackwait` - the time, in milliseconds, to wait before a publish fails due to a timeout.
* `discoverprefix` - the discover prefix for the streaming server.
* `maxpubacksinflight` - maximum pub ACK messages that can be in flight for this connection.
* `connectwait` - the time, in milliseconds, to wait before failing to connect to the streaming server.

<a name="connectors"></a>

## Connectors

The final piece of the replicator configuration is the `connect` section. Connect specifies an array of connector configurations. All connector configs use the same format, relying on optional settings to determine what the do.

```yaml
connect: [
  {
      type: NATSToNATS,
      id: "alpha",
      IncomingSubject: "in",
      IncomingConnection: "connection_one",
      OutgoingSubject: "out",
      OutgoingConnection: "connection_one",
  },
],
```

The most important property in the connector configuration is the `type`. The type determines which kind of connector is instantiated. Available, uni-directional, types include:

* `NATSToNATS` - a subject to subject connector
* `NATSToStan` - a subject to streaming connector
* `StanToNATS` - a streaming to subject connector
* `STANToSTAN` - a streaming to streaming connector

All connectors can have an optional id, which is used in monitoring:

* `id` - (optional) user defined id that will tag the connection in monitoring JSON.

All connectors require a configuration for the connection to use:

* `incomingconnection` - the name of a NATS or streaming connection to subscribe to
* `outgoingconnection` - the name of a NATS or streaming connection to publish to

For NATS connections, specify:

* `incomingsubject` - the subject to subscribe to, depending on the connections direction.
* `incomingqueuename` - the queue group to use in subscriptions, this is optional but useful for load balancing.
* `outgoingsubject` - the subject to publish to, depending on the connections direction.

Keep in mind that NATS queue groups do not guarantee ordering, since the queue subscribers can be on different nats-servers in a cluster. So if you have to replicators running with connectors on the same NATS queue/subject pair and have a high message rate you may get messages to the receiver "out of order." Also, note that there is no outgoing queue.

These settings are directional depending so a `NATSToStan` connector would use an `incomingsubject` while a `StanToNATS` connector would use an `outgoingsubject`. Connectors ignore settings they don't need.

For streaming connections, the channel setting is required (directionality dependent), the others are optional:

* `incomingchannel` - the streaming channel to subscribe to.
* `outgoingchannel` - the streaming channel to publish to.
* `incomingdurablename` - (optional) durable name for the streaming subscription (if appropriate.)
* `incomingstartatsequence` - (optional) start position, use -1 for start with last received, 0 for deliver all available (the default.)
* `incomingstartattime` - (optional) the start position as a time, in Unix seconds since the epoch, mutually exclusive with `startatsequence`.