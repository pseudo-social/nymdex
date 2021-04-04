# nymdex

A language agnostic `neard` real time/historical data source, it can currently produce to Kafka and AMQP clients (such as RabbitMQ).

---

## Rename

This project has become more a utility to source data into many different transport layers, mostly queue and topic based. What should we call it?

**Any ideas?!**

---


# Devleopment


## Get Up & Running

* Clone the repository: `git clone git@github.com:pseudo-exchange/nymdex.git`
  * For now your NEAR configuration should be at `~/.near/config.json` this will be a configuration option later
* Change directories into the repository: `cd nymdex`
* Run the alpha producer setup: `cargo run`

## Documentation

* Run: `make docs`
  * OR: `make docs-view` to open locally in the browser
