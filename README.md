# go-moblin-assistant

A Go implementation of the Moblin remote control assistant protocol. This project provides a WebSocket server to receive telemetry from the Moblin IRL streaming app and a simple CLI REPL to demonstrate usage.

## Prerequisites

* Network access between the hosting server and the Moblin device.
* Moblin app configured to use the assistant endpoint (e.g., `ws://192.168.1.10:8080/ws`). If using a reverse proxy with TLS termination, configure Moblin to use `wss://`.

## Configuration and Execution

The simple cli requires the `MOBLIN_REMOTE_PASS` environment variable to authenticate incoming WebSocket connections. This string must exactly match the password configured in the Moblin app.
