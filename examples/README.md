# FSMP C++ Echo Example

This directory contains a minimal cross-process echo demo built on the `fsmp` C++ runtime:

- `echo_server.cpp`
- `echo_client.cpp`

The goal of these programs is to manually verify that the `fsmpcpp` runtime can:

- start a real server process
- accept a client connection from another process
- resolve a remote container by name
- send a message and receive an echo reply
- expose runtime status through the built-in telnet server

## Build

From the project root:

```bash
cmake -S . -B build
cmake --build build --target echo_server echo_client
```

## Run

Start the server first:

```bash
./build/echo_server
```

You should see output similar to:

```text
fsmp echo server started
fsmp port: 2100
telnet port: 2000
container name: echo
telnet command: echostats
press Ctrl+C to stop
```

Notes:

- The FSMP server listens on port `2100`.
- The telnet server requests port `2000`.
- If telnet port `2000` is already in use, the runtime may fall back to another available port.

Then start the client in another terminal:

```bash
./build/echo_client 127.0.0.1
```

Type a line and press Enter:

```text
hello from fsmpcpp
```

The client should print:

```text
[echo] hello from fsmpcpp
```

Type `quit` or `exit` to close the client.

Press `Ctrl+C` in the server terminal to stop the server.

## Telnet

Open a telnet session to the server:

```bash
telnet 127.0.0.1 2000
```

If port `2000` was not available, use the actual telnet port printed by `echo_server`.

The prompt defaults to:

```text
echo_server$
```

Useful commands:

```text
help
status
nodes
containers
timers
echostats
```

Example:

```text
echo_server$ echostats
fsmp_port=2100
container_name=echo
connected_events=1
message_count=1
echo_server$
```

## What This Example Verifies

This echo example exercises the `fsmpcpp` runtime path, not a separate TCP implementation. In particular it verifies:

- `Instance::startServer(...)`
- `Instance::connectNode(...)`
- `Instance::queryRemoteContainer(...)`
- `Instance::sendMessage(...)`
- remote delivery back to another process

## Troubleshooting

If `telnet localhost 2000` first prints an IPv6 connection refusal and then succeeds over IPv4, that is usually harmless. It means:

- `localhost` resolved to `::1` first
- the current telnet listener is bound on IPv4
- the client retried on `127.0.0.1` and connected successfully

Use this to avoid the extra line:

```bash
telnet 127.0.0.1 2000
```
