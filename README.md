# CorroClient

**The basis of this library was written the old-fashioned way in 2023 as modules within another project. I adulterated them with Claude in 2025, inside [another toy project](https://github.com/clutterstack/corro-port-ex), and the first upload of it as a library, including the initial version of this README, is the result of my asking Claude Sonnet to extract it into a library. As of this writing, I haven't even checked if it works.**

A comprehensive Elixir client library for interacting with [Corrosion](https://github.com/superfly/corrosion) database clusters.

Corrosion is a SQLite-based distributed database that provides strong consistency with multi-master replication. This library provides a complete interface for working with Corrosion clusters from Elixir applications.

## Features

- **Full SQL Support** - Execute arbitrary queries and transactions with parameterization
- **Cluster Operations** - Inspect cluster members, peers, and system state  
- **Real-time Subscriptions** - Stream database changes with robust reconnection logic
- **Connection Pooling** - Built on Req/Finch with configurable pooling and timeouts
- **Comprehensive Error Handling** - Detailed error reporting and graceful degradation
- **Zero Phoenix Dependencies** - Pure OTP/GenServer implementation

## Quick Start

```elixir
# Connect to a Corrosion node
{:ok, conn} = CorroClient.connect("http://localhost:8081")

# Execute queries with parameters
{:ok, users} = CorroClient.query(conn, "SELECT * FROM users WHERE active = ?", [true])

# Execute atomic transactions
{:ok, _} = CorroClient.transaction(conn, [
  "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
  "UPDATE stats SET user_count = user_count + 1"
])

# Get cluster information
{:ok, cluster_info} = CorroClient.get_cluster_info(conn)
IO.puts("Active nodes: #{cluster_info.total_active_nodes}")

# Subscribe to real-time changes
{:ok, sub_pid} = CorroClient.subscribe(conn, "SELECT * FROM messages ORDER BY created_at DESC",
  on_event: fn
    {:new_row, msg} -> IO.puts("New message: #{msg["content"]}")
    {:change, "UPDATE", msg} -> IO.puts("Updated: #{msg["content"]}")
    event -> IO.inspect(event)
  end,
  on_connect: fn watch_id -> IO.puts("Subscription connected: #{watch_id}") end
)
```

## Installation

Add `corro_client` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:corro_client, "~> 0.1.0"}
  ]
end
```

## Configuration

### Basic Connection

```elixir
# Simple connection
conn = CorroClient.connect("http://localhost:8081")

# With custom timeouts
conn = CorroClient.connect("http://localhost:8081", 
  receive_timeout: 10_000
)
```

### Connection Pooling

CorroClient uses Req (built on Finch) for HTTP transport. You can configure connection pooling:

```elixir
conn = CorroClient.connect("http://localhost:8081",
  finch_options: [
    pool_timeout: 5_000,
    pool_max_idle_time: 30_000,
    pool_size: 10
  ]
)
```

### Multi-node Connections

For cluster deployments, create separate connections to different nodes:

```elixir
nodes = [
  CorroClient.connect("http://node1:8081"),
  CorroClient.connect("http://node2:8081"), 
  CorroClient.connect("http://node3:8081")
]

# Round-robin queries across nodes
conn = Enum.random(nodes)
{:ok, result} = CorroClient.query(conn, "SELECT * FROM users")
```

## API Overview

### Core Operations

- `CorroClient.connect/2` - Create connection to a Corrosion node
- `CorroClient.ping/1` - Test connectivity
- `CorroClient.query/3` - Execute SQL queries with optional parameters
- `CorroClient.transaction/2` - Execute atomic transactions

### Cluster Inspection

- `CorroClient.get_cluster_info/1` - Comprehensive cluster state
- `CorroClient.get_cluster_members/1` - Active cluster members
- `CorroClient.get_tracked_peers/1` - Replication peer status
- `CorroClient.get_database_info/1` - Schema and table information

### Real-time Subscriptions

- `CorroClient.subscribe/3` - Start streaming subscription
- `CorroClient.subscription_status/1` - Check subscription health
- `CorroClient.restart_subscription/1` - Manually restart subscription
- `CorroClient.stop_subscription/1` - Stop subscription

## Subscription Events

Subscriptions receive these event types via the `:on_event` callback:

- `{:subscription_ready}` - Initial data loaded, now streaming live changes
- `{:columns_received, columns}` - Column information received  
- `{:initial_row, row_data}` - Initial data row (during startup)
- `{:new_row, row_data}` - New row inserted
- `{:change, change_type, row_data}` - Row updated ("UPDATE", "DELETE", etc.)

## Error Handling

The library provides comprehensive error handling:

```elixir
case CorroClient.query(conn, "SELECT * FROM users") do
  {:ok, users} -> 
    # Process results
    IO.puts("Found #{length(users)} users")
    
  {:error, {:connection_error, reason}} ->
    # Network/connectivity issues
    Logger.error("Connection failed: #{inspect(reason)}")
    
  {:error, {:http_error, status, body}} ->
    # HTTP-level errors (4xx, 5xx)
    Logger.error("HTTP #{status}: #{inspect(body)}")
end
```

Subscriptions include automatic reconnection with exponential backoff:

```elixir
{:ok, pid} = CorroClient.subscribe(conn, "SELECT * FROM events",
  on_event: &handle_event/1,
  on_error: fn error -> Logger.error("Subscription error: #{inspect(error)}") end,
  max_reconnect_attempts: 10,
  reconnect_delays: [1_000, 2_000, 5_000, 10_000, 30_000]
)
```

## Architecture

CorroClient is built with clean separation of concerns:

- **`CorroClient.Client`** - Low-level HTTP transport and SQL execution
- **`CorroClient.Cluster`** - High-level cluster operations and data parsing  
- **`CorroClient.Subscriber`** - Real-time streaming with reconnection logic

The main `CorroClient` module provides a unified interface that delegates to these specialized modules. You can also use the individual modules directly for more control.

## Testing

Run the test suite:

```bash
mix test
```

The tests include unit tests for parsing logic, connection management, and subscription handling. Integration tests require a running Corrosion instance.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Run `mix test` and `mix format`
5. Commit your changes (`git commit -am 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built for the [Corrosion](https://github.com/superfly/corrosion) distributed SQLite database
- Extracted from the [CorroPort](https://github.com/your_org/corro_port) monitoring application
- Uses [Req](https://github.com/wojtekmach/req) for robust HTTP client functionality

