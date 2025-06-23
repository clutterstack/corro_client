defmodule CorroClient do
  @moduledoc """
  Elixir client library for interacting with Corrosion database clusters.

  CorroClient provides a comprehensive interface for working with Corrosion,
  a SQLite-based distributed database. It supports:

  - **Arbitrary SQL queries and transactions** - Full SQL support with parameters
  - **Cluster operations** - Inspect cluster members, peers, and system state
  - **Real-time subscriptions** - Stream database changes with robust reconnection
  - **Configurable HTTP transport** - Built on Req/Finch with connection pooling

  ## Quick Start

      # Connect to a Corrosion node
      {:ok, conn} = CorroClient.connect("http://localhost:8081")

      # Execute queries
      {:ok, users} = CorroClient.query(conn, "SELECT * FROM users WHERE active = ?", [true])

      # Execute transactions
      {:ok, _} = CorroClient.transaction(conn, [
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
        "UPDATE stats SET user_count = user_count + 1"
      ])

      # Get cluster information
      {:ok, cluster_info} = CorroClient.get_cluster_info(conn)

      # Subscribe to real-time changes
      {:ok, sub_pid} = CorroClient.subscribe(conn, "SELECT * FROM messages ORDER BY created_at DESC",
        on_event: fn
          {:new_row, msg} -> IO.puts("New message: \#{msg["content"]}")
          {:change, "UPDATE", msg} -> IO.puts("Updated message: \#{msg["content"]}")
          event -> IO.inspect(event)
        end,
        on_connect: fn id -> IO.puts("Subscription connected: \#{id}") end
      )

  ## Configuration

  Connection options support all Req configuration including Finch pooling:

      {:ok, conn} = CorroClient.connect("http://localhost:8081",
        receive_timeout: 10_000,
        finch_options: [
          pool_timeout: 5_000,
          pool_max_idle_time: 30_000
        ]
      )

  ## Architecture

  CorroClient is built with separate modules for different concerns:

  - `CorroClient.Client` - Low-level HTTP transport and SQL execution
  - `CorroClient.Cluster` - High-level cluster operations and member parsing
  - `CorroClient.Subscriber` - Real-time streaming subscriptions with reconnection

  This allows you to use individual modules directly if needed, or the unified
  interface provided by this main module.
  """

  alias CorroClient.{Client, Cluster, Subscriber}

  @type connection :: Client.connection()
  @type query_result :: Client.query_result()
  @type transaction_result :: Client.transaction_result()

  # Connection Management

  @doc """
  Creates a connection to a Corrosion node.

  ## Parameters
  - `base_url`: The base URL of the Corrosion API (e.g., "http://127.0.0.1:8081")
  - `options`: Additional Req options including Finch configuration

  ## Options
  - `:receive_timeout` - Request timeout in milliseconds (default: 5000)
  - `:finch_options` - Finch-specific options for connection pooling
  - `:connect_options` - Connection-specific options

  ## Examples
      conn = CorroClient.connect("http://localhost:8081")

      # With custom timeouts and pooling
      conn = CorroClient.connect("http://localhost:8081",
        receive_timeout: 10_000,
        finch_options: [pool_timeout: 5000, pool_max_idle_time: 30_000]
      )
  """
  @spec connect(String.t(), keyword()) :: connection()
  defdelegate connect(base_url, options \\ []), to: Client

  @doc """
  Test connectivity to a Corrosion node.

  ## Parameters
  - `connection`: Connection created with `connect/2`

  ## Returns
  - `:ok` if connection successful
  - `{:error, reason}` if connection failed
  """
  @spec ping(connection()) :: :ok | {:error, term()}
  defdelegate ping(connection), to: Client

  # Query and Transaction Operations

  @doc """
  Execute a SQL query with optional parameters.

  Supports arbitrary SQL queries including SELECT, and any read operations.
  For write operations, use `transaction/2`.

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `query`: SQL query string
  - `params`: Optional query parameters (default: [])

  ## Returns
  - `{:ok, results}` - List of maps representing rows
  - `{:error, reason}` - Error details

  ## Examples
      CorroClient.query(conn, "SELECT * FROM users")
      # => {:ok, [%{"id" => 1, "name" => "Alice"}]}

      CorroClient.query(conn, "SELECT * FROM users WHERE age > ?", [21])
      # => {:ok, [%{"id" => 2, "name" => "Bob", "age" => 25}]}
  """
  @spec query(connection(), String.t(), list()) :: query_result()
  defdelegate query(connection, query, params \\ []), to: Client, as: :execute_query

  @doc """
  Execute a list of SQL statements as an atomic transaction.

  All statements execute successfully or the entire transaction is rolled back.
  Supports arbitrary SQL including INSERT, UPDATE, DELETE, and DDL operations.

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `statements`: List of SQL statements to execute atomically

  ## Returns
  - `{:ok, response}` - Transaction succeeded
  - `{:error, reason}` - Transaction failed and was rolled back

  ## Examples
      statements = [
        "INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')",
        "UPDATE stats SET user_count = user_count + 1",
        "INSERT INTO audit_log (action, user_id) VALUES ('user_created', last_insert_rowid())"
      ]
      CorroClient.transaction(conn, statements)
      # => {:ok, response}
  """
  @spec transaction(connection(), [String.t()]) :: transaction_result()
  defdelegate transaction(connection, statements), to: Client, as: :execute_transaction

  # Cluster Operations

  @doc """
  Gets cluster member information from the __corro_members table.

  ## Parameters
  - `connection`: Connection created with `connect/2`

  ## Returns
  - `{:ok, members}` where members is a list of parsed member maps
  - `{:error, reason}` on failure

  ## Examples
      {:ok, members} = CorroClient.get_cluster_members(conn)
      length(members)
      # => 3
  """
  @spec get_cluster_members(connection()) :: {:ok, [map()]} | {:error, term()}
  defdelegate get_cluster_members(connection), to: Cluster, as: :get_members

  @doc """
  Gets comprehensive cluster information.

  Queries essential Corrosion system tables to build complete cluster state
  including member counts, peer information, and node status.

  ## Parameters
  - `connection`: Connection created with `connect/2`

  ## Returns
  Map containing detailed cluster information

  ## Examples
      {:ok, info} = CorroClient.get_cluster_info(conn)
      info.total_active_nodes
      # => 3
  """
  @spec get_cluster_info(connection()) :: {:ok, Cluster.cluster_info()} | {:error, term()}
  defdelegate get_cluster_info(connection), to: Cluster, as: :get_info

  @doc """
  Gets tracked peers from the crsql_tracked_peers table.

  ## Parameters
  - `connection`: Connection created with `connect/2`

  ## Returns
  - `{:ok, peers}` - List of tracked peer information
  - `{:error, reason}` - Query failed or table doesn't exist
  """
  @spec get_tracked_peers(connection()) :: {:ok, [map()]} | {:error, term()}
  defdelegate get_tracked_peers(connection), to: Cluster

  @doc """
  Gets the local node's membership status by matching against a gossip port.

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `local_gossip_port`: The local node's gossip port for identification

  ## Returns
  - `{:ok, member}` - Local node member information
  - `{:error, reason}` - Node not found or query failed
  """
  @spec get_local_member_status(connection(), integer()) :: {:ok, map()} | {:error, term()}
  defdelegate get_local_member_status(connection, local_gossip_port), to: Cluster

  @doc """
  Gets basic database schema information.

  ## Parameters
  - `connection`: Connection created with `connect/2`

  ## Returns
  Map containing database information including table list and counts
  """
  @spec get_database_info(connection()) :: {:ok, map()} | {:error, term()}
  defdelegate get_database_info(connection), to: Cluster

  # Subscription Operations

  @doc """
  Start a real-time subscription to database changes.

  Creates a streaming connection to Corrosion's subscription endpoint for
  real-time change notifications. Supports robust reconnection with
  exponential backoff.

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `query`: SQL query to subscribe to
  - `options`: Subscription configuration options

  ## Options
  - `:on_event` - Callback function for data events (required)
  - `:on_connect` - Callback when subscription connects (optional)
  - `:on_error` - Callback for errors (optional)
  - `:on_disconnect` - Callback when subscription disconnects (optional)
  - `:max_reconnect_attempts` - Maximum reconnection attempts (default: 5)
  - `:reconnect_delays` - Delays between reconnection attempts in ms

  ## Event Types
  The `:on_event` callback receives these event types:
  - `{:subscription_ready}` - Initial data loaded, now streaming live changes
  - `{:columns_received, columns}` - Column information received
  - `{:initial_row, row_data}` - Initial data row (during startup)
  - `{:new_row, row_data}` - New row inserted
  - `{:change, change_type, row_data}` - Row updated ("UPDATE", "DELETE", etc.)

  ## Returns
  - `{:ok, pid}` - Subscription process started successfully
  - `{:error, reason}` - Failed to start subscription

  ## Examples
      # Basic subscription
      {:ok, pid} = CorroClient.subscribe(conn, "SELECT * FROM messages ORDER BY created_at DESC",
        on_event: fn
          {:new_row, msg} -> IO.puts("New: \#{msg["content"]}")
          {:change, "UPDATE", msg} -> IO.puts("Updated: \#{msg["content"]}")
          event -> IO.inspect(event)
        end
      )

      # With connection and error callbacks
      {:ok, pid} = CorroClient.subscribe(conn, "SELECT * FROM users",
        on_event: &handle_user_event/1,
        on_connect: fn watch_id -> Logger.info("Connected: \#{watch_id}") end,
        on_error: fn error -> Logger.error("Subscription error: \#{inspect(error)}") end,
        max_reconnect_attempts: 10
      )
  """
  @spec subscribe(connection(), String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def subscribe(connection, query, options) do
    Subscriber.start_subscription(connection, [query: query] ++ options)
  end

  @doc """
  Get the current status of a subscription.

  ## Parameters
  - `subscription_pid`: Subscription process pid returned by `subscribe/3`

  ## Returns
  Map containing status information including connection state and retry attempts
  """
  @spec subscription_status(pid()) :: map()
  defdelegate subscription_status(subscription_pid), to: Subscriber, as: :get_status

  @doc """
  Manually restart a subscription.

  Useful for recovering from errors or testing reconnection logic.

  ## Parameters
  - `subscription_pid`: Subscription process pid returned by `subscribe/3`

  ## Returns
  `:ok` - Restart initiated
  """
  @spec restart_subscription(pid()) :: :ok
  defdelegate restart_subscription(subscription_pid), to: Subscriber, as: :restart

  @doc """
  Stop a subscription.

  ## Parameters
  - `subscription_pid`: Subscription process pid returned by `subscribe/3`

  ## Returns
  `:ok` - Subscription stopped
  """
  @spec stop_subscription(pid()) :: :ok
  defdelegate stop_subscription(subscription_pid), to: Subscriber, as: :stop
end
