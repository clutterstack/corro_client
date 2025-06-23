defmodule CorroClient.Cluster do
  @moduledoc """
  High-level API for querying Corrosion cluster state and system information.

  This module provides functions for inspecting cluster membership,
  tracked peers, system tables, and node information using the low-level
  `CorroClient.Client` for HTTP transport.
  """
  require Logger

  alias CorroClient.Client

  @type connection :: Client.connection()
  @type cluster_info :: %{
          members: [map()],
          tracked_peers: [map()],
          member_count: non_neg_integer(),
          active_member_count: non_neg_integer(),
          peer_count: non_neg_integer(),
          total_active_nodes: non_neg_integer()
        }

  @doc """
  Gets cluster member information by querying the __corro_members table.

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`

  ## Returns
  - `{:ok, members}` where members is a list of parsed member maps
  - `{:error, reason}` on failure

  ## Examples
      {:ok, members} = CorroClient.Cluster.get_members(conn)
      length(members)
      # => 3
  """
  @spec get_members(connection()) :: {:ok, [map()]} | {:error, term()}
  def get_members(connection) do
    query = "SELECT * FROM __corro_members"

    with {:ok, members} <- Client.execute_query(connection, query) do
      parsed_members = Enum.map(members, &parse_member_foca_state/1)
      {:ok, parsed_members}
    end
  end

  @doc """
  Gets comprehensive cluster information from Corrosion system tables.

  Queries the essential Corrosion system tables to build cluster state:
  - `__corro_members`: Cluster members and their status
  - `crsql_tracked_peers`: Tracked peers for replication

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`

  ## Returns
  Map containing cluster information with counts and node details.

  ## Examples
      {:ok, info} = CorroClient.Cluster.get_info(conn)
      info.total_active_nodes
      # => 3
  """
  @spec get_info(connection()) :: {:ok, cluster_info()} | {:error, term()}
  def get_info(connection) do
    # Start with basic structure
    cluster_data = %{
      members: [],
      tracked_peers: [],
      member_count: 0,
      active_member_count: 0,
      peer_count: 0,
      total_active_nodes: 0
    }

    # Get members and peers
    result =
      cluster_data
      |> fetch_members_with_activity(connection)
      |> fetch_tracked_peers(connection)
      |> calculate_total_active_nodes()

    {:ok, result}
  end

  @doc """
  Gets the local node's membership status by finding it in __corro_members.

  Attempts to identify the local node by matching gossip addresses against
  a provided gossip port.

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`
  - `local_gossip_port`: The local node's gossip port for identification

  ## Returns
  - `{:ok, member}` - Local node member information
  - `{:error, reason}` - Node not found or query failed

  ## Examples
      CorroClient.Cluster.get_local_member_status(conn, 8787)
      # => {:ok, %{"member_state" => "Alive", ...}}
  """
  @spec get_local_member_status(connection(), integer()) :: {:ok, map()} | {:error, term()}
  def get_local_member_status(connection, local_gossip_port) do
    case get_members(connection) do
      {:ok, members} ->
        # Find member whose gossip address matches our local gossip port
        local_member =
          Enum.find(members, fn member ->
            case Map.get(member, "member_addr") do
              addr when is_binary(addr) ->
                case String.split(addr, ":") do
                  [_ip, port_str] ->
                    case Integer.parse(port_str) do
                      {port, _} -> port == local_gossip_port
                      _ -> false
                    end

                  _ ->
                    false
                end

              _ ->
                false
            end
          end)

        case local_member do
          nil -> {:error, :not_found_in_members}
          member -> {:ok, member}
        end

      error ->
        error
    end
  end

  @doc """
  Gets tracked peers from the crsql_tracked_peers table.

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`

  ## Returns
  - `{:ok, peers}` - List of tracked peer information
  - `{:error, reason}` - Query failed or table doesn't exist
  """
  @spec get_tracked_peers(connection()) :: {:ok, [map()]} | {:error, term()}
  def get_tracked_peers(connection) do
    query = "SELECT * FROM crsql_tracked_peers"
    Client.execute_query(connection, query)
  end

  @doc """
  Gets basic database schema information.

  Queries system tables to understand the database structure.

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`

  ## Returns
  Map containing database information including table list and counts.

  ## Examples
      {:ok, info} = CorroClient.Cluster.get_database_info(conn)
      info["Tables"]
      # => {:ok, [%{"name" => "users"}, %{"name" => "__corro_members"}]}
  """
  @spec get_database_info(connection()) :: {:ok, map()} | {:error, term()}
  def get_database_info(connection) do
    # Use only SELECT queries that are guaranteed to be read-only
    queries = [
      {"Tables", "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"},
      {"Table Count", "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table'"}
    ]

    result =
      Enum.reduce(queries, %{}, fn {key, query}, acc ->
        result = Client.execute_query(connection, query)
        Map.put(acc, key, result)
      end)

    {:ok, result}
  end

  # Private functions

  defp fetch_members_with_activity(cluster_data, connection) do
    case get_members(connection) do
      {:ok, members} ->
        # Count active members (not "Down" state)
        active_members =
          Enum.filter(members, fn member ->
            member_state = Map.get(member, "member_state", "Unknown")
            member_state != "Down"
          end)

        Logger.debug("Found #{length(members)} total members, #{length(active_members)} active")

        Map.merge(cluster_data, %{
          members: members,
          member_count: length(members),
          active_member_count: length(active_members)
        })

      {:error, reason} ->
        Logger.debug("Could not fetch cluster members: #{inspect(reason)}")
        cluster_data
    end
  end

  defp fetch_tracked_peers(cluster_data, connection) do
    case get_tracked_peers(connection) do
      {:ok, peers} ->
        Map.merge(cluster_data, %{tracked_peers: peers, peer_count: length(peers)})

      {:error, _} ->
        Logger.debug("Could not fetch tracked peers (table may not exist)")
        cluster_data
    end
  end

  defp calculate_total_active_nodes(cluster_data) do
    # __corro_members table doesn't include local node; if we were able to read the table,
    # the local node is active so add it to the count
    total_active = Map.get(cluster_data, :active_member_count, 0) + 1

    Map.put(cluster_data, :total_active_nodes, total_active)
  end

  @doc """
  Parses a member row from __corro_members table.

  Extracts human-readable information from the foca_state JSON column
  and adds parsed fields to the member data.

  ## Parameters
  - `member_row`: Raw member row from __corro_members table

  ## Returns
  Enhanced member map with parsed foca_state information

  ## Examples
      row = %{"foca_state" => ~s({"state": "Alive", "id": {"addr": "127.0.0.1:8787"}})}
      CorroClient.Cluster.parse_member_foca_state(row)
      # => %{"member_state" => "Alive", "member_addr" => "127.0.0.1:8787", ...}
  """
  @spec parse_member_foca_state(map()) :: map()
  def parse_member_foca_state(member_row) do
    case Map.get(member_row, "foca_state") do
      foca_state when is_binary(foca_state) ->
        case Jason.decode(foca_state) do
          {:ok, parsed} ->
            member_row
            |> Map.put("parsed_foca_state", parsed)
            |> Map.put("member_id", get_in(parsed, ["id", "id"]))
            |> Map.put("member_addr", get_in(parsed, ["id", "addr"]))
            |> Map.put("member_ts", get_in(parsed, ["id", "ts"]))
            |> Map.put("member_cluster_id", get_in(parsed, ["id", "cluster_id"]))
            |> Map.put("member_incarnation", Map.get(parsed, "incarnation"))
            |> Map.put("member_state", Map.get(parsed, "state"))

          {:error, _} ->
            Map.put(member_row, "parse_error", "Invalid JSON in foca_state")
        end

      _ ->
        Map.put(member_row, "parse_error", "Missing or invalid foca_state")
    end
  end
end
