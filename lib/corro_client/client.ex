defmodule CorroClient.Client do
  @moduledoc """
  Low-level HTTP client for interacting with the Corrosion database API.

  This module provides the basic HTTP transport layer for communicating
  with Corrosion's REST API endpoints:
  - `/v1/queries` for read operations
  - `/v1/transactions` for write operations

  Supports arbitrary SQL queries and transactions with configurable
  connection options including Finch pooling settings.
  """
  require Logger

  @type connection :: %{
          base_url: String.t(),
          req_options: keyword()
        }

  @type query_result :: {:ok, [map()]} | {:error, term()}
  @type transaction_result :: {:ok, term()} | {:error, term()}
  @type query_params :: list() | map() | Keyword.t() | nil

  @doc """
  Creates a connection configuration for a Corrosion node.

  ## Parameters
  - `base_url`: The base URL of the Corrosion API (e.g., "http://127.0.0.1:8081")
  - `options`: Additional Req options including Finch configuration

  ## Options
  - `:receive_timeout` - Request timeout in milliseconds (default: 5000)
  - `:finch_options` - Finch-specific options for connection pooling
  - `:connect_options` - Connection-specific options

  ## Examples
      iex> conn = CorroClient.Client.connect("http://localhost:8081")
      %{base_url: "http://localhost:8081", req_options: [...]}

      # With pooling configuration
      iex> conn = CorroClient.Client.connect("http://localhost:8081",
      ...>   finch_options: [pool_timeout: 5000, pool_max_idle_time: 30_000])
  """
  @spec connect(String.t(), keyword()) :: connection()
  def connect(base_url, options \\ []) do
    default_options = [
      receive_timeout: 5000,
      headers: [{"content-type", "application/json"}]
    ]

    req_options = Keyword.merge(default_options, options)

    %{
      base_url: String.trim_trailing(base_url, "/"),
      req_options: req_options
    }
  end

  @doc """
  Execute a SQL query against Corrosion's query endpoint.

  Supports arbitrary SQL queries with optional parameters using the
  same payload formats accepted by Corrosion's `Statement` enum.

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `query`: SQL query string
  - `params`: Optional query parameters (default: [])

  The `params` argument supports the following forms:
  - positional parameters: `[:value, 123, true]` (sends `["SELECT ...", [...]]`)
  - named parameters as a keyword list or map: `[id: 1]` / `%{"id" => 1}`
    (sends `["SELECT ...", {"id": 1}]`)
  - verbose statement map: `%{query: "SELECT ...", params: [...], named_params: %{}}`

  ## Returns
  - `{:ok, results}` - List of maps representing rows
  - `{:error, reason}` - Error details

  ## Examples
      iex> CorroClient.Client.execute_query(conn, "SELECT * FROM users")
      {:ok, [%{"id" => 1, "name" => "Alice"}]}

      iex> CorroClient.Client.execute_query(conn, "SELECT * FROM users WHERE id = ?", [1])
      {:ok, [%{"id" => 1, "name" => "Alice"}]}
  """
  @spec execute_query(connection(), String.t(), query_params()) :: query_result()
  def execute_query(connection, query, params \\ []) do
    query_payload = build_query_payload(query, params)

    with {:ok, body} <- post_request(connection, "/v1/queries", query_payload) do
      {:ok, parse_query_response(body)}
    end
  end

  @doc """
  Execute a list of SQL statements as a transaction.

  Supports arbitrary SQL transactions with rollback on failure. Each
  statement uses the same formats as the `Statement` enum documented by
  Corrosion's API (`"SQL"`, `["SQL", [...]]`, `["SQL", %{...}]`, or
  `%{"query" => ..., "params" => ..., ...}`).

  ## Parameters
  - `connection`: Connection created with `connect/2`
  - `statements`: List of SQL statements (strings, `{statement, params}`, or verbose maps)

  ## Returns
  - `{:ok, response}` - Transaction succeeded
  - `{:error, reason}` - Transaction failed and was rolled back

  ## Examples
      iex> statements = [
      ...>   "INSERT INTO users (name) VALUES ('Alice')",
      ...>   {"UPDATE stats SET count = count + 1", []},
      ...>   {"INSERT INTO audit (user_id) VALUES (?)", [123]},
      ...>   ["UPDATE users SET status = :status WHERE id = :id", %{id: 123, status: "active"}],
      ...>   %{query: "INSERT INTO log(message) VALUES (?)", params: ["Created"]}
      ...> ]
      iex> CorroClient.Client.execute_transaction(conn, statements)
      {:ok, transaction_response}
  """
  @type statement_input ::
          String.t()
          | {String.t(), list() | map() | Keyword.t() | nil}
          | [String.t() | list() | map()]
          | map()

  @spec execute_transaction(connection(), [statement_input()]) :: transaction_result()
  def execute_transaction(connection, statements) when is_list(statements) do
    payload = Enum.map(statements, &build_statement_payload/1)

    post_request(connection, "/v1/transactions", payload)
  end

  @doc """
  Test connectivity to a Corrosion node.

  ## Parameters
  - `connection`: Connection to test

  ## Returns
  - `:ok` if connection successful
  - `{:error, reason}` if connection failed
  """
  @spec ping(connection()) :: :ok | {:error, term()}
  def ping(connection) do
    case execute_query(connection, "SELECT 1") do
      {:ok, _} -> :ok
      error -> error
    end
  end

  # Private functions

  defp post_request(connection, endpoint, payload) do
    url = connection.base_url <> endpoint

    case Req.post(url, [json: payload] ++ connection.req_options) do
      {:ok, %{status: 200, body: body}} ->
        {:ok, body}

      {:ok, %{status: status, body: body}} ->
        Logger.warning("Corrosion request failed with status #{status}: #{inspect(body)}")
        {:error, {:http_error, status, body}}

      {:error, exception} ->
        Logger.warning("Failed to connect to Corrosion API: #{inspect(exception)}")
        {:error, {:connection_error, exception}}
    end
  end

  @doc false
  @spec build_query_payload(String.t(), query_params()) ::
          String.t() | [String.t() | list() | map()]
  def build_query_payload(query, params) do
    cond do
      params in [nil, []] ->
        query

      Keyword.keyword?(params) ->
        [query, build_named_params_map(params)]

      is_map(params) ->
        [query, build_named_params_map(params)]

      true ->
        [query, params]
    end
  end

  @doc false
  @spec build_statement_payload(statement_input()) :: String.t() | list() | map()
  def build_statement_payload(statement) when is_binary(statement), do: statement

  def build_statement_payload({query, params}) when is_binary(query) do
    build_query_payload(query, params)
  end

  def build_statement_payload([query, params]) when is_binary(query) do
    cond do
      Keyword.keyword?(params) -> [query, build_named_params_map(params)]
      is_map(params) -> [query, build_named_params_map(params)]
      true -> [query, params]
    end
  end

  def build_statement_payload(%{} = verbose) do
    verbose
    |> Enum.reduce(%{}, fn
      {:query, value}, acc when is_binary(value) -> Map.put(acc, "query", value)
      {"query", value}, acc when is_binary(value) -> Map.put(acc, "query", value)
      {:params, value}, acc -> Map.put(acc, "params", normalize_params(value))
      {"params", value}, acc -> Map.put(acc, "params", normalize_params(value))
      {:named_params, value}, acc -> Map.put(acc, "named_params", build_named_params_map(value))
      {"named_params", value}, acc -> Map.put(acc, "named_params", build_named_params_map(value))
      {key, value}, acc -> Map.put(acc, normalize_param_key(key), value)
    end)
  end

  def build_statement_payload(statement), do: statement

  defp build_named_params_map(params) when is_map(params) do
    params
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      Map.put(acc, normalize_param_key(key), value)
    end)
  end

  defp build_named_params_map(params) do
    params
    |> Enum.reduce(%{}, fn
      {key, value}, acc -> Map.put(acc, normalize_param_key(key), value)
      key, acc when is_atom(key) or is_binary(key) -> Map.put(acc, normalize_param_key(key), nil)
    end)
  end

  defp normalize_param_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_param_key(key) when is_binary(key), do: key
  defp normalize_param_key(key), do: to_string(key)

  defp normalize_params(params) do
    cond do
      params in [nil, []] -> params
      Keyword.keyword?(params) -> build_named_params_map(params)
      is_map(params) -> build_named_params_map(params)
      true -> params
    end
  end

  @doc """
  Parse Corrosion's JSONL query response format into a list of maps.

  Corrosion returns responses in JSONL format (one JSON object per line):
  - `{"columns": ["col1", "col2", ...]}` - column definitions
  - `{"row": [row_number, [value1, value2, ...]]}` - data rows
  - `{"eoq": true}` - end of query marker

  ## Parameters
  - `response`: Raw response body from Corrosion API

  ## Returns
  List of maps where each map represents a row with column names as keys

  ## Examples
      iex> response = ~s({"columns": ["id", "name"]}\\n{"row": [1, [1, "Alice"]]}\\n{"eoq": true})
      iex> CorroClient.Client.parse_query_response(response)
      [%{"id" => 1, "name" => "Alice"}]
  """
  @spec parse_query_response(String.t() | list()) :: [map()]
  def parse_query_response(response) when is_binary(response) do
    lines = String.split(response, "\n")

    {_columns, rows} =
      Enum.reduce(lines, {nil, []}, fn line, {cols, rows_acc} ->
        case String.trim(line) do
          "" ->
            {cols, rows_acc}

          json_line ->
            case Jason.decode(json_line) do
              {:ok, %{"columns" => columns}} ->
                {columns, rows_acc}

              {:ok, %{"row" => [_row_num, values]}} when not is_nil(cols) ->
                row_map = Enum.zip(cols, values) |> Enum.into(%{})
                {cols, [row_map | rows_acc]}

              {:ok, %{"eoq" => _}} ->
                {cols, rows_acc}

              _ ->
                {cols, rows_acc}
            end
        end
      end)

    Enum.reverse(rows)
  end

  def parse_query_response(response) when is_list(response) do
    # Already parsed
    response
  end

  def parse_query_response(_), do: []
end
