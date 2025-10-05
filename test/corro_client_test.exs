defmodule CorroClientTest do
  use ExUnit.Case
  doctest CorroClient

  alias CorroClient.Client

  describe "connection management" do
    test "connect/2 creates a valid connection structure" do
      conn = CorroClient.connect("http://localhost:8081")

      assert %{base_url: "http://localhost:8081", req_options: options} = conn
      assert Keyword.has_key?(options, :receive_timeout)
      assert Keyword.has_key?(options, :headers)
    end

    test "connect/2 accepts custom options" do
      conn =
        CorroClient.connect("http://localhost:8081",
          receive_timeout: 10_000,
          finch_options: [pool_timeout: 5000]
        )

      assert conn.req_options[:receive_timeout] == 10_000
      assert conn.req_options[:finch_options] == [pool_timeout: 5000]
    end

    test "connect/2 strips trailing slash from base_url" do
      conn = CorroClient.connect("http://localhost:8081/")
      assert conn.base_url == "http://localhost:8081"
    end
  end

  describe "JSONL parsing" do
    test "parse_query_response/1 parses valid JSONL response" do
      response = """
      {"columns": ["id", "name", "age"]}
      {"row": [1, [1, "Alice", 25]]}
      {"row": [2, [2, "Bob", 30]]}
      {"eoq": {"time": 4.2e-8}}
      """

      result = Client.parse_query_response(response)

      assert result == [
               %{"id" => 1, "name" => "Alice", "age" => 25},
               %{"id" => 2, "name" => "Bob", "age" => 30}
             ]
    end

    test "parse_query_response/1 handles empty response" do
      assert Client.parse_query_response("") == []
      assert Client.parse_query_response([]) == []
      assert Client.parse_query_response(nil) == []
    end

    test "parse_query_response/1 handles malformed JSON gracefully" do
      response = """
      {"columns": ["id", "name"]}
      invalid json line
      {"row": [1, [1, "Alice"]]}
      {"eoq": true}
      """

      result = Client.parse_query_response(response)
      assert result == [%{"id" => 1, "name" => "Alice"}]
    end

    test "parse_query_response/1 handles rows without columns" do
      response = """
      {"row": [1, [1, "Alice"]]}
      {"columns": ["id", "name"]}
      {"row": [2, [2, "Bob"]]}
      {"eoq": true}
      """

      result = Client.parse_query_response(response)
      assert result == [%{"id" => 2, "name" => "Bob"}]
    end

    test "parse_query_response/1 returns already parsed lists unchanged" do
      data = [%{"id" => 1, "name" => "Alice"}]
      assert Client.parse_query_response(data) == data
    end
  end

  describe "delegated functions" do
    test "main module delegates to correct submodules" do
      # Test that the main module has the expected delegated functions
      functions = CorroClient.__info__(:functions)

      # Connection functions
      assert Keyword.has_key?(functions, :connect)
      assert Keyword.has_key?(functions, :ping)

      # Query functions
      assert Keyword.has_key?(functions, :query)
      assert Keyword.has_key?(functions, :transaction)

      # Cluster functions
      assert Keyword.has_key?(functions, :get_cluster_members)
      assert Keyword.has_key?(functions, :get_cluster_info)
      assert Keyword.has_key?(functions, :get_tracked_peers)

      # Subscription functions
      assert Keyword.has_key?(functions, :subscribe)
      assert Keyword.has_key?(functions, :subscription_status)
      assert Keyword.has_key?(functions, :restart_subscription)
      assert Keyword.has_key?(functions, :stop_subscription)
    end
  end

  describe "query payload formatting" do
    test "returns raw query when params are empty" do
      assert Client.build_query_payload("SELECT 1", []) == "SELECT 1"
      assert Client.build_query_payload("SELECT 1", nil) == "SELECT 1"
    end

    test "wraps positional parameters in WithParams format" do
      payload = Client.build_query_payload("SELECT * FROM t WHERE id = ?", [123])

      assert payload == ["SELECT * FROM t WHERE id = ?", [123]]
      assert Jason.encode!(payload)
    end

    test "converts keyword lists into named parameter map" do
      payload = Client.build_query_payload("SELECT * FROM t WHERE id = :id", [id: 123, status: "active"])

      assert payload == [
               "SELECT * FROM t WHERE id = :id",
               %{"id" => 123, "status" => "active"}
             ]

      assert Jason.encode!(payload)
    end

    test "converts map parameters and normalises keys" do
      payload = Client.build_query_payload("SELECT * FROM t WHERE id = :id", %{"status" => "active", id: 1})

      assert payload == [
               "SELECT * FROM t WHERE id = :id",
               %{"id" => 1, "status" => "active"}
             ]

      assert Jason.encode!(payload)
    end
  end

  describe "transaction payload formatting" do
    test "leaves plain strings unchanged" do
      assert Client.build_statement_payload("INSERT INTO t VALUES (1)") == "INSERT INTO t VALUES (1)"
    end

    test "converts tuple statements using WithParams format" do
      payload = Client.build_statement_payload({"INSERT INTO t VALUES (?)", ["a"]})

      assert payload == ["INSERT INTO t VALUES (?)", ["a"]]
      assert Jason.encode!(payload)
    end

    test "normalises keyword named params" do
      payload =
        Client.build_statement_payload({"INSERT INTO t VALUES (:id)", [id: 1, status: "active"]})

      assert payload == [
               "INSERT INTO t VALUES (:id)",
               %{"id" => 1, "status" => "active"}
             ]

      assert Jason.encode!(payload)
    end

    test "normalises verbose maps" do
      payload =
        Client.build_statement_payload(%{
          query: "INSERT INTO t VALUES (:id)",
          params: [1],
          named_params: [id: 1]
        })

      assert payload == %{
               "query" => "INSERT INTO t VALUES (:id)",
               "params" => [1],
               "named_params" => %{"id" => 1}
             }

      assert Jason.encode!(payload)
    end
  end
end
