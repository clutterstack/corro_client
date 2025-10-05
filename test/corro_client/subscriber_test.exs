defmodule CorroClient.SubscriberTest do
  use ExUnit.Case
  doctest CorroClient.Subscriber

  alias CorroClient.Subscriber

  describe "start_subscription/2" do
    test "requires query option" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      assert_raise ArgumentError, "query option is required", fn ->
        Subscriber.start_subscription(conn, on_event: fn _ -> :ok end)
      end
    end

    test "requires on_event callback" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      assert_raise ArgumentError, "on_event callback is required", fn ->
        Subscriber.start_subscription(conn, query: "SELECT 1")
      end
    end

    test "starts subscription with valid options" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      # This test would require a running Corrosion instance, so we just test
      # that the GenServer starts without error
      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: "SELECT 1",
          on_event: fn _ -> :ok end
        )

      assert is_pid(pid)
      assert Process.alive?(pid)

      # Clean up
      GenServer.stop(pid)
    end

    test "normalises tuple queries into statement payload" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: {"SELECT * FROM items WHERE id = ?", [123]},
          on_event: fn _ -> :ok end
        )

      state = :sys.get_state(pid)
      assert state.statement == ["SELECT * FROM items WHERE id = ?", [123]]

      GenServer.stop(pid)
    end

    test "normalises verbose map queries" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: %{query: "SELECT * FROM logs WHERE level = :level", named_params: [level: "info"]},
          on_event: fn _ -> :ok end
        )

      state = :sys.get_state(pid)

      assert state.statement == %{
               "query" => "SELECT * FROM logs WHERE level = :level",
               "named_params" => %{"level" => "info"}
             }

      GenServer.stop(pid)
    end
  end

  describe "callback handling" do
    # Note: These are integration-style tests that would need a real Corrosion instance
    # In a real test suite, you'd use mocks or test doubles

    test "accepts all callback options" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      event_log = Agent.start_link(fn -> [] end)
      {:ok, agent} = event_log

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: "SELECT 1",
          on_event: fn event -> Agent.update(agent, &[{:event, event} | &1]) end,
          on_connect: fn watch_id -> Agent.update(agent, &[{:connect, watch_id} | &1]) end,
          on_error: fn error -> Agent.update(agent, &[{:error, error} | &1]) end,
          on_disconnect: fn reason -> Agent.update(agent, &[{:disconnect, reason} | &1]) end,
          max_reconnect_attempts: 3,
          reconnect_delays: [1000, 2000, 5000]
        )

      assert is_pid(pid)

      # Clean up
      GenServer.stop(pid)
      Agent.stop(agent)
    end
  end

  describe "status and control" do
    test "get_status returns status map" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: "SELECT 1",
          on_event: fn _ -> :ok end
        )

      status = Subscriber.get_status(pid)

      assert is_map(status)
      assert Map.has_key?(status, :status)
      assert Map.has_key?(status, :connected)
      assert Map.has_key?(status, :attempts_left)

      GenServer.stop(pid)
    end

    test "restart returns :ok" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: "SELECT 1",
          on_event: fn _ -> :ok end
        )

      assert Subscriber.restart(pid) == :ok

      GenServer.stop(pid)
    end

    test "stop terminates the process" do
      conn = %{base_url: "http://localhost:8081", req_options: []}

      {:ok, pid} =
        Subscriber.start_subscription(conn,
          query: "SELECT 1",
          on_event: fn _ -> :ok end
        )

      assert Process.alive?(pid)
      assert Subscriber.stop(pid) == :ok

      # Give it a moment to stop
      Process.sleep(10)
      refute Process.alive?(pid)
    end
  end
end
