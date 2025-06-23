defmodule CorroClient.Subscriber do
  @moduledoc """
  Manages streaming subscriptions to Corrosion database changes.

  This GenServer manages a long-running HTTP streaming connection to Corrosion's
  subscription endpoint (`/v1/subscriptions`) and delivers events via configurable
  callback functions.

  Supports robust reconnection logic with exponential backoff and comprehensive
  error handling for production use.
  """

  use GenServer
  require Logger

  @type connection :: CorroClient.Client.connection()
  @type event_callback :: (term() -> any())
  @type options :: [
          query: String.t(),
          on_event: event_callback(),
          on_connect: (String.t() -> any()) | nil,
          on_error: (term() -> any()) | nil,
          on_disconnect: (term() -> any()) | nil,
          max_reconnect_attempts: pos_integer(),
          reconnect_delays: [pos_integer()]
        ]

  defstruct [
    :connection,
    :query,
    :on_event,
    :on_connect,
    :on_error,
    :on_disconnect,
    :stream_pid,
    :watch_id,
    :columns,
    max_reconnect_attempts: 5,
    reconnect_delays: [2_000, 5_000, 10_000, 15_000, 30_000],
    attempts_left: 5,
    status: :initializing,
    initial_state_received: false
  ]

  @type t :: %__MODULE__{}

  @doc """
  Start a subscription to a Corrosion table or query.

  ## Parameters
  - `connection`: Connection created with `CorroClient.Client.connect/2`
  - `options`: Subscription configuration

  ## Options
  - `:query` - SQL query to subscribe to (required)
  - `:on_event` - Callback function for data events (required)
  - `:on_connect` - Callback when subscription connects (optional)
  - `:on_error` - Callback for errors (optional)
  - `:on_disconnect` - Callback when subscription disconnects (optional)
  - `:max_reconnect_attempts` - Maximum reconnection attempts (default: 5)
  - `:reconnect_delays` - Delays between reconnection attempts in ms (default: exponential backoff)

  ## Returns
  - `{:ok, pid}` - Subscription process started successfully
  - `{:error, reason}` - Failed to start subscription

  ## Examples
      {:ok, pid} = CorroClient.Subscriber.start_subscription(conn,
        query: "SELECT * FROM messages ORDER BY timestamp DESC",
        on_event: fn event -> IO.inspect(event) end,
        on_connect: fn watch_id -> IO.puts("Connected: \#{watch_id}") end
      )
  """
  @spec start_subscription(connection(), options()) :: {:ok, pid()} | {:error, term()}
  def start_subscription(connection, options) do
    unless Keyword.has_key?(options, :query) do
      raise ArgumentError, "query option is required"
    end

    unless Keyword.has_key?(options, :on_event) do
      raise ArgumentError, "on_event callback is required"
    end

    initial_state = %__MODULE__{
      connection: connection,
      query: Keyword.get(options, :query),
      on_event: Keyword.get(options, :on_event),
      on_connect: Keyword.get(options, :on_connect),
      on_error: Keyword.get(options, :on_error),
      on_disconnect: Keyword.get(options, :on_disconnect),
      max_reconnect_attempts: Keyword.get(options, :max_reconnect_attempts, 5),
      reconnect_delays: Keyword.get(options, :reconnect_delays, [2_000, 5_000, 10_000, 15_000, 30_000])
    }

    initial_state = %{initial_state | attempts_left: initial_state.max_reconnect_attempts}

    GenServer.start_link(__MODULE__, initial_state)
  end

  @doc """
  Get the current status of a subscription.

  ## Parameters
  - `pid`: Subscription process pid

  ## Returns
  Map containing status information
  """
  @spec get_status(pid()) :: map()
  def get_status(pid) do
    GenServer.call(pid, :status, 1000)
  rescue
    _ -> %{status: :unknown, connected: false}
  end

  @doc """
  Manually restart a subscription.

  ## Parameters
  - `pid`: Subscription process pid

  ## Returns
  `:ok` - Restart initiated
  """
  @spec restart(pid()) :: :ok
  def restart(pid) do
    GenServer.call(pid, :restart)
  end

  @doc """
  Stop a subscription.

  ## Parameters
  - `pid`: Subscription process pid

  ## Returns
  `:ok` - Subscription stopped
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  # GenServer callbacks

  @impl true
  def init(state) do
    Process.send_after(self(), :start_subscription, 1000)
    {:ok, state}
  end

  @impl true
  def handle_info(:start_subscription, state) do
    # Kill any existing stream process
    if state.stream_pid do
      Process.exit(state.stream_pid, :kill)
    end

    # Start the streaming connection in a separate process
    parent_pid = self()
    stream_pid = spawn_link(fn -> run_subscription_stream(parent_pid, state) end)

    new_state = %{
      state
      | stream_pid: stream_pid,
        status: :connecting,
        attempts_left: state.max_reconnect_attempts,
        columns: nil,
        initial_state_received: false,
        watch_id: nil
    }

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:reconnect, state) do
    if state.attempts_left > 0 do
      Logger.warning(
        "CorroClient.Subscriber: Attempting to reconnect (#{state.attempts_left} attempts remain)"
      )

      send(self(), :start_subscription)
      {:noreply, state}
    else
      Logger.error("CorroClient.Subscriber: Max reconnection attempts reached, giving up")
      call_if_present(state.on_error, {:subscription_failed, :max_retries})
      {:noreply, %{state | status: :failed}}
    end
  end

  @impl true
  def handle_info({:subscription_connected, watch_id}, state) do
    Logger.info("CorroClient.Subscriber: Connected with watch_id: #{watch_id}")
    call_if_present(state.on_connect, watch_id)

    {:noreply,
     %{
       state
       | status: :connected,
         attempts_left: state.max_reconnect_attempts,
         watch_id: watch_id
     }}
  end

  @impl true
  def handle_info({:subscription_data, data}, state) do
    updated_state = process_streaming_data(data, state)
    {:noreply, updated_state}
  end

  @impl true
  def handle_info({:subscription_error, error}, state) do
    Logger.warning("CorroClient.Subscriber: Subscription error: #{inspect(error)}")
    call_if_present(state.on_error, error)

    new_state = %{
      state
      | status: :error,
        stream_pid: nil,
        watch_id: nil,
        attempts_left: state.attempts_left - 1
    }

    if new_state.attempts_left > 0 do
      schedule_reconnect(new_state)
    else
      Logger.error("CorroClient.Subscriber: Max reconnection attempts reached, giving up")
      call_if_present(state.on_error, {:subscription_failed, :max_retries})
      {:noreply, %{new_state | status: :failed}}
    end
  end

  @impl true
  def handle_info({:subscription_closed, reason}, state) do
    Logger.warning("CorroClient.Subscriber: Subscription closed: #{inspect(reason)}")
    call_if_present(state.on_disconnect, reason)

    new_state = %{
      state
      | status: :disconnected,
        stream_pid: nil,
        watch_id: nil,
        attempts_left: state.attempts_left - 1
    }

    if new_state.attempts_left > 0 do
      schedule_reconnect(new_state)
    else
      Logger.error("CorroClient.Subscriber: Max reconnection attempts reached, giving up")
      call_if_present(state.on_error, {:subscription_failed, :max_retries})
      {:noreply, %{new_state | status: :failed}}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, %{stream_pid: pid} = state) do
    Logger.warning("CorroClient.Subscriber: Stream process crashed: #{inspect(reason)}")

    new_state = %{
      state
      | status: :error,
        stream_pid: nil,
        watch_id: nil,
        attempts_left: state.attempts_left - 1
    }

    if new_state.attempts_left > 0 do
      schedule_reconnect(new_state)
    else
      Logger.error("CorroClient.Subscriber: Max reconnection attempts reached, giving up")
      call_if_present(state.on_error, {:subscription_failed, :max_retries})
      {:noreply, %{new_state | status: :failed}}
    end
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("CorroClient.Subscriber: Unhandled message: #{inspect(msg)}")
    {:noreply, state}
  end

  @impl true
  def handle_call(:restart, _from, state) do
    Logger.info("CorroClient.Subscriber: Manual restart requested")

    if state.stream_pid do
      Process.exit(state.stream_pid, :kill)
    end

    send(self(), :start_subscription)

    new_state = %{
      state
      | stream_pid: nil,
        status: :restarting,
        attempts_left: state.max_reconnect_attempts,
        watch_id: nil
    }

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      status: state.status,
      connected: state.status == :connected && !is_nil(state.stream_pid),
      attempts_left: state.attempts_left,
      watch_id: state.watch_id,
      initial_state_received: state.initial_state_received
    }

    {:reply, status, state}
  end

  # Private functions

  defp run_subscription_stream(parent_pid, state) do
    url = state.connection.base_url <> "/v1/subscriptions"
    json_query = Jason.encode!(state.query)

    Logger.info("CorroClient.Subscriber: Starting subscription stream to #{url}")

    # Use Finch streaming pattern
    finch_fun = fn request, finch_req, finch_name, finch_opts ->
      finch_acc = fn
        {:status, status}, response ->
          if status != 200 do
            send(parent_pid, {:subscription_error, {:http_status, status}})
          end

          %Req.Response{response | status: status}

        {:headers, headers}, response ->
          case Enum.find(headers, fn {key, _} -> key == "corro-query-id" end) do
            {"corro-query-id", watch_id} ->
              send(parent_pid, {:subscription_connected, watch_id})

            _ ->
              Logger.warning("CorroClient.Subscriber: No corro-query-id found in headers")
          end

          %Req.Response{response | headers: headers}

        {:data, data}, response ->
          send(parent_pid, {:subscription_data, data})
          response
      end

      case Finch.stream(finch_req, finch_name, Req.Response.new(), finch_acc,
             finch_opts ++ [receive_timeout: :infinity]
           ) do
        {:ok, response} ->
          send(parent_pid, {:subscription_closed, :normal})
          {request, response}

        {:error, exception} ->
          send(parent_pid, {:subscription_error, {:finch_error, exception}})
          {request, exception}
      end
    end

    try do
      Req.post!(
        url,
        [
          headers: [{"content-type", "application/json"}],
          body: json_query,
          finch_request: finch_fun
        ] ++ state.connection.req_options
      )
    rescue
      e ->
        Logger.error("CorroClient.Subscriber: Exception in subscription: #{inspect(e)}")
        send(parent_pid, {:subscription_error, {:exception, e}})
    end
  end

  defp process_streaming_data(data, state) do
    lines = String.split(data, "\n", trim: true)

    Enum.reduce(lines, state, fn line, acc_state ->
      trimmed_line = String.trim(line)

      if trimmed_line == "" do
        acc_state
      else
        case Jason.decode(trimmed_line) do
          {:ok, json_data} ->
            json_with_watch_id = Map.put(json_data, "watch_id", state.watch_id)
            handle_message_event(json_with_watch_id, acc_state)

          {:error, reason} ->
            Logger.warning("CorroClient.Subscriber: Failed to decode JSON: #{inspect(reason)}")
            acc_state
        end
      end
    end)
  end

  defp handle_message_event(data, state) do
    case data do
      %{"eoq" => _time} ->
        Logger.info("CorroClient.Subscriber: End of initial query - subscription is now live")
        call_if_present(state.on_event, {:subscription_ready})
        %{state | initial_state_received: true}

      %{"columns" => columns} ->
        Logger.info("CorroClient.Subscriber: Got column names: #{inspect(columns)}")
        call_if_present(state.on_event, {:columns_received, columns})
        %{state | columns: columns}

      %{"row" => [_row_id, values]} when not is_nil(state.columns) ->
        message_map = build_message_map(values, state.columns)

        event_type = if state.initial_state_received, do: :new_row, else: :initial_row
        call_if_present(state.on_event, {event_type, message_map})

        state

      %{"change" => [change_type, _change_id, values, _version]} when not is_nil(state.columns) ->
        message_map = build_message_map(values, state.columns)
        call_if_present(state.on_event, {:change, String.upcase(change_type), message_map})
        state

      %{"row" => _} ->
        Logger.warning("CorroClient.Subscriber: Got row data but no columns stored yet")
        state

      %{"change" => _} ->
        Logger.warning("CorroClient.Subscriber: Got change data but no columns stored yet")
        state

      %{"error" => error_msg} ->
        Logger.error("CorroClient.Subscriber: Subscription error: #{error_msg}")
        call_if_present(state.on_error, {:corrosion_error, error_msg})
        state

      other ->
        Logger.warning("CorroClient.Subscriber: Unhandled message event: #{inspect(other)}")
        state
    end
  end

  defp build_message_map(values, columns) when is_list(values) and is_list(columns) do
    if length(values) == length(columns) do
      Enum.zip(columns, values) |> Enum.into(%{})
    else
      Logger.warning(
        "CorroClient.Subscriber: Mismatch: #{length(values)} values vs #{length(columns)} columns"
      )

      %{}
    end
  end

  defp build_message_map(values, columns) do
    Logger.warning(
      "CorroClient.Subscriber: Unable to build message map from values: #{inspect(values)} and columns: #{inspect(columns)}"
    )

    %{}
  end

  defp call_if_present(nil, _arg), do: :ok
  defp call_if_present(callback, arg) when is_function(callback, 1), do: callback.(arg)

  defp schedule_reconnect(state) do
    # Calculate delay based on how many attempts have been made
    attempts_made = state.max_reconnect_attempts - state.attempts_left
    delay_index = min(attempts_made, length(state.reconnect_delays) - 1)
    delay = Enum.at(state.reconnect_delays, delay_index)

    Logger.warning(
      "CorroClient.Subscriber: Scheduling reconnect in #{delay}ms (#{state.attempts_left} attempts left)"
    )

    Process.send_after(self(), :reconnect, delay)
    {:noreply, %{state | status: :reconnecting}}
  end
end
