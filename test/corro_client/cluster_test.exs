defmodule CorroClient.ClusterTest do
  use ExUnit.Case
  doctest CorroClient.Cluster

  alias CorroClient.Cluster

  describe "parse_member_foca_state/1" do
    test "parses valid foca_state JSON" do
      member_row = %{
        "actor_id" => [62, 127, 19, 213, 213, 89, 71, 245, 157, 144, 209, 152, 13, 19, 194, 147],
        "address" => "127.0.0.1:8788",
        "foca_state" => ~s({
          "id": {
            "id": "3e7f13d5-d559-47f5-9d90-d1980d13c293",
            "addr": "127.0.0.1:8788",
            "ts": 7517131088795160372,
            "cluster_id": 0
          },
          "incarnation": 0,
          "state": "Alive"
        }),
        "rtt_min" => 0,
        "updated_at" => "2025-06-18 17:48:58.714332+00:00"
      }

      result = Cluster.parse_member_foca_state(member_row)

      assert result["member_id"] == "3e7f13d5-d559-47f5-9d90-d1980d13c293"
      assert result["member_addr"] == "127.0.0.1:8788"
      assert result["member_ts"] == 7517131088795160372
      assert result["member_cluster_id"] == 0
      assert result["member_incarnation"] == 0
      assert result["member_state"] == "Alive"
      assert Map.has_key?(result, "parsed_foca_state")
    end

    test "handles invalid JSON in foca_state" do
      member_row = %{
        "actor_id" => [],
        "foca_state" => "invalid json"
      }

      result = Cluster.parse_member_foca_state(member_row)
      assert result["parse_error"] == "Invalid JSON in foca_state"
    end

    test "handles missing foca_state" do
      member_row = %{"actor_id" => []}

      result = Cluster.parse_member_foca_state(member_row)
      assert result["parse_error"] == "Missing or invalid foca_state"
    end

    test "handles nil foca_state" do
      member_row = %{"foca_state" => nil}

      result = Cluster.parse_member_foca_state(member_row)
      assert result["parse_error"] == "Missing or invalid foca_state"
    end

    test "handles partial foca_state data" do
      member_row = %{
        "foca_state" => ~s({"state": "Down"})
      }

      result = Cluster.parse_member_foca_state(member_row)

      assert result["member_state"] == "Down"
      assert is_nil(result["member_id"])
      assert is_nil(result["member_addr"])
    end
  end
end
