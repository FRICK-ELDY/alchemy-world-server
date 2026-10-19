defmodule Network.RoomIndexTest do
  use ExUnit.Case, async: false

  setup do
    if is_nil(Process.whereis(Network.RoomIndex)) do
      start_supervised!(Network.RoomIndex)
    end

    prev = Application.get_env(:network, Network.RoomIndex)

    on_exit(fn ->
      if is_nil(prev) do
        Application.delete_env(:network, Network.RoomIndex)
      else
        Application.put_env(:network, Network.RoomIndex, prev)
      end
    end)

    :ok
  end

  test "ヒット中は探索関数を呼ばない" do
    room_id = unique_room()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fetch = fn ->
      Agent.update(counter, &(&1 + 1))
      {node(), room_id}
    end

    assert {:ok, n} = Network.RoomIndex.resolve(room_id, fetch)
    assert n == node()
    assert {:ok, ^n} = Network.RoomIndex.resolve(room_id, fetch)
    assert Agent.get(counter, & &1) == 1

    Network.RoomIndex.invalidate(room_id)
  end

  test "不在はキャッシュしない" do
    room_id = unique_room()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fetch = fn ->
      Agent.update(counter, &(&1 + 1))
      nil
    end

    assert :miss = Network.RoomIndex.resolve(room_id, fetch)
    assert :miss = Network.RoomIndex.resolve(room_id, fetch)
    assert Agent.get(counter, & &1) == 2
  end

  test "TTL を過ぎると再探索する" do
    Application.put_env(:network, Network.RoomIndex, ttl_ms: 30)
    room_id = unique_room()
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fetch = fn ->
      Agent.update(counter, &(&1 + 1))
      {node(), room_id}
    end

    assert {:ok, _} = Network.RoomIndex.resolve(room_id, fetch)

    assert eventually(fn ->
             Network.RoomIndex.resolve(room_id, fetch)
             Agent.get(counter, & &1) == 2
           end)

    Network.RoomIndex.invalidate(room_id)
  end

  test "forget_node は対象ノードのエントリを捨てる" do
    room_id = unique_room()
    :ok = Network.RoomIndex.put(room_id, node())
    assert {:ok, n} = Network.RoomIndex.lookup(room_id)
    assert n == node()

    :ok = Network.RoomIndex.forget_node(node())
    assert :miss = Network.RoomIndex.lookup(room_id)
  end

  test "接続していないノードのエントリはヒットしない" do
    room_id = unique_room()
    :ok = Network.RoomIndex.put(room_id, :ghost@nohost)
    assert :miss = Network.RoomIndex.lookup(room_id)
  end

  defp unique_room, do: "idx_#{System.unique_integer([:positive])}"

  defp eventually(fun, attempts \\ 50) do
    fun.() or
      if attempts <= 1 do
        false
      else
        Process.sleep(20)
        eventually(fun, attempts - 1)
      end
  end
end
