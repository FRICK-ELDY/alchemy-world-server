defmodule Network.Distributed do
  @moduledoc """
  複数ノード間でのルーム管理。

  libcluster によりクラスタが形成されている場合、ルームを任意のノードに配置し、
  ノード間で broadcast をルーティングする。

  ## 単一ノード時
  `Node.list() == []` の場合は `Network.Local` に委譲し、既存の挙動を維持する。

  ## 複数ノード時
  - `open_room/1`: どのノードにも無いと確認できたときだけローカルで作成する。
    既にあれば `{:error, :already_started}`。ノードが応答しないときは作らず
    `{:error, {:rpc_failed, reason}}` を返す。
  - `broadcast/2`: ルームが配置されているノードを検索し、そのノードの `Network.Local.broadcast/2` を RPC で呼ぶ。
  - `list_rooms/0`: クラスタ全ノードのルームを集約して返す。応答しないノードは省く。

  所在の探索結果は `Network.RoomIndex` に TTL 付きで残す。不在はキャッシュしない。

  `broadcast/2` はヒットを信用し、全ノード RPC を繰り返さない。
  配信先が不在または RPC 失敗のときはエントリを捨て、一度だけ再探索する。
  自ノードの古いエントリも同じである。

  `open_room/1`、`close_room/1`、`connect_rooms/2`、`connected?/2` はヒットを存在の証明にしない。
  キャッシュヒットだけ、そのノードの `list_rooms` で確認する。スキャンで見つけた結果は
  その場の一覧を確認として使い、直後に同じノードへ問い合わせ直さない。
  応答しないことは不在ではない。`close_room/1` と `connect_rooms/2` が一度再試行するのは、
  配置先がルームを持っていなかったときだけである。RPC 失敗はそのエラーを返す。
  """

  @type room_id :: String.t() | atom()
  @type event :: term()

  # ── 公開 API（Network モジュールから委譲される）───────────────────────

  @doc """
  ルームを起動する。

  単一ノードの場合は `Network.Local.open_room/1` に委譲。
  複数ノードの場合は、ルームが未存在ならローカルノードで作成する。
  """
  @spec open_room(room_id()) :: {:ok, pid()} | {:error, term()}
  def open_room(room_id) do
    if clustered?() do
      open_room_clustered(room_id)
    else
      Network.Local.open_room(room_id)
    end
  end

  @doc """
  ルームを停止する。

  ルームが配置されているノードの `Network.Local.close_room/1` を呼ぶ。
  """
  @spec close_room(room_id()) :: :ok | {:error, term()}
  def close_room(room_id) do
    if clustered?() do
      close_room_clustered(room_id)
    else
      Network.Local.close_room(room_id)
    end
  end

  @doc """
  ルームを接続テーブルに登録する。`Network.Local.register_room/1` に委譲。
  """
  @spec register_room(room_id()) :: :ok
  def register_room(room_id) do
    result = Network.Local.register_room(room_id)
    if result == :ok, do: Network.RoomIndex.put(room_id, node())
    result
  end

  @doc """
  ルームの登録を解除する。`Network.Local.unregister_room/1` に委譲。
  """
  @spec unregister_room(room_id()) :: :ok
  def unregister_room(room_id) do
    result = Network.Local.unregister_room(room_id)
    Network.RoomIndex.invalidate(room_id)
    result
  end

  @doc """
  2 つのルームを双方向に接続する。

  分散時は両ルームが同一ノードにある必要がある。異なるノードにある場合は
  `{:error, :rooms_on_different_nodes}` を返す。
  """
  @spec connect_rooms(room_id(), room_id()) :: :ok | {:error, term()}
  def connect_rooms(room_a, room_b) do
    if clustered?() do
      connect_rooms_clustered(room_a, room_b)
    else
      Network.Local.connect_rooms(room_a, room_b)
    end
  end

  @doc """
  接続を解除する。`Network.Local.disconnect_rooms/2` に委譲。
  """
  @spec disconnect_rooms(room_id(), room_id()) :: :ok
  def disconnect_rooms(room_a, room_b) do
    Network.Local.disconnect_rooms(room_a, room_b)
  end

  @doc """
  指定ルームとその接続先にイベントをブロードキャストする。

  分散時はルームが配置されているノードの `Network.Local.broadcast/2` を RPC で呼ぶ。
  """
  @spec broadcast(room_id(), event()) :: :ok | {:error, :room_not_found}
  def broadcast(room_id, event) do
    if clustered?() do
      broadcast_clustered(room_id, event)
    else
      Network.Local.broadcast(room_id, event)
    end
  end

  @doc """
  クラスタ全体で起動中のルーム一覧を返す。

  単一ノードの場合は `Network.Local.list_rooms/0` に委譲。
  """
  @spec list_rooms() :: [room_id()]
  def list_rooms do
    if clustered?() do
      list_rooms_clustered()
    else
      Network.Local.list_rooms()
    end
  end

  @doc """
  2 つのルームが接続されているか。`Network.Local.connected?/2` に委譲。

  分散時は両ルームが同一ノードにある場合のみ判定可能。
  異なるノードにある場合は `false` を返す。
  """
  @spec connected?(room_id(), room_id()) :: boolean()
  def connected?(room_a, room_b) do
    if clustered?() do
      connected_clustered?(room_a, room_b)
    else
      Network.Local.connected?(room_a, room_b)
    end
  end

  # ── クラスタ検知 ────────────────────────────────────────────────────

  defp clustered? do
    Node.list() != []
  end

  defp cluster_nodes do
    [node() | Node.list()]
  end

  # ── 分散時の実装 ─────────────────────────────────────────────────────

  defp open_room_clustered(room_id) do
    case find_room_node(room_id) do
      {:error, _} = err ->
        err

      :absent ->
        case Network.Local.open_room(room_id) do
          {:ok, _pid} = ok ->
            Network.RoomIndex.put(room_id, node())
            ok

          other ->
            other
        end

      {_node, _} ->
        {:error, :already_started}
    end
  end

  defp close_room_clustered(room_id, retried? \\ false) do
    case find_room_node(room_id) do
      :absent ->
        {:error, :not_found}

      {:error, _} = err ->
        err

      {target_node, ^room_id} ->
        case close_on_node(target_node, room_id) do
          :ok ->
            Network.RoomIndex.invalidate(room_id)
            :ok

          {:error, :not_found} when not retried? ->
            Network.RoomIndex.invalidate(room_id)
            close_room_clustered(room_id, true)

          other ->
            other
        end
    end
  end

  defp close_on_node(target_node, room_id) when target_node == node() do
    Network.Local.close_room(room_id)
  end

  defp close_on_node(target_node, room_id) do
    case :rpc.call(target_node, Network.Local, :close_room, [room_id]) do
      :ok -> :ok
      {:error, _} = err -> err
      {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
    end
  end

  defp connect_rooms_clustered(room_a, room_b, retried? \\ false) do
    case {find_room_node(room_a), find_room_node(room_b)} do
      {{:error, _} = err, _} ->
        err

      {_, {:error, _} = err} ->
        err

      {{node_a, _}, {node_b, _}} when node_a != node_b ->
        {:error, :rooms_on_different_nodes}

      {:absent, _} ->
        {:error, {:room_not_found, room_a}}

      {_, :absent} ->
        {:error, {:room_not_found, room_b}}

      {{target_node, _}, {target_node, _}} ->
        case rpc_local_or_remote(target_node, Network.Local, :connect_rooms, [room_a, room_b]) do
          :ok ->
            :ok

          {:error, {:room_not_found, _}} when not retried? ->
            Network.RoomIndex.invalidate(room_a)
            Network.RoomIndex.invalidate(room_b)
            connect_rooms_clustered(room_a, room_b, true)

          other ->
            other
        end
    end
  end

  defp broadcast_clustered(room_id, event, retried? \\ false) do
    case lookup_cached_room_node(room_id) do
      {target_node, ^room_id} when target_node == node() ->
        case Network.Local.broadcast(room_id, event) do
          :ok ->
            :ok

          {:error, :room_not_found} ->
            retry_broadcast(room_id, event, retried?)
        end

      {target_node, ^room_id} ->
        case :rpc.call(target_node, Network.Local, :broadcast, [room_id, event]) do
          :ok ->
            :ok

          {:error, :room_not_found} ->
            retry_broadcast(room_id, event, retried?)

          {:badrpc, _} ->
            retry_broadcast(room_id, event, retried?)
        end

      nil ->
        {:error, :room_not_found}
    end
  end

  defp retry_broadcast(room_id, event, retried?) do
    Network.RoomIndex.invalidate(room_id)

    if retried?,
      do: {:error, :room_not_found},
      else: broadcast_clustered(room_id, event, true)
  end

  defp list_rooms_clustered do
    cluster_nodes()
    |> Enum.flat_map(fn n ->
      case fetch_rooms_from_node(n) do
        {:ok, rooms} -> rooms
        {:error, _} -> []
      end
    end)
    |> Enum.uniq()
  end

  defp fetch_rooms_from_node(n) when n == node(), do: {:ok, Network.Local.list_rooms()}

  defp fetch_rooms_from_node(n) do
    case :rpc.call(n, Network.Local, :list_rooms, []) do
      rooms when is_list(rooms) -> {:ok, rooms}
      {:badrpc, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  defp connected_clustered?(room_a, room_b) do
    case {find_room_node(room_a), find_room_node(room_b)} do
      {{node_a, _}, {node_b, _}} when node_a != node_b -> false
      {{target_node, _}, {target_node, _}} -> rpc_connected?(target_node, room_a, room_b)
      _ -> false
    end
  end

  defp rpc_connected?(target_node, room_a, room_b) do
    if target_node == node() do
      Network.Local.connected?(room_a, room_b)
    else
      case :rpc.call(target_node, Network.Local, :connected?, [room_a, room_b]) do
        result when is_boolean(result) -> result
        _ -> false
      end
    end
  end

  # broadcast 用。ヒットは確認せず、失敗した呼び出し側が invalidate する。
  # スキャンで応答できないノードがあっても、ここは不在として配信を諦める。
  defp lookup_cached_room_node(room_id) do
    case Network.RoomIndex.resolve(room_id, fn ->
           case scan_room_node(room_id) do
             {:ok, found} -> found
             _ -> nil
           end
         end) do
      {:ok, node} -> {node, room_id}
      :miss -> nil
    end
  end

  # 制御用。戻りは {node, room_id} | :absent | {:error, {:rpc_failed, reason}}。
  # キャッシュヒットだけ room_present?/2 で確認する。ミスはスキャン結果をそのまま使う。
  defp find_room_node(room_id, retried? \\ false) do
    case Network.RoomIndex.lookup(room_id) do
      {:ok, cached} ->
        confirm_cached_node(room_id, cached, retried?)

      :miss ->
        case scan_room_node(room_id) do
          {:ok, {found, _} = located} ->
            Network.RoomIndex.put(room_id, found)
            located

          :absent ->
            :absent

          {:error, reason} ->
            {:error, {:rpc_failed, reason}}
        end
    end
  end

  defp confirm_cached_node(room_id, cached, retried?) do
    case room_present?(cached, room_id) do
      :yes ->
        {cached, room_id}

      :no ->
        Network.RoomIndex.invalidate(room_id)
        if retried?, do: :absent, else: find_room_node(room_id, true)

      {:error, reason} ->
        Network.RoomIndex.invalidate(room_id)

        if retried? do
          {:error, {:rpc_failed, reason}}
        else
          find_room_node(room_id, true)
        end
    end
  end

  defp room_present?(target_node, room_id) when target_node == node() do
    if room_id in Network.Local.list_rooms(), do: :yes, else: :no
  end

  defp room_present?(target_node, room_id) do
    case fetch_rooms_from_node(target_node) do
      {:ok, rooms} -> if room_id in rooms, do: :yes, else: :no
      {:error, reason} -> {:error, reason}
    end
  end

  # 見つかれば {:ok, {node, room_id}}。全ノードが応答して無ければ :absent。
  # 応答できないノードがあり、応答したノードにも無ければ {:error, reason}。
  defp scan_room_node(room_id) do
    Enum.reduce_while(cluster_nodes(), :absent, fn n, acc ->
      case fetch_rooms_from_node(n) do
        {:ok, rooms} ->
          if room_id in rooms do
            {:halt, {:ok, {n, room_id}}}
          else
            {:cont, acc}
          end

        {:error, reason} ->
          {:cont, remember_scan_error(acc, reason)}
      end
    end)
  end

  defp remember_scan_error(:absent, reason), do: {:error, reason}
  defp remember_scan_error({:error, reason}, _later), do: {:error, reason}

  defp rpc_local_or_remote(target_node, module, fun, args) do
    if target_node == node() do
      apply(module, fun, args)
    else
      case :rpc.call(target_node, module, fun, args) do
        :ok -> :ok
        {:error, _} = err -> err
        {:badrpc, reason} -> {:error, {:rpc_failed, reason}}
      end
    end
  end
end
