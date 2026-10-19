defmodule Network.RoomIndex do
  @moduledoc """
  ルーム所在の探索結果キャッシュ。

  `Network.Distributed` が全ノードへ `:rpc` する回数を、TTL のあいだ抑える。

  ## 保証する

  - TTL 内のヒットは探索関数を呼ばない
  - 不在はキャッシュしない（作られた直後のルームを見逃さない）
  - `Node.list/0` に無いノードのエントリはヒットにしない
  - `:nodedown` と `forget_node/1` でそのノードのエントリを捨てる

  ## 保証しない

  - TTL 内のリモート移動の即時反映。RPC が不在や失敗を返した側が `invalidate/1` する
  """

  use GenServer

  @table __MODULE__
  @default_ttl_ms 5_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  キャッシュを見て、ミスのときだけ `fetch_fun` を呼ぶ。

  `fetch_fun` は `{node, room_id}` または `nil` を返す。
  """
  @spec resolve(term(), (-> {node(), term()} | nil)) :: {:ok, node()} | :miss
  def resolve(room_id, fetch_fun) when is_function(fetch_fun, 0) do
    case lookup(room_id) do
      {:ok, node} ->
        {:ok, node}

      :miss ->
        case fetch_fun.() do
          {node, _} when is_atom(node) ->
            put(room_id, node)
            {:ok, node}

          _ ->
            :miss
        end
    end
  end

  @spec lookup(term()) :: {:ok, node()} | :miss
  def lookup(room_id) do
    if table_ready?() do
      lookup_entry(room_id)
    else
      :miss
    end
  end

  @spec put(term(), node()) :: :ok
  def put(room_id, node) when is_atom(node) do
    if table_ready?() do
      :ets.insert(@table, {room_id, node, now_ms() + ttl_ms()})
    end

    :ok
  end

  @spec invalidate(term()) :: :ok
  def invalidate(room_id) do
    if table_ready?(), do: :ets.delete(@table, room_id)
    :ok
  end

  @spec forget_node(node()) :: :ok
  def forget_node(node) when is_atom(node) do
    if table_ready?(), do: :ets.match_delete(@table, {:_, node, :_})
    :ok
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    # 名前付きノードでない起動では net_kernel が落ちる。所在キャッシュ自体は動く。
    if Node.alive?(), do: :net_kernel.monitor_nodes(true)

    {:ok, %{}}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    forget_node(node)
    {:noreply, state}
  end

  def handle_info({:nodeup, _node}, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  defp lookup_entry(room_id) do
    case :ets.lookup(@table, room_id) do
      [{^room_id, node, expires_at}] ->
        cond do
          now_ms() >= expires_at ->
            :ets.delete(@table, room_id)
            :miss

          node != node() and node not in Node.list() ->
            :ets.delete(@table, room_id)
            :miss

          true ->
            {:ok, node}
        end

      [] ->
        :miss
    end
  end

  defp table_ready? do
    :ets.whereis(@table) != :undefined
  end

  defp ttl_ms do
    Application.get_env(:network, __MODULE__, [])
    |> Keyword.get(:ttl_ms, @default_ttl_ms)
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
