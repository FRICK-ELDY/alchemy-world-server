defmodule Network.UDP do
  @moduledoc """
  `:gen_udp` による UDP トランスポートサーバー。

  クライアント（別 OS プロセス・別ノード）が UDP で接続し、
  ゲーム入力を送信してフレームイベントを受信できる。

  ## 接続フロー

      クライアント                   Network.UDP
          |                               |
          |--- JOIN(seq=1, "room_a") ---->|  register_room + セッション登録
          |<-- JOIN_ACK(seq=1, "room_a") -|
          |                               |
          |--- INPUT(seq=2, dx, dy) ----->|  GameEvents に :move_input を送信
          |<-- FRAME(seq=N, events) ------|  GameEvents からの :frame_events を転送
          |                               |
          |--- LEAVE(seq=M, "room_a") --->|  セッション削除
          |                               |

  ## セッション管理

  クライアントは `{ip, port}` で識別される。
  同一アドレスからの JOIN で既存セッションは上書きされる。

  各セッションは最終受信時刻（`last_seen_ms`、単調時計）を持ち、
  JOIN / INPUT / ACTION / PING で更新される。`session_timeout_ms` を超えて
  無通信のエントリは定期スイープで除去する（切断検知・テーブル肥大防止）。
  アイドル中のクライアントは PING をハートビートとして送る想定。

  `AUTH_REQUIRED=true` のとき JOIN に RoomToken（`room_id <<0>> token`）が必須。
  オフ時は従来どおり room_id のみで参加できる（デモ・ローカル互換）。

  ## パケット形式と契約

  形式と、到達・順序・リプレイ・断片化を保証しないことは `Network.UDP.Protocol` を参照。
  このサーバーが追加で保証するものは入口の上限だけである。

  - 受信データグラムは `max_packet_bytes` を超えたら捨てる（分割しない）
  - 新規 JOIN は `max_sessions` で打ち切る（既存クライアントの再 JOIN は許す）
  - `{ip, port}` ごとにトークンバケット（`rate_capacity` / `rate_per_sec`）
  - レート表は `max_sessions * 2` まで。上限に達した未知の送信元は記録しない
  - ソケットは `active: N`。`N` 通ごとに `{active, N}` を張り直す

  ## 設定

      config :network, Network.UDP,
        port: 4001,                 # デフォルト: 4001
        session_timeout_ms: 30_000, # 無通信でセッション除去
        sweep_interval_ms: 5_000,   # 淘汰スイープ間隔
        max_packet_bytes: 8_192,
        max_sessions: 1_024,
        rate_capacity: 30,
        rate_per_sec: 60,
        active_n: 64

  実行時にポートを変更する場合は `config/runtime.exs` で `NETWORK_UDP_PORT` を設定する。

  ## 受け入れ基準（フェーズ3）

  - 異なる OS プロセスから同一ルームに 2 プレイヤーが参加できる
  - ルームのクラッシュが他のルームに影響しない
  - localhost でのフレームイベント配信レイテンシ < 5ms
  """

  use GenServer
  require Logger

  alias Alchemy.Render.RenderFrame
  alias Network.UDP.Protocol

  @default_port 4001
  # 無通信クライアントの除去閾値（アーカイブ改善計画の 30 秒）
  @default_session_timeout_ms 30_000
  @default_sweep_interval_ms 5_000
  @default_max_packet_bytes 8_192
  @default_max_sessions 1_024
  @default_rate_capacity 30
  @default_rate_per_sec 60
  @default_active_n 64
  # 1 データグラムを切り詰めない。超過は受信後に max_packet_bytes で捨てる。
  @udp_datagram_buffer 65_535
  # カーネルの受信キュー。1 パケット長に合わせるとバーストを黙って捨てる。
  @udp_recv_buffer 1_048_576

  @type client_key :: {:inet.ip_address(), :inet.port_number()}
  @type session :: %{room_id: String.t(), last_seen_ms: integer()}

  # ── 公開 API ────────────────────────────────────────────────────────

  @doc """
  UDP サーバーを起動する。

  ポート番号は `config :network, Network.UDP, port: N` から読み取る。
  `opts` に `port:` を渡すことで上書きできる（テスト用途など）。
  """
  def start_link(opts \\ []) do
    port =
      Keyword.get_lazy(opts, :port, fn ->
        Application.get_env(:network, __MODULE__, [])
        |> Keyword.get(:port, @default_port)
      end)

    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @doc """
  UDP サーバーが使用しているポート番号を返す。
  """
  @spec port() :: non_neg_integer()
  def port do
    GenServer.call(__MODULE__, :port)
  end

  @doc """
  現在接続中のクライアント一覧を返す。
  各エントリは `{{ip, port}, session}` のタプル。
  """
  @spec sessions() :: [{client_key(), session()}]
  def sessions do
    GenServer.call(__MODULE__, :sessions)
  end

  @doc """
  レート表のエントリ数を返す。上限は `max_sessions * 2`。
  """
  @spec rate_entry_count() :: non_neg_integer()
  def rate_entry_count do
    GenServer.call(__MODULE__, :rate_entry_count)
  end

  @doc """
  指定ルームに接続している全クライアントにフレームイベントを送信する。
  `GameEvents` から `{:frame_events, events}` を受け取ったときに呼ばれる想定。
  """
  @spec broadcast_frame(String.t(), binary() | RenderFrame.t()) :: :ok
  def broadcast_frame(room_id, %RenderFrame{} = render_frame) do
    broadcast_frame(room_id, RenderFrame.encode(render_frame))
  end

  def broadcast_frame(room_id, frame_payload) when is_binary(frame_payload) do
    GenServer.cast(__MODULE__, {:broadcast_frame, room_id, frame_payload})
  end

  # ── GenServer コールバック ───────────────────────────────────────────

  @impl true
  def init(port) do
    # reuseaddr: true — テスト時のポート再利用（クラッシュ後の即時再起動）を可能にする。
    # UDP には TCP の TIME_WAIT がないため本番環境でも副作用はない。
    # active: N — 一度に受け取るデータグラム数を限り、処理が追いつくまでカーネルに留める。
    # buffer はユーザ空間の 1 データグラム上限。max_packet_bytes にすると超過分が
    # 切り詰められ、短い正当パケットとして decode され得る。
    # recbuf は SO_RCVBUF。データグラム上限ではない。
    active_n = active_n()

    case :gen_udp.open(port, [
           :binary,
           reuseaddr: true,
           active: active_n,
           buffer: @udp_datagram_buffer,
           recbuf: @udp_recv_buffer
         ]) do
      {:ok, socket} ->
        actual_port =
          case port do
            0 ->
              case :inet.port(socket) do
                {:ok, p} ->
                  p

                {:error, reason} ->
                  Logger.warning(
                    "[Network.UDP] :inet.port/1 failed after bind on port 0: #{inspect(reason)}; " <>
                      "state.port may be incorrect"
                  )

                  port
              end

            _ ->
              port
          end

        Logger.info("[Network.UDP] Listening on UDP port #{actual_port}")
        schedule_sweep()

        {:ok,
         %{
           socket: socket,
           port: actual_port,
           sessions: %{},
           rates: %{},
           next_seq: 0,
           active_n: active_n
         }}

      {:error, reason} ->
        {:stop, {:udp_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.port, state}
  end

  def handle_call(:sessions, _from, state) do
    {:reply, Map.to_list(state.sessions), state}
  end

  def handle_call(:rate_entry_count, _from, state) do
    {:reply, map_size(state.rates), state}
  end

  @impl true
  def handle_cast({:broadcast_frame, room_id, frame_payload}, state) do
    {seq, new_state} = next_seq(state)

    case Protocol.encode({:frame, seq, frame_payload}) do
      {:ok, packet} ->
        if byte_size(packet) > max_packet_bytes() do
          Logger.error(
            "[Network.UDP] Frame for room=#{room_id} exceeds max_packet_bytes " <>
              "(#{byte_size(packet)}). Fragmentation is not provided"
          )
        else
          state.sessions
          |> Enum.filter(fn {_key, session} -> session.room_id == room_id end)
          |> Enum.each(fn {{ip, port}, _session} ->
            :gen_udp.send(new_state.socket, ip, port, packet)
          end)
        end

      {:error, reason} ->
        Logger.error(
          "[Network.UDP] Failed to encode frame for room=#{room_id}: #{inspect(reason)}"
        )
    end

    {:noreply, new_state}
  end

  # ── UDP パケット受信 ─────────────────────────────────────────────────

  @impl true
  def handle_info({:udp, _socket, ip, port, data}, state) do
    client = {ip, port}
    {decision, state} = take_token(state, client)

    new_state =
      cond do
        decision == :limited ->
          state

        byte_size(data) > max_packet_bytes() ->
          Logger.debug(
            "[Network.UDP] Dropping oversized packet bytes=#{byte_size(data)} from #{inspect(client)}"
          )

          state

        true ->
          case Protocol.decode(data) do
            {:ok, packet} ->
              handle_packet(packet, client, state)

            {:error, :invalid_packet} ->
              Logger.warning("[Network.UDP] Invalid packet from #{inspect(client)}")
              state
          end
      end

    {:noreply, new_state}
  end

  def handle_info({:udp_passive, socket}, state) do
    :inet.setopts(socket, active: state.active_n)
    {:noreply, state}
  end

  def handle_info(:sweep_sessions, state) do
    new_state = sweep_stale_sessions(state)
    schedule_sweep()
    {:noreply, new_state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :gen_udp.close(state.socket)
  end

  # ── パケットハンドラ ─────────────────────────────────────────────────

  defp handle_packet({:join, seq, room_id, token}, client, state) do
    case Network.RoomAuth.verify_join_token(token, room_id) do
      :ok ->
        do_join(seq, room_id, client, state)

      {:error, reason} ->
        Logger.warning(
          "[Network.UDP] JOIN rejected room=#{room_id} client=#{inspect(client)} reason=#{inspect(reason)}"
        )

        {:ok, err_packet} = Protocol.encode({:error, seq, unauthorized_reason(reason)})
        {ip, port} = client
        :gen_udp.send(state.socket, ip, port, err_packet)

        state
    end
  end

  defp handle_packet({:leave, _seq, room_id}, client, state) do
    Logger.info("[Network.UDP] Client #{inspect(client)} left room=#{room_id}")
    %{state | sessions: Map.delete(state.sessions, client)}
  end

  defp handle_packet({:input, _seq, dx, dy}, client, state) do
    case Map.get(state.sessions, client) do
      nil ->
        Logger.warning("[Network.UDP] Input from unknown client #{inspect(client)}")
        state

      %{room_id: room_id} ->
        case Core.RoomRegistry.get_loop(room_id) do
          {:ok, pid} ->
            send(pid, {:move_input, dx, dy})

          :error ->
            Logger.warning("[Network.UDP] Room #{room_id} not found for input")
        end

        touch_session(state, client)
    end
  end

  defp handle_packet({:action, _seq, name}, client, state) do
    case Map.get(state.sessions, client) do
      nil ->
        Logger.warning("[Network.UDP] Action from unknown client #{inspect(client)}")
        state

      %{room_id: room_id} ->
        case Core.RoomRegistry.get_loop(room_id) do
          {:ok, pid} ->
            send(pid, {:ui_action, name})

          :error ->
            Logger.warning("[Network.UDP] Room #{room_id} not found for action")
        end

        touch_session(state, client)
    end
  end

  defp handle_packet({:ping, seq}, client, state) do
    ts = System.system_time(:millisecond)
    {:ok, pong} = Protocol.encode({:pong, seq, ts})
    {ip, port} = client
    :gen_udp.send(state.socket, ip, port, pong)

    # 登録済みならハートビートとして last_seen を延長（未登録は touch_session が no-op）
    touch_session(state, client)
  end

  defp handle_packet(packet, client, state) do
    Logger.debug(
      "[Network.UDP] Unhandled packet #{inspect(elem(packet, 0))} from #{inspect(client)}"
    )

    state
  end

  defp do_join(seq, room_id, client, state) do
    if not Map.has_key?(state.sessions, client) and map_size(state.sessions) >= max_sessions() do
      Logger.warning(
        "[Network.UDP] JOIN rejected session limit client=#{inspect(client)} room=#{room_id}"
      )

      {:ok, err_packet} = Protocol.encode({:error, seq, "session_limit"})
      {ip, port} = client
      :gen_udp.send(state.socket, ip, port, err_packet)
      state
    else
      register_join(seq, room_id, client, state)
    end
  end

  defp register_join(seq, room_id, client, state) do
    case Network.Local.register_room(room_id) do
      :ok ->
        session = %{room_id: room_id, last_seen_ms: now_ms()}
        new_sessions = Map.put(state.sessions, client, session)
        Logger.info("[Network.UDP] Client #{inspect(client)} joined room=#{room_id}")

        {:ok, ack} = Protocol.encode({:join_ack, seq, room_id})
        {ip, port} = client
        :gen_udp.send(state.socket, ip, port, ack)

        %{state | sessions: new_sessions}

      {:error, reason} ->
        Logger.warning(
          "[Network.UDP] register_room failed for room=#{room_id}: #{inspect(reason)}"
        )

        {:ok, err_packet} = Protocol.encode({:error, seq, "register_failed"})
        {ip, port} = client
        :gen_udp.send(state.socket, ip, port, err_packet)

        state
    end
  end

  defp unauthorized_reason(:missing), do: "token_required"
  defp unauthorized_reason(:expired), do: "token_expired"
  defp unauthorized_reason(:invalid), do: "invalid_token"
  defp unauthorized_reason(:scope_mismatch), do: "token_scope_mismatch"
  defp unauthorized_reason(_), do: "unauthorized"

  # ── ユーティリティ ───────────────────────────────────────────────────

  # 単調増加するシーケンス番号を発行する。32bit でラップアラウンドする。
  # 受信側はこの値を検証しない。到達・順序・リプレイ防止は保証しない。
  defp next_seq(%{next_seq: seq} = state) do
    new_seq = rem(seq + 1, 0x100000000)
    {seq, %{state | next_seq: new_seq}}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp udp_config do
    Application.get_env(:network, __MODULE__, [])
  end

  defp session_timeout_ms do
    Keyword.get(udp_config(), :session_timeout_ms, @default_session_timeout_ms)
  end

  defp sweep_interval_ms do
    Keyword.get(udp_config(), :sweep_interval_ms, @default_sweep_interval_ms)
  end

  defp max_packet_bytes do
    Keyword.get(udp_config(), :max_packet_bytes, @default_max_packet_bytes)
  end

  defp max_sessions do
    Keyword.get(udp_config(), :max_sessions, @default_max_sessions)
  end

  defp rate_capacity do
    Keyword.get(udp_config(), :rate_capacity, @default_rate_capacity)
  end

  defp rate_per_sec do
    Keyword.get(udp_config(), :rate_per_sec, @default_rate_per_sec)
  end

  defp active_n do
    Keyword.get(udp_config(), :active_n, @default_active_n)
  end

  # 未知アドレスのレート表がセッション上限の 2 倍に達したら、新規キーは作らない。
  # 判定はトークン補充より前。初期トークンは capacity なので、後段だと新規が必ず入る。
  defp max_rate_entries do
    max(max_sessions() * 2, 1)
  end

  defp take_token(state, client) do
    now = now_ms()
    state = evict_stale_rates_if_full(state, client, now)

    if not Map.has_key?(state.rates, client) and map_size(state.rates) >= max_rate_entries() do
      {:limited, state}
    else
      charge_token(state, client, now)
    end
  end

  defp evict_stale_rates_if_full(state, client, now) do
    if Map.has_key?(state.rates, client) or map_size(state.rates) < max_rate_entries() do
      state
    else
      %{state | rates: sweep_rates(state.rates, now, session_timeout_ms())}
    end
  end

  defp charge_token(state, client, now) do
    cap = rate_capacity() * 1.0
    per_ms = rate_per_sec() / 1000
    {tokens, last} = Map.get(state.rates, client, {cap, now})
    elapsed = max(now - last, 0)
    tokens = min(cap, tokens + elapsed * per_ms)

    if tokens >= 1.0 do
      rates = Map.put(state.rates, client, {tokens - 1.0, now})
      {:allow, %{state | rates: rates}}
    else
      rates = Map.put(state.rates, client, {tokens, now})
      {:limited, %{state | rates: rates}}
    end
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep_sessions, sweep_interval_ms())
  end

  defp touch_session(state, client) do
    case state.sessions do
      %{^client => session} ->
        %{state | sessions: Map.put(state.sessions, client, %{session | last_seen_ms: now_ms()})}

      _ ->
        state
    end
  end

  defp sweep_stale_sessions(state) do
    now = now_ms()
    timeout = session_timeout_ms()

    {kept, expired} =
      Enum.split_with(state.sessions, fn {_client, session} ->
        now - session.last_seen_ms <= timeout
      end)

    Enum.each(expired, fn {client, session} ->
      Logger.info(
        "[Network.UDP] Session timed out client=#{inspect(client)} room=#{session.room_id}"
      )
    end)

    %{state | sessions: Map.new(kept), rates: sweep_rates(state.rates, now, timeout)}
  end

  defp sweep_rates(rates, now, timeout) do
    Map.filter(rates, fn {_client, {_tokens, last}} ->
      now - last <= timeout
    end)
  end
end
