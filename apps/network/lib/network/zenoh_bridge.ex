defmodule Network.ZenohBridge do
  @moduledoc """
  Zenoh によるサーバー側トランスポート（フェーズ 3）。

  - フレーム publish: `game/room/{room_id}/frame`
  - movement/action subscribe: `game/room/*/input/movement`, `game/room/*/input/action`
  - client_info subscribe: `contents/room/*/client/info` → `:client_info` ETS に保存
  - 受信した入力は `Contents.Events.Game` へ `{:move_input, dx, dy}` / `{:ui_action, name}` で配送

  `AUTH_REQUIRED=true` のとき、movement / action / client_info のペイロードは
  `Network.RoomAuth.wrap_payload/2` 形式（RoomToken + protobuf）を必須とする。
  オフ時は従来どおり生 protobuf のみ（デモ・ローカル互換）。

  入力ペイロードの解釈は **protobuf**（movement / action / client_info）。

  設定: `config :network, :zenoh_enabled, true` で有効化。

  ## 再接続

  セッションの死活を見て、失敗したら指数バックオフ（500ms から 2 倍、上限 8s）で
  開き直し、3 本の subscriber を宣言し直す。初回の `open` 失敗でもプロセスは
  止まらず、同じバックオフで再試行する。

  保証する: セッションを捨てた時点で generation を進める。ゲーム入力にするのは、
  その時点の generation と一致する `{:zenoh_sample, generation, sample}` だけである。
  宣言の途中で失敗しても、宣言済み subscriber を undeclare してから close し、
  閉じた session id は state に残さない。死活の `info` が同じ id で走っている間は
  close しない。close するのは、その session の info を実行したプローブの結果か
  そのプローブの DOWN、またはその info が 5 秒以内に戻らなかったときである。
  別の open や次のセッションの info では閉じない。
  保証しない: 張り直しの隙間に届いた入力の保存、Zenoh 自体の到達保証。
  5 秒は info の所要時間の実測ではない。戻らない info が session を保留したままにしない上限である。

  `Session.info/1` が成功している間はセッションを維持する。peer モードで
  `routers_zid` が空なのは、接続先がルータではないときの正常な結果である。
  client モードの空一覧も、それだけでは捨てない。接続確立の遅延は実測していない。
  put の再接続は、閉じた・見つからない・接続拒否のようにセッションが使えない理由だけである。
  `Session.open/1` と `Session.info/1` は GenServer の外で実行し、結果をメッセージで受ける。
  open と info は 5 秒で待ちを打ち切る。これは所要時間の実測ではなく、
  `:opening` と保留 session を終わらせる上限である。遅れて開いたセッションは close する。
  停止時は、飛行中の open と info を合わせて最大 2 秒待つ。時間切れでも残りの session は close する。
  待ちはプローブの本数だけ足さない。
  """

  use GenServer
  require Logger

  # Zenoh key 形式
  @frame_key "game/room"

  # ワイルドカード購読用
  @movement_selector "game/room/*/input/movement"
  @action_selector "game/room/*/input/action"
  @client_info_selector "contents/room/*/client/info"

  # クライアント `reconnect_with_backoff` と同じ間隔。
  @reconnect_init_ms 500
  @reconnect_max_ms 8_000
  @liveness_interval_ms 1_000
  # :opening と、戻らない info が session を保留し続けるのを終わらせる上限。所要時間の実測値ではない。
  @open_timeout_ms 5_000
  # 停止時に、実行中の open / info をまとめて待つ上限。プローブごとに足さない。
  @release_wait_ms 2_000
  # GenServer mailbox がこの件数以上なら、inbox は sample を転送しない。
  @inbox_queue_max 64

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  フレームを Zenoh で publish する。
  GenServer.cast で非同期実行（60Hz を考慮）。

  ブリッジ未起動時も `GenServer.cast/2` はローカル名未登録ならクラッシュせず `:ok` を返す。
  contents 側は MFA 注入で本関数を呼ぶため、プロセス有無を contents が知らなくてよい。
  """
  def publish_frame(room_id, frame_binary) when is_binary(frame_binary) do
    GenServer.cast(__MODULE__, {:publish_frame, normalize_room_id(room_id), frame_binary})
  end

  defp normalize_room_id(:main), do: "main"
  defp normalize_room_id(id) when is_binary(id), do: id
  defp normalize_room_id(id) when is_atom(id), do: Atom.to_string(id)

  # ── GenServer ────────────────────────────────────────────────────────

  @doc false
  def initial_state do
    %{
      generation: 0,
      session_id: nil,
      inbox: nil,
      inbox_ref: nil,
      mov_sub: nil,
      act_sub: nil,
      info_sub: nil,
      phase: :down,
      client_mode: false,
      backoff_ms: @reconnect_init_ms,
      reconnect_pending: false,
      reconnect_ref: nil,
      opener_ref: nil,
      probe_ref: nil,
      liveness_ref: nil,
      pending_releases: []
    }
  end

  @doc false
  def next_backoff(current_ms) when is_integer(current_ms) and current_ms > 0 do
    min(current_ms * 2, @reconnect_max_ms)
  end

  @doc false
  def session_dead?(_client_mode, {:ok, _info}), do: false
  def session_dead?(_client_mode, _probe_result), do: true

  @doc false
  def session_put_fatal?(reason) do
    tokens =
      reason
      |> put_error_text()
      |> String.downcase()
      |> String.split(~r/[^a-z0-9]+/, trim: true)

    cond do
      tokens == ["closed"] -> true
      "closed" in tokens and "session" in tokens -> true
      tokens == ["not", "found"] -> true
      "invalid" in tokens and "session" in tokens -> true
      "disconnected" in tokens -> true
      "not" in tokens and "connected" in tokens -> true
      "econnrefused" in tokens -> true
      "connection" in tokens and "refused" in tokens -> true
      "econnreset" in tokens -> true
      "connection" in tokens and "reset" in tokens and "peer" in tokens -> true
      "broken" in tokens and "pipe" in tokens -> true
      "epipe" in tokens -> true
      true -> false
    end
  end

  defp put_error_text(reason) when is_atom(reason), do: Atom.to_string(reason)

  defp put_error_text(reason) when is_binary(reason) do
    case Regex.run(~r/native\/zenohex_nif\/.+:\d+:\s*(.*)/s, reason) do
      [_, message] -> message
      _ -> reason
    end
  end

  defp put_error_text(reason), do: inspect(reason)

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    ensure_client_info_table()
    # open が失敗しても止めない。購読は :reconnect で宣言する。
    {:ok, schedule_reconnect(initial_state(), 0)}
  end

  @impl true
  def terminate(_reason, state) do
    state = state |> await_inflight() |> release_all_pending()
    drop_session(state)
    :ok
  end

  defp zenoh_config do
    case Application.get_env(:network, :zenoh_config) do
      nil ->
        # zenoh_connect が指定されていれば client モードで zenohd に接続
        case Application.get_env(:network, :zenoh_connect) do
          nil ->
            Zenohex.Config.default()

          "" ->
            Zenohex.Config.default()

          connect ->
            # mode: client, connect.endpoints を設定して zenohd に明示接続
            Jason.encode!(%{
              "mode" => "client",
              "connect" => %{"endpoints" => [connect]}
            })
        end

      config ->
        config
    end
  end

  @impl true
  def handle_cast({:publish_frame, _room_id, _frame_binary}, %{session_id: nil} = state) do
    {:noreply, state}
  end

  def handle_cast({:publish_frame, room_id, frame_binary}, state) do
    key = "#{@frame_key}/#{room_id}/frame"

    case session_put(state, state.session_id, key, frame_binary) do
      :ok ->
        # デバッグ: 初回 5 回 + 以降 60 フレームに 1 回
        count = :persistent_term.get({__MODULE__, :publish_count}, 0) + 1
        :persistent_term.put({__MODULE__, :publish_count}, count)

        if count <= 5 or rem(count, 60) == 1 do
          Logger.info(
            "[ZenohBridge] publish ok key=#{key} size=#{byte_size(frame_binary)} count=#{count}"
          )
        end

        {:noreply, state}

      {:error, reason} ->
        Logger.warning("[ZenohBridge] publish_frame failed room=#{room_id}: #{inspect(reason)}")

        if session_put_fatal?(reason) do
          state = drop_session(%{state | backoff_ms: @reconnect_init_ms})
          {:noreply, schedule_reconnect(state, @reconnect_init_ms)}
        else
          {:noreply, state}
        end
    end
  end

  defp put_opts do
    # 古いフレームをキューに溜めず、バッチ待ちもせずすぐ送る（テザリングの TCP 遅延蓄積対策）
    [congestion_control: :drop, express: true, priority: :real_time]
  end

  @impl true
  def handle_info({:reconnect, ref}, %{reconnect_ref: ref} = state) do
    state = %{
      drop_session(state)
      | reconnect_pending: false,
        reconnect_ref: ref,
        phase: :opening,
        liveness_ref: nil
    }

    if sync_io?(state) do
      finish_open(state)
    else
      start_open(state, ref)
    end
  end

  def handle_info({:reconnect, _ref}, state), do: {:noreply, state}

  def handle_info(
        {:reconnect_opened, ref, result},
        %{reconnect_ref: ref, phase: :opening} = state
      ) do
    state = demonitor_opener(state)

    case result do
      {:ok, session_id} -> finish_subscribe(state, session_id)
      {:error, reason} -> fail_open(state, reason)
    end
  end

  def handle_info({:reconnect_opened, _ref, {:ok, session_id}}, state) do
    close_session(session_id, state)
    {:noreply, state}
  end

  def handle_info({:reconnect_opened, _ref, _result}, state), do: {:noreply, state}

  def handle_info({:check_liveness, ref}, %{liveness_ref: ref, phase: :up} = state) do
    if sync_io?(state) do
      apply_liveness(state, session_probe(state).(state.session_id))
    else
      start_probe(state, ref)
    end
  end

  def handle_info({:check_liveness, _ref}, state), do: {:noreply, state}

  def handle_info({:liveness_result, ref, result}, state) do
    state = finish_probe_result(state, ref)

    if state.liveness_ref == ref and state.phase == :up do
      apply_liveness(state, result)
    else
      {:noreply, state}
    end
  end

  def handle_info({:open_timeout, ref}, %{reconnect_ref: ref, phase: :opening} = state) do
    Logger.warning("[ZenohBridge] session open timed out after #{@open_timeout_ms}ms")
    state = demonitor_opener(state)
    fail_open(%{state | reconnect_ref: nil}, :open_timeout)
  end

  def handle_info({:open_timeout, _ref}, state), do: {:noreply, state}

  def handle_info({:info_timeout, ref}, state) do
    cond do
      bundle = Enum.find(state.pending_releases, &(&1.liveness_ref == ref)) ->
        Logger.warning("[ZenohBridge] deferred info timed out; closing session")
        demonitor_ref(bundle.probe_ref)
        {:noreply, release_by_probe(state, bundle.probe_ref)}

      state.liveness_ref == ref and state.phase == :up ->
        Logger.warning("[ZenohBridge] info timed out after #{@open_timeout_ms}ms; reconnecting")
        state = drop_session(%{demonitor_probe(state) | backoff_ms: @reconnect_init_ms})
        {:noreply, schedule_reconnect(state, @reconnect_init_ms)}

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cond do
      pending_probe?(state, ref) ->
        {:noreply, release_by_probe(state, ref)}

      state.opener_ref == ref and state.phase == :opening ->
        Logger.warning("[ZenohBridge] open task exited (#{inspect(reason)})")
        fail_open(%{state | opener_ref: nil}, :opener_down)

      state.opener_ref == ref ->
        {:noreply, %{state | opener_ref: nil}}

      state.probe_ref == ref and state.phase == :up ->
        Logger.warning("[ZenohBridge] liveness probe exited (#{inspect(reason)}); reconnecting")
        state = drop_session(%{state | probe_ref: nil, backoff_ms: @reconnect_init_ms})
        {:noreply, schedule_reconnect(state, @reconnect_init_ms)}

      state.probe_ref == ref ->
        {:noreply, %{state | probe_ref: nil}}

      state.inbox_ref == ref and state.phase == :up ->
        Logger.warning("[ZenohBridge] inbox exited (#{inspect(reason)}); reconnecting")
        state = drop_session(%{state | backoff_ms: @reconnect_init_ms})
        {:noreply, schedule_reconnect(state, @reconnect_init_ms)}

      state.inbox_ref == ref ->
        {:noreply, %{state | inbox: nil, inbox_ref: nil}}

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  # 購読コールバックは inbox が generation を付けて転送する。
  # 世代が違う sample は、閉じたセッションの遅延配送なので捨てる。
  def handle_info({:zenoh_sample, generation, %Zenohex.Sample{} = sample}, state) do
    if generation == state.generation do
      {:noreply, apply_sample(sample, state)}
    else
      Logger.debug(
        "[ZenohBridge] discard stale sample generation=#{generation} current=#{state.generation}"
      )

      {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    Logger.debug("[input:ZenohBridge] handle_info UNMATCHED msg=#{inspect(msg, limit: 3)}")
    {:noreply, state}
  end

  defp apply_sample(%Zenohex.Sample{key_expr: key_expr, payload: payload, kind: kind}, state) do
    parsed = parse_input_key(key_expr)

    if kind != :put do
      Logger.warning(
        "[input:ZenohBridge] Sample kind is not :put (got #{inspect(kind)}), skipping"
      )
    else
      case parsed do
        {:movement, room_id} ->
          handle_movement(room_id, payload)

        {:action, room_id} ->
          handle_action(room_id, payload)

        {:client_info, room_id} ->
          handle_client_info(room_id, payload)

        :unknown ->
          Logger.debug("[input:ZenohBridge] Unknown key_expr=#{key_expr}")
      end
    end

    state
  end

  defp schedule_reconnect(%{reconnect_pending: true} = state, _delay_ms), do: state

  defp schedule_reconnect(state, delay_ms) do
    ref = make_ref()
    send_later(state, {:reconnect, ref}, delay_ms)
    %{state | reconnect_pending: true, reconnect_ref: ref, phase: :down, liveness_ref: nil}
  end

  defp schedule_liveness(state) do
    ref = make_ref()
    send_later(state, {:check_liveness, ref}, @liveness_interval_ms)
    %{state | liveness_ref: ref, phase: :up}
  end

  defp send_later(state, message, delay_ms) do
    sender = Map.get(state, :send_after, &Process.send_after/3)
    sender.(self(), message, delay_ms)
    state
  end

  defp sync_io?(state), do: Map.get(state, :sync_io, false)

  defp start_open(state, ref) do
    parent = self()
    config = zenoh_config()

    case Task.start(fn -> send(parent, {:reconnect_opened, ref, open_session(config, state)}) end) do
      {:ok, pid} ->
        state = %{state | opener_ref: Process.monitor(pid)}
        send_later(state, {:open_timeout, ref}, @open_timeout_ms)
        {:noreply, state}

      {:error, reason} ->
        fail_open(state, reason)
    end
  end

  defp start_probe(state, ref) do
    parent = self()
    probe = session_probe(state)
    session_id = state.session_id

    case Task.start(fn -> send(parent, {:liveness_result, ref, probe.(session_id)}) end) do
      {:ok, pid} ->
        state = %{state | probe_ref: Process.monitor(pid)}
        send_later(state, {:info_timeout, ref}, @open_timeout_ms)
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("[ZenohBridge] liveness probe failed to start: #{inspect(reason)}")
        {:noreply, schedule_liveness(state)}
    end
  end

  defp finish_open(state) do
    case open_session(zenoh_config(), state) do
      {:ok, session_id} -> finish_subscribe(state, session_id)
      {:error, reason} -> fail_open(state, reason)
    end
  end

  defp finish_subscribe(state, session_id) do
    case subscribe_session(state, session_id) do
      {:ok, state} ->
        Logger.info(
          "[ZenohBridge] subscribed generation=#{state.generation} movement=#{@movement_selector}"
        )

        {:noreply, schedule_liveness(%{state | backoff_ms: @reconnect_init_ms})}

      {:error, reason, state} ->
        fail_open(state, reason)
    end
  end

  defp fail_open(state, reason) do
    delay = state.backoff_ms

    Logger.warning("[ZenohBridge] session open failed: #{inspect(reason)}; retry in #{delay}ms")

    state = %{state | backoff_ms: next_backoff(delay)}
    {:noreply, schedule_reconnect(state, delay)}
  end

  defp demonitor_opener(%{opener_ref: ref} = state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    %{state | opener_ref: nil}
  end

  defp demonitor_opener(state), do: state

  defp apply_liveness(state, result) do
    if session_dead?(state.client_mode, result) do
      Logger.warning("[ZenohBridge] session lost; reconnecting")
      state = drop_session(%{state | backoff_ms: @reconnect_init_ms})
      {:noreply, schedule_reconnect(state, @reconnect_init_ms)}
    else
      {:noreply, schedule_liveness(state)}
    end
  end

  defp subscribe_session(state, session_id) do
    generation = state.generation + 1
    inbox = start_inbox(self(), generation)
    inbox_ref = Process.monitor(inbox)

    state = %{
      state
      | generation: generation,
        client_mode: client_mode?(zenoh_config()),
        session_id: session_id,
        inbox: inbox,
        inbox_ref: inbox_ref
    }

    case declare_into(session_id, inbox, state, [
           {:mov_sub, @movement_selector},
           {:act_sub, @action_selector},
           {:info_sub, @client_info_selector}
         ]) do
      {:ok, state} ->
        {:ok, %{state | phase: :up}}

      {:error, reason, state} ->
        {:error, reason, drop_session(state)}
    end
  end

  defp declare_into(_session_id, _inbox, state, []), do: {:ok, state}

  defp declare_into(session_id, inbox, state, [{key, selector} | rest]) do
    case declare_sub(session_id, selector, inbox, state) do
      {:ok, sub} ->
        declare_into(session_id, inbox, Map.put(state, key, sub), rest)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp start_inbox(parent, generation) do
    # ブリッジが落ちたら inbox も落とす。:stop による終了は :normal なので
    # リンク先は巻き込まれない。
    spawn_link(fn -> inbox_loop(parent, generation) end)
  end

  defp inbox_loop(parent, generation) do
    receive do
      :stop ->
        :ok

      %Zenohex.Sample{} = sample ->
        unless mailbox_over_cap?(parent) do
          send(parent, {:zenoh_sample, generation, sample})
        end

        inbox_loop(parent, generation)

      _other ->
        inbox_loop(parent, generation)
    end
  end

  defp mailbox_over_cap?(pid) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} -> len >= @inbox_queue_max
      _ -> true
    end
  end

  defp drop_session(state) do
    state = retire_generation(state)
    state = stop_inbox(state)

    if probe_in_flight?(state) and session_held?(state) do
      defer_release(state)
    else
      release_session(state)
    end
  end

  defp probe_in_flight?(%{probe_ref: ref, phase: :up}) when is_reference(ref), do: true
  defp probe_in_flight?(_state), do: false

  defp session_held?(state) do
    state.session_id != nil or state.mov_sub != nil or state.act_sub != nil or
      state.info_sub != nil
  end

  defp defer_release(state) do
    bundle = %{
      probe_ref: state.probe_ref,
      liveness_ref: state.liveness_ref,
      session_id: state.session_id,
      mov_sub: state.mov_sub,
      act_sub: state.act_sub,
      info_sub: state.info_sub
    }

    %{
      state
      | session_id: nil,
        mov_sub: nil,
        act_sub: nil,
        info_sub: nil,
        liveness_ref: nil,
        probe_ref: nil,
        pending_releases: [bundle | state.pending_releases]
    }
  end

  defp release_session(state) do
    undeclare_sub(state.mov_sub, state)
    undeclare_sub(state.act_sub, state)
    undeclare_sub(state.info_sub, state)
    close_session(state.session_id, state)
    %{state | session_id: nil, mov_sub: nil, act_sub: nil, info_sub: nil}
  end

  defp finish_probe_result(state, ref) do
    case Enum.find(state.pending_releases, &(&1.liveness_ref == ref)) do
      nil ->
        if state.liveness_ref == ref, do: demonitor_probe(state), else: state

      bundle ->
        demonitor_ref(bundle.probe_ref)
        release_by_probe(state, bundle.probe_ref)
    end
  end

  defp pending_probe?(state, ref) do
    Enum.any?(state.pending_releases, &(&1.probe_ref == ref))
  end

  defp release_by_probe(state, probe_ref) do
    {taken, rest} = Enum.split_with(state.pending_releases, &(&1.probe_ref == probe_ref))
    Enum.each(taken, &close_bundle(&1, state))
    %{state | pending_releases: rest}
  end

  defp release_all_pending(state) do
    Enum.each(state.pending_releases, &close_bundle(&1, state))
    %{state | pending_releases: []}
  end

  defp close_bundle(bundle, state) do
    undeclare_sub(bundle.mov_sub, state)
    undeclare_sub(bundle.act_sub, state)
    undeclare_sub(bundle.info_sub, state)
    close_session(bundle.session_id, state)
  end

  defp demonitor_probe(%{probe_ref: ref} = state) when is_reference(ref) do
    demonitor_ref(ref)
    %{state | probe_ref: nil}
  end

  defp demonitor_probe(state), do: state

  defp demonitor_ref(ref) when is_reference(ref), do: Process.demonitor(ref, [:flush])
  defp demonitor_ref(_ref), do: false

  defp await_inflight(state) do
    deadline = System.monotonic_time(:millisecond) + @release_wait_ms
    await_inflight(state, inflight_waits(state), deadline)
  end

  defp inflight_waits(state) do
    pending =
      Map.new(state.pending_releases, fn bundle ->
        {bundle.probe_ref, {:probe, bundle.liveness_ref}}
      end)

    current =
      if is_reference(state.probe_ref),
        do: %{state.probe_ref => {:probe, state.liveness_ref}},
        else: %{}

    opener =
      if is_reference(state.opener_ref),
        do: %{state.opener_ref => {:open, state.reconnect_ref}},
        else: %{}

    pending |> Map.merge(current) |> Map.merge(opener)
  end

  defp await_inflight(state, waits, _deadline) when map_size(waits) == 0, do: state

  defp await_inflight(state, waits, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    liveness_to_probe =
      Map.new(waits, fn
        {probe_ref, {:probe, liveness_ref}} when is_reference(liveness_ref) ->
          {liveness_ref, probe_ref}

        _ ->
          {nil, nil}
      end)

    open_to_monitor =
      Map.new(waits, fn
        {opener_ref, {:open, reconnect_ref}} when is_reference(reconnect_ref) ->
          {reconnect_ref, opener_ref}

        _ ->
          {nil, nil}
      end)

    receive do
      {:DOWN, ref, :process, _pid, _reason} when is_map_key(waits, ref) ->
        await_inflight(clear_wait_ref(state, ref), Map.delete(waits, ref), deadline)

      {:liveness_result, ref, _result} when is_map_key(liveness_to_probe, ref) ->
        probe_ref = Map.fetch!(liveness_to_probe, ref)
        demonitor_ref(probe_ref)

        await_inflight(
          clear_wait_ref(state, probe_ref),
          Map.delete(waits, probe_ref),
          deadline
        )

      {:reconnect_opened, ref, {:ok, session_id}} when is_map_key(open_to_monitor, ref) ->
        close_session(session_id, state)
        opener_ref = Map.fetch!(open_to_monitor, ref)
        demonitor_ref(opener_ref)

        await_inflight(
          clear_wait_ref(state, opener_ref),
          Map.delete(waits, opener_ref),
          deadline
        )

      {:reconnect_opened, ref, _result} when is_map_key(open_to_monitor, ref) ->
        opener_ref = Map.fetch!(open_to_monitor, ref)
        demonitor_ref(opener_ref)

        await_inflight(
          clear_wait_ref(state, opener_ref),
          Map.delete(waits, opener_ref),
          deadline
        )
    after
      timeout ->
        Logger.warning(
          "[ZenohBridge] timed out waiting for #{map_size(waits)} session tasks before close"
        )

        Enum.each(Map.keys(waits), &demonitor_ref/1)
        Enum.reduce(Map.keys(waits), state, &clear_wait_ref(&2, &1))
    end
  end

  defp clear_wait_ref(state, ref) do
    state = clear_probe_ref(state, ref)
    if state.opener_ref == ref, do: %{state | opener_ref: nil}, else: state
  end

  defp clear_probe_ref(%{probe_ref: ref} = state, ref), do: %{state | probe_ref: nil}
  defp clear_probe_ref(state, _ref), do: state

  defp retire_generation(
         %{session_id: nil, inbox: nil, mov_sub: nil, act_sub: nil, info_sub: nil} = state
       ) do
    state
  end

  defp retire_generation(state), do: %{state | generation: state.generation + 1}

  defp stop_inbox(%{inbox: nil} = state), do: state

  defp stop_inbox(state) do
    send(state.inbox, :stop)
    if state.inbox_ref, do: Process.demonitor(state.inbox_ref, [:flush])
    %{state | inbox: nil, inbox_ref: nil}
  end

  defp undeclare_sub(nil, _state), do: :ok

  defp undeclare_sub(id, state) do
    case Map.get(state, :undeclare_subscriber) do
      nil ->
        undeclare_sub_default(id)

      fun ->
        fun.(id)
    end
  end

  defp undeclare_sub_default(id) when is_reference(id) do
    case Zenohex.Subscriber.undeclare(id) do
      :ok -> :ok
      {:error, reason} -> Logger.debug("[ZenohBridge] undeclare failed: #{inspect(reason)}")
    end
  end

  defp undeclare_sub_default(_id), do: :ok

  defp close_session(nil, _state), do: :ok

  defp close_session(session_id, state) do
    case session_closer(state).(session_id) do
      :ok -> :ok
      {:error, reason} -> Logger.debug("[ZenohBridge] session close failed: #{inspect(reason)}")
    end
  end

  defp open_session(config, state) do
    Map.get(state, :session_opener, &Zenohex.Session.open/1).(config)
  end

  defp declare_sub(session_id, selector, inbox, state) do
    Map.get(state, :declare_subscriber, &Zenohex.Session.declare_subscriber/3).(
      session_id,
      selector,
      inbox
    )
  end

  defp session_put(state, session_id, key, payload) do
    Map.get(state, :session_put, &Zenohex.Session.put/4).(session_id, key, payload, put_opts())
  end

  defp session_probe(state) do
    Map.get(state, :session_probe, &Zenohex.Session.info/1)
  end

  defp session_closer(state) do
    Map.get(state, :session_closer, &Zenohex.Session.close/1)
  end

  defp client_mode?(config) when is_binary(config) do
    # JSON と JSON5 のどちらでも mode を読む。Jason は JSON5 を解けない。
    Regex.match?(~r/mode\s*:\s*["']client["']/, config)
  end

  defp client_mode?(_config), do: false

  defp parse_input_key(key_expr) do
    # "game/room/main/input/movement" -> {:movement, "main"}
    # "contents/room/main/client/info" -> {:client_info, "main"}
    parts = String.split(key_expr, "/")

    case parts do
      ["contents", "room", room_id, "client", "info"] ->
        {:client_info, room_id}

      ["game", "room", room_id | _rest] ->
        suffix = Enum.drop(parts, 3) |> Enum.join("/")

        cond do
          suffix == "input/movement" -> {:movement, room_id}
          suffix == "input/action" -> {:action, room_id}
          true -> :unknown
        end

      _ ->
        :unknown
    end
  end

  defp handle_movement(room_id, payload) do
    case Network.RoomAuth.unwrap_payload(payload, room_id) do
      {:ok, inner} ->
        case decode_movement(inner) do
          {:ok, {dx, dy}} ->
            forward_move_input(room_id, dx, dy)

          :error ->
            Logger.warning("[input:ZenohBridge] handle_movement decode error room=#{room_id}")
        end

      {:error, reason} ->
        Logger.debug(
          "[input:ZenohBridge] movement unauthorized room=#{room_id} reason=#{inspect(reason)}"
        )
    end
  end

  defp decode_movement(payload) do
    case try_decode_movement_protobuf(payload) do
      {:ok, {dx, dy}} -> {:ok, {dx * 1.0, dy * 1.0}}
      {:error, _} -> :error
    end
  end

  defp handle_action(room_id, payload) do
    case Network.RoomAuth.unwrap_payload(payload, room_id) do
      {:ok, inner} ->
        case decode_action(inner) do
          {:ok, name} ->
            forward_ui_action(room_id, name)

          :error ->
            Logger.warning("[input:ZenohBridge] Invalid action payload room=#{room_id}")
        end

      {:error, reason} ->
        Logger.debug(
          "[input:ZenohBridge] action unauthorized room=#{room_id} reason=#{inspect(reason)}"
        )
    end
  end

  defp decode_action(payload) do
    case try_decode_action_protobuf(payload) do
      {:ok, name} when is_binary(name) -> {:ok, name}
      {:error, _} -> :error
    end
  end

  defp forward_move_input(room_id, dx, dy) do
    room_key = room_id_for_registry(room_id)

    case Core.RoomRegistry.get_loop(room_key) do
      {:ok, pid} ->
        send(pid, {:move_input, dx, dy})

      :error ->
        Logger.warning(
          "[input:ZenohBridge] forward_move_input FAILED: No event handler for room=#{room_id}, dropping (dx=#{dx}, dy=#{dy})"
        )
    end
  end

  defp forward_ui_action(room_id, name) do
    room_key = room_id_for_registry(room_id)

    case Core.RoomRegistry.get_loop(room_key) do
      {:ok, pid} ->
        send(pid, {:ui_action, name})

      :error ->
        Logger.debug(
          "[ZenohBridge] No event handler for room=#{room_id}, dropping action #{name}"
        )
    end
  end

  # Core.RoomRegistry の登録形式: :main は atom、他ルームは binary のまま
  defp room_id_for_registry("main"), do: :main
  defp room_id_for_registry(id) when is_binary(id), do: id

  # ── client_info ETS ──────────────────────────────────────────────────────

  defp ensure_client_info_table do
    if :ets.whereis(:client_info) == :undefined do
      :ets.new(:client_info, [:named_table, :public, :set, read_concurrency: true])
    end
  end

  # client_info の room_id 最大数（DoS 対策: 無制限の room 作成でメモリ枯渇を防ぐ）
  @client_info_max_rooms 100

  defp handle_client_info(room_id, payload) do
    case Network.RoomAuth.unwrap_payload(payload, room_id) do
      {:ok, inner} ->
        if valid_room_id_for_client_info?(room_id) do
          room_key = if room_id == "main", do: :main, else: room_id

          if new_client_info_room?(room_key) and client_info_table_at_limit?() do
            Logger.warning(
              "[ZenohBridge] Rejected client_info: max rooms (#{@client_info_max_rooms}) reached"
            )
          else
            do_handle_client_info(room_id, room_key, inner)
          end
        else
          Logger.debug("[ZenohBridge] Rejected client_info: invalid room_id=#{inspect(room_id)}")
        end

      {:error, reason} ->
        Logger.warning(
          "[ZenohBridge] client_info unauthorized room=#{room_id} reason=#{inspect(reason)}"
        )
    end
  end

  defp do_handle_client_info(room_id, room_key, payload) do
    case decode_client_info(payload) do
      {:ok, info} when is_map(info) ->
        case normalize_client_info(info) do
          normalized when is_map(normalized) ->
            :ets.insert(:client_info, {{room_key, :info}, normalized})

          _ ->
            Logger.debug("[ZenohBridge] Invalid client info structure room=#{room_id}")
        end

      _ ->
        Logger.debug("[ZenohBridge] Invalid client info payload room=#{room_id}")
    end
  end

  defp valid_room_id_for_client_info?(room_id) when is_binary(room_id) do
    byte_size(room_id) in 1..64 and Regex.match?(~r/^[a-zA-Z0-9_-]+$/, room_id)
  end

  defp valid_room_id_for_client_info?(_), do: false

  defp new_client_info_room?(room_key) do
    :ets.whereis(:client_info) != :undefined and
      :ets.lookup(:client_info, {room_key, :info}) == []
  end

  defp client_info_table_at_limit? do
    case :ets.info(:client_info, :size) do
      size when is_integer(size) -> size >= @client_info_max_rooms
      _ -> false
    end
  end

  defp decode_client_info(payload) when is_binary(payload) and byte_size(payload) > 0 do
    case try_decode_client_info_protobuf(payload) do
      {:ok, info} ->
        {:ok, info}

      {:error, _} ->
        :error
    end
  end

  defp decode_client_info(_), do: :error

  defp try_decode_movement_protobuf(payload) when is_binary(payload) do
    case Alchemy.Input.Movement.decode(payload) do
      %Alchemy.Input.Movement{dx: dx, dy: dy} when is_number(dx) and is_number(dy) ->
        {:ok, {dx, dy}}

      _ ->
        {:error, :invalid_protobuf_movement}
    end
  rescue
    e ->
      Logger.debug("[input:ZenohBridge] movement protobuf decode failed: #{Exception.message(e)}")

      {:error, :invalid_protobuf_movement}
  end

  defp try_decode_action_protobuf(payload) when is_binary(payload) do
    case Alchemy.Input.Action.decode(payload) do
      %Alchemy.Input.Action{name: name} when is_binary(name) and byte_size(name) > 0 ->
        {:ok, name}

      _ ->
        {:error, :invalid_protobuf_action}
    end
  rescue
    e ->
      Logger.debug("[input:ZenohBridge] action protobuf decode failed: #{Exception.message(e)}")

      {:error, :invalid_protobuf_action}
  end

  defp try_decode_client_info_protobuf(payload) when is_binary(payload) do
    case Alchemy.Client.ClientInfo.decode(payload) do
      %Alchemy.Client.ClientInfo{os: os, arch: arch, family: family}
      when is_binary(os) and is_binary(arch) and is_binary(family) ->
        {:ok, %{"os" => os, "arch" => arch, "family" => family}}

      _ ->
        {:error, :invalid_protobuf_client_info}
    end
  rescue
    e ->
      Logger.debug("[ZenohBridge] client_info protobuf decode failed: #{Exception.message(e)}")

      {:error, :invalid_protobuf_client_info}
  end

  defp normalize_client_info(%{os: o, arch: a, family: f}) do
    with {:ok, os} <- safe_to_string(o),
         {:ok, arch} <- safe_to_string(a),
         {:ok, family} <- safe_to_string(f) do
      %{os: os, arch: arch, family: family}
    else
      _ -> nil
    end
  end

  defp normalize_client_info(%{"os" => o, "arch" => a, "family" => f}) do
    with {:ok, os} <- safe_to_string(o),
         {:ok, arch} <- safe_to_string(a),
         {:ok, family} <- safe_to_string(f) do
      %{os: os, arch: arch, family: family}
    else
      _ -> nil
    end
  end

  defp normalize_client_info(_), do: nil

  # to_string/1 は map や不正な list 等で ArgumentError を起こすため、
  # 攻撃者が ZenohBridge をクラッシュさせる DoS の原因になる。
  # 安全な型のみ許可する。
  defp safe_to_string(v) when is_binary(v), do: {:ok, v}
  defp safe_to_string(v) when is_atom(v), do: {:ok, Atom.to_string(v)}
  defp safe_to_string(v) when is_integer(v), do: {:ok, Integer.to_string(v)}
  defp safe_to_string(v) when is_float(v), do: {:ok, Float.to_string(v)}

  defp safe_to_string(v) when is_list(v) do
    if List.ascii_printable?(v), do: {:ok, List.to_string(v)}, else: :error
  end

  defp safe_to_string(_), do: :error
end
