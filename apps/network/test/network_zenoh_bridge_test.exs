defmodule Network.ZenohBridgeTest do
  @moduledoc false
  use ExUnit.Case, async: false

  setup do
    if :ets.whereis(:client_info) == :undefined do
      :ets.new(:client_info, [:named_table, :public, :set, read_concurrency: true])
    end

    :ets.delete_all_objects(:client_info)

    on_exit(fn ->
      Application.put_env(:network, :auth_required, false)
    end)

    :ok
  end

  describe "handle_info/2 client_info decode path" do
    test "valid protobuf payload is stored in ETS" do
      payload =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "win32",
          arch: "x86_64",
          family: "windows"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: payload,
        kind: :put
      }

      state = %{generation: 1, test_state: true}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [{{:main, :info}, info}] = :ets.lookup(:client_info, {:main, :info})
      assert info == %{os: "win32", arch: "x86_64", family: "windows"}
    end

    test "invalid payload is discarded and processing continues" do
      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: <<0xDE, 0xAD, 0xBE, 0xEF>>,
        kind: :put
      }

      state = %{generation: 1, test_state: true}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [] == :ets.lookup(:client_info, {:main, :info})
    end

    test "AUTH_REQUIRED 時は token なし client_info を拒否する" do
      Application.put_env(:network, :auth_required, true)

      payload =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "win32",
          arch: "x86_64",
          family: "windows"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: payload,
        kind: :put
      }

      state = %{generation: 1}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [] == :ets.lookup(:client_info, {:main, :info})
    end

    test "世代の付かない Sample は入力にしない" do
      payload =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "win32",
          arch: "x86_64",
          family: "windows"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: payload,
        kind: :put
      }

      state = %{generation: 1}

      assert {:noreply, ^state} = Network.ZenohBridge.handle_info(sample, state)
      assert [] == :ets.lookup(:client_info, {:main, :info})
    end

    test "AUTH_REQUIRED 時は RoomToken 付き client_info を受け入れる" do
      Application.put_env(:network, :auth_required, true)
      {:ok, token} = Network.RoomToken.sign("main")

      protobuf =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "linux",
          arch: "aarch64",
          family: "unix"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: Network.RoomAuth.wrap_payload(token, protobuf),
        kind: :put
      }

      state = %{generation: 1}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [{{:main, :info}, info}] = :ets.lookup(:client_info, {:main, :info})
      assert info.os == "linux"
    end

    test "AUTH_REQUIRED オフでもラップ済み client_info を受け入れる" do
      Application.put_env(:network, :auth_required, false)
      {:ok, token} = Network.RoomToken.sign("main")

      protobuf =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "darwin",
          arch: "arm64",
          family: "unix"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: Network.RoomAuth.wrap_payload(token, protobuf),
        kind: :put
      }

      state = %{generation: 1}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [{{:main, :info}, info}] = :ets.lookup(:client_info, {:main, :info})
      assert info.os == "darwin"
    end
  end

  describe "reconnect" do
    test "古い generation の sample は捨てる" do
      payload =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "win32",
          arch: "x86_64",
          family: "windows"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: payload,
        kind: :put
      }

      state = %{Network.ZenohBridge.initial_state() | generation: 2}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 1, sample}, state)

      assert [] == :ets.lookup(:client_info, {:main, :info})
    end

    test "現世代の sample は受け付ける" do
      payload =
        Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
          os: "linux",
          arch: "aarch64",
          family: "unix"
        })

      sample = %Zenohex.Sample{
        key_expr: "contents/room/main/client/info",
        payload: payload,
        kind: :put
      }

      state = %{Network.ZenohBridge.initial_state() | generation: 4}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 4, sample}, state)

      assert [{{:main, :info}, %{os: "linux"}}] = :ets.lookup(:client_info, {:main, :info})
    end

    test "死活失敗で購読を落とし、再接続を予約する" do
      ref = make_ref()

      state =
        bridge_state(%{
          phase: :up,
          liveness_ref: ref,
          session_id: :dead,
          client_mode: true,
          generation: 3,
          session_probe: fn _ -> {:error, :closed} end,
          session_closer: fn _ -> :ok end
        })

      assert {:noreply, new_state} =
               Network.ZenohBridge.handle_info({:check_liveness, ref}, state)

      assert new_state.phase == :down
      assert new_state.session_id == nil
      assert new_state.generation == 4
      assert new_state.reconnect_pending
      assert is_reference(new_state.reconnect_ref)

      stale = client_info_sample("win32", "x86_64", "windows")

      assert {:noreply, ^new_state} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 3, stale}, new_state)

      assert [] == :ets.lookup(:client_info, {:main, :info})
    end

    test "再接続に成功すると generation を進めて購読し直す" do
      ref = make_ref()

      previous_config = Application.get_env(:network, :zenoh_config)

      Application.put_env(
        :network,
        :zenoh_config,
        ~s|{ mode: "client", connect: { endpoints: ["tcp/127.0.0.1:7447"] } }|
      )

      on_exit(fn -> restore_env(:zenoh_config, previous_config) end)

      state =
        bridge_state(%{
          reconnect_pending: true,
          reconnect_ref: ref,
          session_opener: fn _config -> {:ok, :session} end,
          declare_subscriber: fn _sid, selector, pid -> {:ok, {selector, pid}} end,
          session_closer: fn _ -> :ok end
        })

      assert {:noreply, new_state} = Network.ZenohBridge.handle_info({:reconnect, ref}, state)
      assert new_state.generation == 1
      assert new_state.phase == :up
      assert new_state.session_id == :session
      assert new_state.client_mode
      {_, inbox} = new_state.mov_sub
      assert is_pid(inbox)
      assert elem(new_state.act_sub, 1) == inbox
      assert elem(new_state.info_sub, 1) == inbox

      on_exit(fn -> send(inbox, :stop) end)
    end

    test "再接続の open 失敗はバックオフを伸ばして再予約する" do
      ref = make_ref()

      state =
        bridge_state(%{
          reconnect_pending: true,
          reconnect_ref: ref,
          backoff_ms: 500,
          session_opener: fn _config -> {:error, :refused} end
        })

      assert {:noreply, new_state} = Network.ZenohBridge.handle_info({:reconnect, ref}, state)
      assert new_state.phase == :down
      assert new_state.generation == 0
      assert new_state.session_id == nil
      assert new_state.backoff_ms == 1_000
      assert new_state.reconnect_pending
      assert new_state.reconnect_ref != ref
    end

    test "info が成功している間は空の routers_zid でもセッションを維持する" do
      ref = make_ref()

      state =
        bridge_state(%{
          phase: :up,
          liveness_ref: ref,
          session_id: :session,
          client_mode: true,
          generation: 2,
          session_probe: fn _ ->
            {:ok, %Zenohex.Session.Info{zid: "z", routers_zid: [], peers_zid: []}}
          end,
          session_closer: fn _ -> flunk("empty routers must not close the session") end
        })

      assert {:noreply, held} = Network.ZenohBridge.handle_info({:check_liveness, ref}, state)
      assert held.phase == :up
      assert held.session_id == :session
      assert held.generation == 2
    end

    test "宣言の途中失敗は undeclare してから一度だけ close する" do
      ref = make_ref()
      {:ok, log} = Agent.start(fn -> [] end)
      on_exit(fn -> Agent.stop(log) end)

      state =
        bridge_state(%{
          reconnect_pending: true,
          reconnect_ref: ref,
          generation: 5,
          session_opener: fn _ -> {:ok, :session} end,
          declare_subscriber: fn _sid, selector, _pid ->
            if String.ends_with?(selector, "movement") do
              {:ok, :mov}
            else
              {:error, :declare_failed}
            end
          end,
          undeclare_subscriber: fn id -> Agent.update(log, &[id | &1]) end,
          session_closer: fn id -> Agent.update(log, &[{:close, id} | &1]) end
        })

      assert {:noreply, failed} = Network.ZenohBridge.handle_info({:reconnect, ref}, state)
      assert failed.session_id == nil
      assert failed.inbox == nil
      assert failed.mov_sub == nil
      assert failed.generation == 7
      assert Agent.get(log, &Enum.reverse/1) == [:mov, {:close, :session}]

      stale = client_info_sample("linux", "aarch64", "unix")

      assert {:noreply, ^failed} =
               Network.ZenohBridge.handle_info({:zenoh_sample, 6, stale}, failed)

      assert [] == :ets.lookup(:client_info, {:main, :info})

      assert {:noreply, again} =
               Network.ZenohBridge.handle_info({:reconnect, failed.reconnect_ref}, %{
                 failed
                 | session_opener: fn _ -> {:error, :refused} end
               })

      assert again.session_id == nil
      assert Agent.get(log, &Enum.reverse/1) == [:mov, {:close, :session}]
    end

    test "inbox の DOWN で世代を進めて再接続する" do
      ref = make_ref()

      inbox =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      state =
        bridge_state(%{
          phase: :up,
          inbox_ref: ref,
          inbox: inbox,
          session_id: :session,
          generation: 2,
          session_closer: fn _ -> :ok end
        })

      assert {:noreply, new_state} =
               Network.ZenohBridge.handle_info(
                 {:DOWN, ref, :process, self(), :killed},
                 state
               )

      assert new_state.phase == :down
      assert new_state.session_id == nil
      assert new_state.generation == 3
    end

    test "open はプロセスの外で走り、結果メッセージで購読する" do
      ref = make_ref()

      state =
        bridge_state(%{
          sync_io: false,
          reconnect_pending: true,
          reconnect_ref: ref,
          session_opener: fn _ -> {:ok, :session} end,
          declare_subscriber: fn _sid, selector, pid -> {:ok, {selector, pid}} end,
          session_closer: fn _ -> :ok end
        })

      assert {:noreply, opening} = Network.ZenohBridge.handle_info({:reconnect, ref}, state)
      assert opening.phase == :opening
      assert opening.session_id == nil

      assert_receive {:reconnect_opened, ^ref, {:ok, :session}}, 1_000

      assert {:noreply, up} =
               Network.ZenohBridge.handle_info(
                 {:reconnect_opened, ref, {:ok, :session}},
                 opening
               )

      assert up.phase == :up
      assert up.generation == 1
      {_selector, inbox} = up.mov_sub
      on_exit(fn -> send(inbox, :stop) end)
    end

    test "一致しない再接続トークンは無視する" do
      ref = make_ref()
      state = %{Network.ZenohBridge.initial_state() | reconnect_ref: ref, reconnect_pending: true}

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_info({:reconnect, make_ref()}, state)
    end

    test "put 失敗でセッションを閉じて再接続を予約する" do
      state =
        bridge_state(%{
          phase: :up,
          session_id: :session,
          generation: 4,
          session_put: fn _, _, _, _ -> {:error, :closed} end,
          session_closer: fn _ -> :ok end
        })

      assert {:noreply, new_state} =
               Network.ZenohBridge.handle_cast({:publish_frame, "main", <<1>>}, state)

      assert new_state.phase == :down
      assert new_state.session_id == nil
      assert new_state.generation == 5
      assert new_state.reconnect_pending
    end

    test "死活の info が終わるまで close しない" do
      liveness_ref = make_ref()
      probe_ref = make_ref()
      {:ok, log} = Agent.start(fn -> [] end)
      on_exit(fn -> Agent.stop(log) end)

      state =
        bridge_state(%{
          phase: :up,
          liveness_ref: liveness_ref,
          probe_ref: probe_ref,
          session_id: :session,
          mov_sub: :mov,
          generation: 4,
          session_put: fn _, _, _, _ -> {:error, :closed} end,
          undeclare_subscriber: fn id -> Agent.update(log, &[{:undeclare, id} | &1]) end,
          session_closer: fn id -> Agent.update(log, &[{:close, id} | &1]) end
        })

      assert {:noreply, waiting} =
               Network.ZenohBridge.handle_cast({:publish_frame, "main", <<1>>}, state)

      assert waiting.session_id == nil
      assert [%{session_id: :session, probe_ref: ^probe_ref}] = waiting.pending_releases
      assert Agent.get(log, &Enum.reverse/1) == []

      new_open = make_ref()
      other_liveness = make_ref()

      overwritten = %{
        waiting
        | opener_ref: new_open,
          phase: :opening,
          liveness_ref: other_liveness
      }

      assert {:noreply, still_waiting} =
               Network.ZenohBridge.handle_info(
                 {:liveness_result, other_liveness, {:ok, :next_info}},
                 overwritten
               )

      assert still_waiting.opener_ref == new_open
      assert [%{session_id: :session}] = still_waiting.pending_releases
      assert Agent.get(log, &Enum.reverse/1) == []

      assert {:noreply, open_down} =
               Network.ZenohBridge.handle_info(
                 {:DOWN, new_open, :process, self(), :noproc},
                 still_waiting
               )

      assert open_down.pending_releases == still_waiting.pending_releases
      assert Agent.get(log, &Enum.reverse/1) == []

      assert {:noreply, released} =
               Network.ZenohBridge.handle_info(
                 {:liveness_result, liveness_ref, {:error, :closed}},
                 open_down
               )

      assert released.pending_releases == []
      assert released.opener_ref == nil
      assert Agent.get(log, &Enum.reverse/1) == [{:undeclare, :mov}, {:close, :session}]
    end

    test "info を返さず落ちたプローブはセッションを閉じて再接続する" do
      probe_ref = make_ref()
      {:ok, log} = Agent.start(fn -> [] end)
      on_exit(fn -> Agent.stop(log) end)

      state =
        bridge_state(%{
          phase: :up,
          probe_ref: probe_ref,
          session_id: :session,
          generation: 2,
          session_closer: fn :session -> Agent.update(log, fn _ -> true end) end
        })

      assert {:noreply, new_state} =
               Network.ZenohBridge.handle_info(
                 {:DOWN, probe_ref, :process, self(), :noproc},
                 state
               )

      assert new_state.phase == :down
      assert new_state.session_id == nil
      assert new_state.generation == 3
      assert Agent.get(log, & &1)
    end

    test "open の待ちは打ち切り、遅れて開いた session は close する" do
      ref = make_ref()
      {:ok, log} = Agent.start(fn -> [] end)
      on_exit(fn -> Agent.stop(log) end)

      state =
        bridge_state(%{
          phase: :opening,
          reconnect_ref: ref,
          reconnect_pending: false,
          session_closer: fn id -> Agent.update(log, &[id | &1]) end
        })

      assert {:noreply, timed_out} =
               Network.ZenohBridge.handle_info({:open_timeout, ref}, state)

      assert timed_out.phase == :down
      assert timed_out.session_id == nil
      assert timed_out.reconnect_ref != ref

      assert {:noreply, _} =
               Network.ZenohBridge.handle_info(
                 {:reconnect_opened, ref, {:ok, :late}},
                 timed_out
               )

      assert Agent.get(log, & &1) == [:late]
    end

    test "戻らない info の保留 session は打ち切りで close する" do
      old_liveness = make_ref()
      old_probe = make_ref()
      current_liveness = make_ref()
      {:ok, log} = Agent.start(fn -> [] end)
      on_exit(fn -> Agent.stop(log) end)

      state =
        bridge_state(%{
          phase: :up,
          session_id: :current,
          liveness_ref: current_liveness,
          probe_ref: make_ref(),
          pending_releases: [
            %{
              probe_ref: old_probe,
              liveness_ref: old_liveness,
              session_id: :old,
              mov_sub: :mov,
              act_sub: nil,
              info_sub: nil
            }
          ],
          undeclare_subscriber: fn id -> Agent.update(log, &[{:undeclare, id} | &1]) end,
          session_closer: fn id -> Agent.update(log, &[{:close, id} | &1]) end
        })

      assert {:noreply, ignored} =
               Network.ZenohBridge.handle_info({:info_timeout, make_ref()}, state)

      assert ignored.pending_releases == state.pending_releases
      assert ignored.session_id == :current

      assert {:noreply, released} =
               Network.ZenohBridge.handle_info({:info_timeout, old_liveness}, ignored)

      assert released.pending_releases == []
      assert released.session_id == :current
      assert released.phase == :up
      assert Agent.get(log, &Enum.reverse/1) == [{:undeclare, :mov}, {:close, :old}]
    end

    test "実行中の info が打ち切り時間を超えたらセッションを捨てる" do
      liveness_ref = make_ref()
      {:ok, closed} = Agent.start(fn -> false end)
      on_exit(fn -> Agent.stop(closed) end)

      state =
        bridge_state(%{
          phase: :up,
          liveness_ref: liveness_ref,
          probe_ref: make_ref(),
          session_id: :session,
          session_closer: fn :session -> Agent.update(closed, fn _ -> true end) end
        })

      assert {:noreply, new_state} =
               Network.ZenohBridge.handle_info({:info_timeout, liveness_ref}, state)

      assert new_state.phase == :down
      assert new_state.session_id == nil
      assert new_state.pending_releases == []
      assert Agent.get(closed, & &1)
    end

    test "put の一時失敗ではセッションを捨てない" do
      state =
        bridge_state(%{
          phase: :up,
          session_id: :session,
          generation: 4,
          session_put: fn _, _, _, _ -> {:error, :congested} end,
          session_closer: fn _ -> flunk("transient put failure must not close the session") end
        })

      assert {:noreply, ^state} =
               Network.ZenohBridge.handle_cast({:publish_frame, "main", <<1>>}, state)
    end

    test "session_dead?/2 は info の成功を維持、失敗を切断にする" do
      empty = {:ok, %Zenohex.Session.Info{zid: "z", routers_zid: [], peers_zid: []}}
      linked = {:ok, %Zenohex.Session.Info{zid: "z", routers_zid: ["r"], peers_zid: []}}

      refute Network.ZenohBridge.session_dead?(true, empty)
      refute Network.ZenohBridge.session_dead?(false, empty)
      refute Network.ZenohBridge.session_dead?(true, linked)
      assert Network.ZenohBridge.session_dead?(true, {:error, :closed})
    end

    test "session_put_fatal?/1 は理由の語をそのまま見る" do
      assert Network.ZenohBridge.session_put_fatal?(:closed)
      assert Network.ZenohBridge.session_put_fatal?(:not_found)
      assert Network.ZenohBridge.session_put_fatal?(:invalid_session)
      assert Network.ZenohBridge.session_put_fatal?(:econnrefused)
      assert Network.ZenohBridge.session_put_fatal?("connection reset by peer")

      refute Network.ZenohBridge.session_put_fatal?(:reset)
      refute Network.ZenohBridge.session_put_fatal?(:congested)
      refute Network.ZenohBridge.session_put_fatal?("preset")
      refute Network.ZenohBridge.session_put_fatal?({:reset, %{detail: :closed}})
      refute Network.ZenohBridge.session_put_fatal?(%{reason: :closed})

      refute Network.ZenohBridge.session_put_fatal?(
               "native/zenohex_nif/src/session.rs:10: preset"
             )
    end

    test "next_backoff/1 は 2 倍し 8 秒で頭打ちにする" do
      assert Network.ZenohBridge.next_backoff(500) == 1_000
      assert Network.ZenohBridge.next_backoff(8_000) == 8_000
    end

    test "terminate/2 はセッションを close する" do
      {:ok, closed} = Agent.start(fn -> false end)
      on_exit(fn -> Agent.stop(closed) end)

      state =
        bridge_state(%{
          session_id: :session,
          session_closer: fn :session ->
            Agent.update(closed, fn _ -> true end)
            :ok
          end
        })

      assert :ok = Network.ZenohBridge.terminate(:shutdown, state)
      assert Agent.get(closed, & &1)
    end
  end

  defp bridge_state(overrides) do
    Network.ZenohBridge.initial_state()
    |> Map.merge(%{sync_io: true, send_after: fn _pid, _msg, _delay -> make_ref() end})
    |> Map.merge(overrides)
  end

  defp client_info_sample(os, arch, family) do
    payload =
      Alchemy.Client.ClientInfo.encode(%Alchemy.Client.ClientInfo{
        os: os,
        arch: arch,
        family: family
      })

    %Zenohex.Sample{
      key_expr: "contents/room/main/client/info",
      payload: payload,
      kind: :put
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:network, key)
  defp restore_env(key, value), do: Application.put_env(:network, key, value)
end
