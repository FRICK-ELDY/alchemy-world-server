defmodule Contents.Events.GameRemoteActionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Contents.Events.RemoteActionFixture

  @room_id "remote_action_test"

  setup do
    ensure_room_registry!()

    case Contents.Events.Game.start_link(room_id: @room_id) do
      {:ok, pid} ->
        on_exit(fn ->
          if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1000)
        end)

        {:ok, pid: pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid: pid}
    end
  end

  test "リモートの終了と未知メッセージではルームが落ちず、許可された action はシーンを更新する", %{
    pid: pid
  } do
    previous = Application.get_env(:server, :current)
    Application.put_env(:server, :current, RemoteActionFixture)

    on_exit(fn ->
      Application.put_env(:server, :current, previous)
      clear_scene_flags()
    end)

    %{frame_count: before_count} = :sys.get_state(pid)
    blob = :binary.copy(<<1>>, 10_000)

    log =
      capture_log(fn ->
        send(pid, {:ui_action, "__quit__"})
        send(pid, {:ui_action, 1})
        send(pid, {:blob, blob})
        send(pid, :quit_requested)
        send(pid, {:ui_action, "__retry__"})
        send(pid, {:ui_action, "__ping__"})
        _ = :sys.get_state(pid)
      end)

    assert Process.alive?(pid)
    assert log =~ "rejected ui_action=\"__quit__\""
    assert log =~ "ignoring unknown message"
    assert log =~ "ignored :quit_requested"
    refute log =~ "rejected ui_action=\"__retry__\""
    refute log =~ "rejected ui_action=\"__ping__\""
    assert byte_size(log) < 4_000

    scene = Contents.Scenes.Stack.get_scene_state(:playing)
    assert scene.retry == true
    assert scene.pinged == true
    refute Map.has_key?(scene, :quit_ran)

    send(pid, :elixir_frame_tick)
    %{frame_count: after_count} = :sys.get_state(pid)
    assert after_count > before_count
  end

  defp clear_scene_flags do
    if Process.whereis(Contents.Scenes.Stack) do
      Contents.Scenes.Stack.update_by_scene_type(:playing, fn state ->
        state
        |> Map.delete(:retry)
        |> Map.delete(:pinged)
        |> Map.delete(:quit_ran)
      end)
    end
  end

  defp ensure_room_registry! do
    case Process.whereis(Core.RoomRegistry) do
      nil ->
        case Registry.start_link(keys: :unique, name: Core.RoomRegistry) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end
end
