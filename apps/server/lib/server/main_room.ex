defmodule Server.MainRoom do
  @moduledoc """
  起動時、および `Core.RoomSupervisor` の再起動後に `:main` を生成する。

  `init/1` が唯一の入口なので、スーパーバイザがこのプロセスを再起動するたびに
  `:main` が戻る。
  """

  use GenServer

  # 旧 :main が Registry とローカル名を離すまでの待ち。100 * 50ms。
  @max_attempts 100
  @retry_ms 50

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case ensure_default_room(@max_attempts) do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp ensure_default_room(0), do: {:error, :main_room_not_started}

  defp ensure_default_room(attempts) do
    case Core.RoomRegistry.get_loop(:main) do
      {:ok, pid} ->
        if supervised_main?(pid), do: :ok, else: wait_and_retry(attempts)

      :error ->
        start_or_wait(attempts)
    end
  end

  defp start_or_wait(attempts) do
    case Core.RoomSupervisor.start_room(:main) do
      {:ok, pid} ->
        if supervised_main?(pid), do: :ok, else: wait_and_retry(attempts)

      {:error, :already_started} ->
        wait_and_retry(attempts)

      # :main は Contents.Events.Game というローカル名を取る。
      # 旧プロセスが名前を離す前は {:already_started, pid} になる。その pid が
      # 今の RoomSupervisor の子でなければ、死ぬまで待ってから作り直す。
      {:error, {:already_started, pid}} ->
        if supervised_main?(pid), do: :ok, else: wait_and_retry(attempts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp supervised_main?(pid) when is_pid(pid) do
    Process.alive?(pid) and room_supervisor_child?(pid)
  end

  defp room_supervisor_child?(pid) do
    Core.RoomSupervisor
    |> DynamicSupervisor.which_children()
    |> Enum.any?(fn {_id, child, _type, _modules} -> child == pid end)
  catch
    :exit, _ -> false
  end

  defp wait_and_retry(attempts) do
    receive do
    after
      @retry_ms -> ensure_default_room(attempts - 1)
    end
  end
end
