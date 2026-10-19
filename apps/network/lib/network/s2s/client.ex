defmodule Network.S2S.Client do
  @moduledoc """
  他インスタンスの read-only S2S API クライアント。

  ローカル鍵で短命 JWT を署名し、`GET /api/s2s/worlds` を取得する。

  ピア URL は `https://` だけを受け付ける。
  dev / test の平文は `config :network, :s2s_allow_http, true` のときだけ許す。
  本番（`config_env() == :prod`）の既定は false。
  """

  @doc """
  リモートのワールド一覧を取得する。

  `peer_base_url` 例: `"https://peer.example"`
  `audience` 未指定時は well-known の `domain` を使う。
  スキームが `https` でない場合は `{:error, :https_required}`。
  """
  @spec fetch_worlds(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def fetch_worlds(peer_base_url, opts \\ []) when is_binary(peer_base_url) do
    base = String.trim_trailing(peer_base_url, "/")
    get_fun = Keyword.get(opts, :get_fun, &default_get/2)

    with :ok <- ensure_https(base),
         {:ok, audience} <- resolve_audience(base, opts, get_fun),
         {:ok, token} <- Network.S2S.Instance.sign_request_token(audience) do
      get_fun.("#{base}/api/s2s/worlds", token)
    end
  end

  defp ensure_https(base) do
    scheme =
      case URI.parse(base).scheme do
        nil -> ""
        value -> String.downcase(value)
      end

    cond do
      scheme == "https" -> :ok
      scheme == "http" and s2s_allow_http?() -> :ok
      true -> {:error, :https_required}
    end
  end

  defp s2s_allow_http? do
    Application.get_env(:network, :s2s_allow_http, false)
  end

  defp resolve_audience(base, opts, get_fun) do
    case Keyword.get(opts, :audience) do
      aud when is_binary(aud) and aud != "" ->
        {:ok, aud}

      _ ->
        case get_fun.("#{base}/.well-known/alchemy-s2s.json", nil) do
          {:ok, %{"domain" => domain}} when is_binary(domain) and domain != "" ->
            {:ok, domain}

          {:ok, _} ->
            {:error, :peer_domain_missing}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp default_get(url, nil) do
    case Req.get(url, receive_timeout: 5_000) do
      {:ok, %{status: 200, body: body}} when is_map(body) -> {:ok, body}
      {:ok, %{status: 200}} -> {:error, :invalid_response_format}
      {:ok, %{status: status}} -> {:error, {:http_status, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp default_get(url, token) when is_binary(token) do
    case Req.get(url,
           headers: [{"authorization", "Bearer #{token}"}],
           receive_timeout: 5_000
         ) do
      {:ok, %{status: 200, body: body}} when is_map(body) -> {:ok, body}
      {:ok, %{status: 200, body: body}} -> {:error, {:invalid_response_format, body}}
      {:ok, %{status: status, body: body}} -> {:error, {:http_status, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end
end
