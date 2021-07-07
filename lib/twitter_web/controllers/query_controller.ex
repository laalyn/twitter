defmodule TwitterWeb.QueryController do
  use TwitterWeb, :controller

  alias Twitter.Queries

  alias TwitterWeb.FallbackController

  action_fallback TwitterWeb.FallbackController

  def countries(conn, _) do
    try do
      (fn -> Queries.countries! end)
      |> spawn

      conn
      |> put_status(:accepted)
      |> json(%{status: "started"})
    rescue err ->
      IO.inspect(__STACKTRACE__)
      FallbackController.call(conn, {:error, err})
    end
  end
end
