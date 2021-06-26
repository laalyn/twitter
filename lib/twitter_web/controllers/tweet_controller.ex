defmodule TwitterWeb.TweetController do
  use TwitterWeb, :controller

  alias Twitter.Tweets

  alias TwitterWeb.FallbackController

  action_fallback TwitterWeb.FallbackController

  def stream(conn, _) do
    try do
      (fn -> Tweets.stream_tweets! end)
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
