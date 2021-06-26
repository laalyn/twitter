defmodule Twitter.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    "[info] Starting Twitter.Application (#{:erlang.system_info(:emu_flavor)}, #{:erlang.system_info(:emu_type)})"
    |> IO.puts()

    ExTwitter.configure(
      consumer_key: System.get_env("TWITTER_CONSUMER_KEY"),
      consumer_secret: System.get_env("TWITTER_CONSUMER_SECRET"),
      access_token: System.get_env("TWITTER_ACCESS_TOKEN"),
      access_token_secret: System.get_env("TWITTER_ACCESS_SECRET")
    )

    children = [
      # Start the Ecto repository
      Twitter.Repo,
      # Start the Telemetry supervisor
      TwitterWeb.Telemetry,
      # Start the PubSub system
      {Phoenix.PubSub, name: Twitter.PubSub},
      # Start the Endpoint (http/https)
      TwitterWeb.Endpoint
      # Start a worker by calling: Twitter.Worker.start_link(arg)
      # {Twitter.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Twitter.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  def config_change(changed, _new, removed) do
    TwitterWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
