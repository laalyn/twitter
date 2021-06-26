defmodule TwitterWeb.FallbackController do
  use TwitterWeb, :controller

  def call(conn, err) do
    IO.inspect(err)

    err = case elem(err, 1) do
      {:error, %Ecto.Changeset{} = chg} ->
        chg.errors
        |> hd
        |> elem(1)
        |> elem(0)
      %Ecto.InvalidChangesetError{} = chg_err ->
        chg_err.changeset.errors
        |> hd
        |> elem(1)
        |> elem(0)
      %RuntimeError{} = run_err ->
        run_err.message
      msg ->
        if String.valid?(msg) do
          msg
        else
          "something went wrong"
        end
    end

    if conn do
      conn
      |> put_status(:bad_request)
      |> json(%{error: err})
    else
      %{reason: err}
    end
  end
end
