defmodule Twitter.Events.Event do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [updated_at: false, updated_at_source: false, type: :utc_datetime_usec]
  schema "events" do
    field :val, :map

    timestamps()
  end
end
