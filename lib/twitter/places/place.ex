defmodule Twitter.Places.Place do
  use Ecto.Schema

  alias Geo.PostGIS.Geometry

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "places" do
    field :twitter_id_str, :string

    field :type, :string

    field :name, :string
    field :country_code, :string
    field :country_name, :string
    field :full_name, :string
    field :coordinates, Geometry
    field :url, :string
  end
end
