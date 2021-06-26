defmodule Twitter.Repo.Migrations.CreatePlaces do
  use Ecto.Migration

  def change do
    create table(:places, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :twitter_id_str, :string

      add :type, :string

      add :name, :text
      add :country_code, :string
      add :country_name, :string
      add :full_name, :text
      add :coordinates, :geometry
      add :url, :string
    end

    create unique_index(:places, [:twitter_id_str])
  end
end
