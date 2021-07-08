defmodule Twitter.Repo.Migrations.CreatePlaceIndexes do
  use Ecto.Migration

  def change do
    create index(:places, [:type])
    create index(:places, [:country_code])
    create index(:places, [:coordinates], [using: "GIST"])
  end
end
