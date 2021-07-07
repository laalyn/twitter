defmodule Twitter.Repo.Migrations.CreateTrgmIndexes do
  use Ecto.Migration

  def change do
    execute "create index if not exists users_location_index on users using gin (location gin_trgm_ops)"
  end
end
