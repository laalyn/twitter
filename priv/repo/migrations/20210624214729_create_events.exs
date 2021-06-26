defmodule Twitter.Repo.Migrations.CreateEvents do
  use Ecto.Migration

  def change do
    create table(:events, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :val, :jsonb

      timestamps([updated_at: false, type: :utc_datetime_usec])
    end
  end
end
