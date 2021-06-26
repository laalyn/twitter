defmodule Twitter.Repo.Migrations.CreateTweetDeletes do
  use Ecto.Migration

  def change do
    create table(:tweet_deletes, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :twitter_user_id, :bigint
      add :twitter_tweet_id, :bigint
      add :event_id, references(:events, [type: :binary_id, on_delete: :delete_all, on_update: :update_all])

      timestamps([updated_at: false, type: :utc_datetime_usec])
    end

    create unique_index(:tweet_deletes, [:twitter_user_id, :twitter_tweet_id])

    create index(:tweet_deletes, [:event_id])
  end
end
