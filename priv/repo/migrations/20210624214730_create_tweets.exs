defmodule Twitter.Repo.Migrations.CreateTweets do
  use Ecto.Migration

  def change do
    create table(:tweets, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :twitter_id, :bigint

      add :filter_level, :string
      add :possibly_sensitive, :boolean
      add :is_retweet, :boolean
      add :is_quote, :boolean

      add :lang, :string
      add :text, :text
      add :truncated, :boolean
      add :source, :string
      add :coordinates, :geometry

      add :num_likes, :bigint
      add :num_retweets, :bigint

      add :in_reply_to_username, :string
      add :in_reply_to_twitter_user_id, :bigint
      add :in_reply_to_twitter_tweet_id, :bigint

      add :twitter_created_at, :utc_datetime

      add :retweeted_tweet_id, references(:tweets, [type: :binary_id, on_delete: :nilify_all, on_update: :update_all])
      add :quoted_tweet_id, references(:tweets, [type: :binary_id, on_delete: :nilify_all, on_update: :update_all])
      add :place_id, references(:places, [type: :binary_id, on_delete: :nilify_all, on_update: :update_all])
      add :user_id, references(:users, [type: :binary_id, on_delete: :delete_all, on_update: :update_all])

      timestamps([updated_at: false, type: :utc_datetime_usec])
    end

    create unique_index(:tweets, [:twitter_id])

    create index(:tweets, [:retweeted_tweet_id])
    create index(:tweets, [:quoted_tweet_id])
    create index(:tweets, [:place_id])
    create index(:tweets, [:user_id])

    # TODO indices to speed up sorts
  end
end
