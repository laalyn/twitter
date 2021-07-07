defmodule Twitter.Repo.Migrations.CreateIndexes do
  use Ecto.Migration

  def change do
    create index(:users, [:verified])
    # create index(:users, [:location], [using: "GIN"])
    create index(:users, [:num_followers])
    create index(:users, [:num_following])
    create index(:users, [:num_tweets])
    create index(:users, [:num_likes])
    create index(:users, [:num_lists])
    create index(:users, [:twitter_created_at])

    create index(:tweets, [:is_retweet])
    create index(:tweets, [:is_quote])
    create index(:tweets, [:lang])
    create index(:tweets, [:truncated])
    create index(:tweets, [:coordinates], [using: "GIST"])
    create index(:tweets, [:num_likes])
    create index(:tweets, [:num_retweets])
    create index(:tweets, [:twitter_created_at])
  end
end
