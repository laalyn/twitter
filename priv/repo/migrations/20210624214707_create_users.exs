defmodule Twitter.Repo.Migrations.CreateUsers do
  use Ecto.Migration

  def change do
    create table(:users, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :twitter_id, :bigint

      add :protected, :boolean
      add :verified, :boolean

      add :default_profile, :boolean
        add :default_profile_image, :boolean
          add :profile_image_url, :string
        add :username, :string
        add :name, :string
        add :description, :text
        add :location, :string
        add :url, :string

      add :num_followers, :bigint
      add :num_following, :bigint
      add :num_tweets, :bigint
      add :num_likes, :bigint
      add :num_lists, :bigint

      add :twitter_created_at, :utc_datetime

      timestamps([type: :utc_datetime_usec])
    end

    create unique_index(:users, [:twitter_id])

    # TODO indices to speed up sorts
  end
end
