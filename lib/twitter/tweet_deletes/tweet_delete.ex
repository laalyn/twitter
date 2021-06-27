defmodule Twitter.TweetDeletes.TweetDelete do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [updated_at: false, updated_at_source: false, type: :utc_datetime_usec]
  schema "tweet_deletes" do
    field :twitter_user_id, :integer
    field :twitter_tweet_id, :integer

    timestamps()
  end
end
