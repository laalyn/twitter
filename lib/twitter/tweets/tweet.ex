defmodule Twitter.Tweets.Tweet do
  use Ecto.Schema

  alias Geo.PostGIS.Geometry

  alias Twitter.Events.Event
  alias Twitter.Users.User
  alias Twitter.Places.Place
  alias Twitter.Tweets.Tweet

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [updated_at: false, updated_at_source: false, type: :utc_datetime_usec]
  schema "tweets" do
    field :twitter_id, :integer

    field :filter_level, :string
    field :possibly_sensitive, :boolean
    field :is_retweet, :boolean
    field :is_quote, :boolean

    field :lang, :string
    field :text, :string
    field :truncated, :boolean
    field :source, :string
    field :coordinates, Geometry

    field :num_likes, :integer
    field :num_retweets, :integer

    field :in_reply_to_username, :string
    field :in_reply_to_twitter_user_id, :integer
    field :in_reply_to_twitter_tweet_id, :integer

    field :twitter_created_at, :utc_datetime

    belongs_to :retweeted_tweet, Tweet
    belongs_to :quoted_tweet, Tweet
    belongs_to :place, Place
    belongs_to :user, User
    belongs_to :event, Event

    timestamps()
  end
end
