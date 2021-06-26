defmodule Twitter.Users.User do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]
  schema "users" do
    field :twitter_id, :integer

    field :protected, :boolean
    field :verified, :boolean

    field :default_profile, :boolean
    field :default_profile_image, :boolean
    field :profile_image_url, :string
    field :username, :string
    field :name, :string
    field :description, :string
    field :location, :string
    field :url, :string

    field :num_followers, :integer
    field :num_following, :integer
    field :num_tweets, :integer
    field :num_likes, :integer
    field :num_lists, :integer

    field :twitter_created_at, :utc_datetime

    timestamps()
  end
end
