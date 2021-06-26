defmodule Twitter.Tweets do
  import Ecto.Query, warn: false
  alias Ecto.UUID
  alias Twitter.Repo

  use Timex

  alias Twitter.Users.User
  alias Twitter.Places.Place
  alias Twitter.Tweets.Tweet
  alias Twitter.TweetDeletes.TweetDelete
  alias Twitter.Events.Event

  alias ExTwitter.Model

  def stream_tweets!() do
    ExTwitter.stream_sample([receive_messages: true])
    |> Enum.reduce({{{0, DateTime.utc_now}, {DateTime.utc_now}}, {[], [], [], {[], [], [], []}, []}, {%{}, %{}, %{}}}, fn (cur, {{{cnt, cpt} = cpt_timer, {last_insert} = insert_timer} = timers, {events, tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}) ->
      try do
        cpt_diff = DateTime.utc_now
                   |> DateTime.diff(cpt, :second)
                   |> abs

        cpt_timer = if cpt_diff > 60 do
          IO.ANSI.format([:black_background, IO.ANSI.format([:blue, "[tweets] #{cnt} cycles/min"])])
          |> IO.puts

          {0, DateTime.utc_now}
        else
          {cnt + 1, cpt}
        end

        insert_diff = DateTime.utc_now
                      |> DateTime.diff(last_insert, :second)
                      |> abs

        {{last_insert} = insert_timer, {events, tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups} =
          if insert_diff > 10 do
            Repo.transaction(fn ->
              IO.ANSI.format([:white_background, IO.ANSI.format([:black, "[tweets] inserting #{length(events)} events"])])
              |> IO.puts

              Event
              |> Repo.insert_all(events)

              users_w_both = Enum.uniq_by(users_w_both, fn (cur) ->
                cur.twitter_id
              end)

              IO.ANSI.format([:blue_background, IO.ANSI.format([:black, "[tweets] inserting #{length(users_w_both)} users w both followers and following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_w_both, on_conflict: {:replace_all_except, [:num_followers, :num_following, :inserted_at]}, conflict_target: [:twitter_id])

              users_wo_followers = Enum.uniq_by(users_wo_followers, fn (cur) ->
                cur.twitter_id
              end)

              IO.ANSI.format([:blue_background, IO.ANSI.format([:black, "[tweets] inserting #{length(users_wo_followers)} users w/o any followers"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_followers, on_conflict: {:replace_all_except, [:num_followers, :inserted_at]}, conflict_target: [:twitter_id])

              users_wo_following = Enum.uniq_by(users_wo_following, fn (cur) ->
                cur.twitter_id
              end)

              IO.ANSI.format([:blue_background, IO.ANSI.format([:black, "[tweets] inserting #{length(users_wo_following)} users w/o any following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_following, on_conflict: {:replace_all_except, [:num_following, :inserted_at]}, conflict_target: [:twitter_id])

              users_wo_both = Enum.uniq_by(users_wo_both, fn (cur) ->
                cur.twitter_id
              end)

              IO.ANSI.format([:blue_background, IO.ANSI.format([:black, "[tweets] inserting #{length(users_wo_both)} users w/o either followers nor following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_both, on_conflict: {:replace_all_except, [:inserted_at]}, conflict_target: [:twitter_id])

              places = Enum.uniq_by(places, fn (cur) ->
                cur.twitter_id_str
              end)

              IO.ANSI.format([:yellow_background, IO.ANSI.format([:black, "[tweets] inserting #{length(places)} places"])])
              |> IO.puts

              Place
              |> Repo.insert_all(places, on_conflict: :replace_all, conflict_target: [:twitter_id_str])

              tweets = Enum.uniq_by(tweets, fn ({depth, cur}) ->
                cur.twitter_id
              end)

              {max_depth, tweets} = Enum.reduce(tweets, {0, %{}}, fn ({depth, cur}, {max_depth, acc}) ->
                tweets = if acc[depth] === nil do
                  []
                else
                  acc[depth]
                end

                acc = acc
                      |> Map.put(depth, [cur | tweets])

                {max(depth, max_depth), acc}
              end)

              max_depth..0
              |> Enum.each(fn (cur) ->
                IO.ANSI.format([:green_background, IO.ANSI.format([:black, "[tweets] inserting #{length(tweets[cur])} tweets at depth #{cur}"])])
                |> IO.puts

                Tweet
                |> Repo.insert_all(tweets[cur], on_conflict: :replace_all, conflict_target: [:twitter_id])
              end)

              tweet_deletes = Enum.uniq_by(tweet_deletes, fn (cur) ->
                {cur.twitter_user_id, cur.twitter_tweet_id}
              end)

              IO.ANSI.format([:black_background, IO.ANSI.format([:green, "[tweets] inserting #{length(tweet_deletes)} tweet deletes"])])
              |> IO.puts

              TweetDelete
              |> Repo.insert_all(tweet_deletes, on_conflict: :replace_all, conflict_target: [:twitter_user_id, :twitter_tweet_id])

              {:ok}
            end, timeout: :infinity)

            {{DateTime.utc_now}, {[], [], [], {[], [], [], []}, []}, {%{}, %{}, %{}}}
          else
            {insert_timer, inserts, lookups}
          end

        case cur do
          %Model.Tweet{} ->
            {_, {inserts, lookups}} = add_tweet!(cur, {inserts, lookups}, 0)

            {{cpt_timer, insert_timer}, inserts, lookups}
          %Model.DeletedTweet{} ->
            event = cur
                    |> Map.from_struct()

            event = %{
              id: UUID.generate(),
              val: event,
              inserted_at: DateTime.utc_now,
            }

            events = [event | events]

            tweet_delete = cur.status

            tweet_delete = %{
              id: UUID.generate(),
              twitter_user_id: tweet_delete.user_id,
              twitter_tweet_id: tweet_delete.id,
              event_id: event.id,
              inserted_at: DateTime.utc_now
            }

            tweet_deletes = [tweet_delete | tweet_deletes]

            {{cpt_timer, insert_timer}, {events, tweets, tweet_deletes, users, places}, lookups}
          %Model.Limit{} ->
            IO.ANSI.format([:black_background, IO.ANSI.format([:yellow, "[tweets] got limit on #{cur.track}"])])
            |> IO.puts

            event = cur
                    |> Map.from_struct()

            %Event {
              val: event
            }
            |> Repo.insert!

            {{cpt_timer, insert_timer}, inserts, lookups}
          %Model.StallWarning{} ->
            IO.ANSI.format([:black_background, IO.ANSI.format([:yellow, "[tweets] got stall warning #{cur.code}, #{cur.message} (#{cur.percent_full})"])])
            |> IO.puts

            event = cur
                    |> Map.from_struct()

            %Event {
              val: event
            }
            |> Repo.insert!

            {{cpt_timer, insert_timer}, inserts, lookups}
          _ ->
            {{cpt_timer, insert_timer}, inserts, lookups}
        end
      rescue err ->
        IO.inspect(__STACKTRACE__)
        IO.inspect(err)

        IO.ANSI.format([:red_background, IO.ANSI.format([:black, "[tweets] cycle failed, attempting to continue..."])])
        |> IO.puts

        {timers, inserts, lookups}
      end
    end)

    IO.ANSI.format([:black_background, IO.ANSI.format([:red, "[tweets] stream stopped"])])
    |> IO.puts

    :ok
  end

  defp add_tweet!(cur, {{events, tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}, depth) do
    event = cur.raw_data

    event = %{
      id: UUID.generate(),
      val: event,
      inserted_at: DateTime.utc_now,
    }

    events = [event | events]

    user = cur.user

    user_created_at = user.created_at
                      |> Timex.parse!("%a %b %d %T %z %Y", :strftime)

    user_inserted_at = DateTime.utc_now

    user_id_lookup = if user_id_lookup[user.id] === nil do
      user_id_lookup
      |> Map.put(user.id, UUID.generate())
    else
      user_id_lookup
    end

    user = %{
      id: user_id_lookup[user.id],
      twitter_id: user.id,
      protected: user.protected,
      verified: user.verified,
      default_profile: user.default_profile,
      default_profile_image: (if user.profile_image_url_https == "https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png", do: true, else: user.default_profile_image),
      profile_image_url: user.profile_image_url_https,
      username: user.screen_name,
      name: user.name,
      description: user.description,
      location: user.location,
      url: user.url,
      num_followers: user.followers_count,
      num_following: user.friends_count,
      num_tweets: user.statuses_count,
      num_likes: user.favourites_count,
      num_lists: user.listed_count,
      twitter_created_at: user_created_at,
      inserted_at: user_inserted_at,
      updated_at: user_inserted_at,
    }

    users = cond do
      user.num_followers == 0 && user.num_following == 0 ->
        {users_w_both, users_wo_followers, users_wo_following, [user | users_wo_both]}
      user.num_followers == 0 ->
        {users_w_both, [user | users_wo_followers], users_wo_following, users_wo_both}
      user.num_following == 0 ->
        {users_w_both, users_wo_followers, [user | users_wo_following], users_wo_both}
      true ->
        {[user | users_w_both], users_wo_followers, users_wo_following, users_wo_both}
    end

    place = cur.place

    {places, place_id_lookup} = if place !== nil do
      coords = place.bounding_box
               |> map_cast!
               |> Map.put(:coordinates, [hd(place.bounding_box.coordinates) ++ [hd(hd(place.bounding_box.coordinates))]])
               |> Map.delete(:raw_data)
               |> Jason.encode!
               |> Jason.decode!
               |> Geo.JSON.decode!

      place_id_lookup = if place_id_lookup[place.id] === nil do
        place_id_lookup
        |> Map.put(place.id, UUID.generate())
      else
        place_id_lookup
      end

      place = %{
        id: place_id_lookup[place.id],
        twitter_id_str: place.id,
        type: place.place_type,
        name: place.name,
        country_code: place.country_code,
        country_name: place.country,
        full_name: place.full_name,
        coordinates: coords,
        url: place.url,
      }

      {[place | places], place_id_lookup}
    else
      {places, place_id_lookup}
    end

    tweet = cur

    {inserts, lookups} = {{events, tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}

    {is_retweet, {retweeted_tweet_id, {{events, tweets, tweet_deletes, users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}}} =
      if tweet.raw_data[:retweeted_status] !== nil do
        result = tweet.raw_data.retweeted_status
                 |> Map.put(:raw_data, tweet.raw_data.retweeted_status)
                 |> add_tweet!({inserts, lookups}, depth + 1)

        {true, result}
      else
        {false, {nil, {inserts, lookups}}}
      end

    {inserts, lookups} = {{events, tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}

    {quoted_tweet_id, {{events, tweets, tweet_deletes, users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}} =
      if tweet.raw_data[:quoted_status] !== nil do
        tweet.raw_data.quoted_status
        |> Map.put(:raw_data, tweet.raw_data.quoted_status)
        |> add_tweet!({inserts, lookups}, depth + 1)
      else
        {nil, {inserts, lookups}}
      end

    coords = if tweet.coordinates !== nil do
      tweet.coordinates
      |> map_cast!
      |> Map.delete(:raw_data)
      |> Jason.encode!
      |> Jason.decode!
      |> Geo.JSON.decode!
    else
      tweet.coordinates
    end

    tweet_created_at = tweet.created_at
                       |> Timex.parse!("%a %b %d %T %z %Y", :strftime)

    tweet_id_lookup = if tweet_id_lookup[tweet.id] === nil do
      tweet_id_lookup
      |> Map.put(tweet.id, UUID.generate())
    else
      tweet_id_lookup
    end

    {_, inner} = tweet = {depth, %{
      id: tweet_id_lookup[tweet.id],
      twitter_id: tweet.id,
      filter_level: tweet.filter_level,
      possibly_sensitive: tweet.raw_data[:possibly_sensitive],
      is_retweet: is_retweet,
      is_quote: tweet.is_quote_status,
      lang: tweet.lang,
      text: tweet.text,
      truncated: tweet.truncated,
      source: tweet.source,
      coordinates: coords,
      num_likes: tweet.favorite_count,
      num_retweets: tweet.retweet_count,
      in_reply_to_username: tweet.in_reply_to_screen_name,
      in_reply_to_twitter_user_id: tweet.in_reply_to_user_id,
      in_reply_to_twitter_tweet_id: tweet.in_reply_to_status_id,
      twitter_created_at: tweet_created_at,
      retweeted_tweet_id: retweeted_tweet_id,
      quoted_tweet_id: quoted_tweet_id,
      place_id: (if place !== nil, do: hd(places).id, else: nil),
      user_id: user.id,
      event_id: event.id,
      inserted_at: DateTime.utc_now
    }}

    tweets = [tweet | tweets]

    {inner.id, {{events, tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}}
  end

  defp map_cast!(%{__struct__: _} = struct) do
    struct
    |> Map.from_struct()
  end

  defp map_cast!(%{} = map) do
    map
  end
end
