defmodule Twitter.Tweets do
  import Ecto.Query, warn: false
  alias Ecto.UUID
  alias Twitter.Repo

  use Timex

  alias Twitter.Users.User
  alias Twitter.Places.Place
  alias Twitter.Tweets.Tweet
  alias Twitter.TweetDeletes.TweetDelete

  alias ExTwitter.Model

  alias IO.ANSI

  @max_usage_mb 1024 * 8

  def stream_tweets!() do
    try do
      stream_tweets_imp!
    rescue err ->
      IO.inspect(__STACKTRACE__)
      IO.inspect(err)

      ANSI.format([:black_background, ANSI.format([:red, "[tweets] cycle broken, restarting..."])])
      |> IO.puts

      stream_tweets!
    end
  end

  defp stream_tweets_imp!() do
    ExTwitter.stream_sample([receive_messages: true])
    |> Enum.reduce({{{0, DateTime.utc_now}, {DateTime.utc_now}}, {[], [], {[], [], [], []}, []}, {%{}, %{}, %{}}}, fn (cur, {{{cnt, cpt} = cpt_timer, {last_insert} = insert_timer} = timers, {tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}) ->
      try do
        cpt_diff = DateTime.utc_now
                   |> DateTime.diff(cpt, :second)
                   |> abs

        cpt_timer = if cpt_diff > 60 do
          ANSI.format([:black_background, ANSI.format([:blue, "[tweets] #{cnt} cycles/min"])])
          |> IO.puts

          {0, DateTime.utc_now}
        else
          {cnt + 1, cpt}
        end

        insert_diff = DateTime.utc_now
                      |> DateTime.diff(last_insert, :second)
                      |> abs

        {{last_insert} = insert_timer, {tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups} =
          if insert_diff > 10 do
            insert_timer = {DateTime.utc_now}

            users_w_both = Enum.uniq_by(users_w_both, fn (cur) ->
              cur.twitter_id
            end)

            users_wo_followers = Enum.uniq_by(users_wo_followers, fn (cur) ->
              cur.twitter_id
            end)

            users_wo_following = Enum.uniq_by(users_wo_following, fn (cur) ->
              cur.twitter_id
            end)

            users_wo_both = Enum.uniq_by(users_wo_both, fn (cur) ->
              cur.twitter_id
            end)

            places = Enum.uniq_by(places, fn (cur) ->
              cur.twitter_id_str
            end)

            tweets = Enum.uniq_by(tweets, fn ({depth, cur}) ->
              cur.twitter_id
            end)

            {max_tweet_depth, tweets} = Enum.reduce(tweets, {0, %{}}, fn ({depth, cur}, {max_depth, acc}) ->
              tweets = if acc[depth] === nil do
                []
              else
                acc[depth]
              end

              acc = acc
                    |> Map.put(depth, [cur | tweets])

              {max(depth, max_depth), acc}
            end)

            tweet_deletes = Enum.uniq_by(tweet_deletes, fn (cur) ->
              {cur.twitter_user_id, cur.twitter_tweet_id}
            end)

            Repo.transaction(fn ->
              ANSI.format([:blue_background, ANSI.format([:black, "[tweets] inserting #{length(users_w_both)} users w both followers and following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_w_both, on_conflict: {:replace_all_except, [:num_followers, :num_following, :inserted_at]}, conflict_target: [:twitter_id])

              ANSI.format([:blue_background, ANSI.format([:black, "[tweets] inserting #{length(users_wo_followers)} users w/o any followers"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_followers, on_conflict: {:replace_all_except, [:num_followers, :inserted_at]}, conflict_target: [:twitter_id])

              ANSI.format([:blue_background, ANSI.format([:black, "[tweets] inserting #{length(users_wo_following)} users w/o any following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_following, on_conflict: {:replace_all_except, [:num_following, :inserted_at]}, conflict_target: [:twitter_id])

              ANSI.format([:blue_background, ANSI.format([:black, "[tweets] inserting #{length(users_wo_both)} users w/o either followers nor following"])])
              |> IO.puts

              User
              |> Repo.insert_all(users_wo_both, on_conflict: {:replace_all_except, [:inserted_at]}, conflict_target: [:twitter_id])

              ANSI.format([:yellow_background, ANSI.format([:black, "[tweets] inserting #{length(places)} places"])])
              |> IO.puts

              Place
              |> Repo.insert_all(places, on_conflict: :replace_all, conflict_target: [:twitter_id_str])

              max_tweet_depth..0
              |> Enum.each(fn (cur) ->
                ANSI.format([:green_background, ANSI.format([:black, "[tweets] inserting #{length(tweets[cur])} tweets at depth #{cur}"])])
                |> IO.puts

                Tweet
                |> Repo.insert_all(tweets[cur], on_conflict: :replace_all, conflict_target: [:twitter_id])
              end)

              # ignored for now to save space
              ANSI.format([:black_background, ANSI.format([:green, "[tweets] ignoring #{length(tweet_deletes)} tweet deletes"])])
              |> IO.puts

              # TweetDelete
              # |> Repo.insert_all(tweet_deletes, on_conflict: :replace_all, conflict_target: [:twitter_user_id, :twitter_tweet_id])

              {:ok}
            end, timeout: :infinity)

            %Postgrex.Result {
              rows: [[usage]]
            } = Repo.query!("select pg_database_size('#{System.get_env("DB_NAME")}')")

            initial_usage_mb = div(usage, 1_048_576)

            ANSI.format([:white_background, ANSI.format([:black, "[tweets] db at #{initial_usage_mb} mb"])])
            |> IO.puts

            # TODO 8 gigs
            if initial_usage_mb > @max_usage_mb do
              num_users = from u in User,
                            select: count(u)
              num_tweets = from t in Tweet,
                             select: count(t)

              [num_users_b4] = num_users
                               |> Repo.all()
              [num_tweets_b4] = num_tweets
                                |> Repo.all()

              ANSI.format([:white_background, ANSI.format([:black, "[tweets] cleaning db... (layer 1)"])])
              |> IO.puts

              Repo.transaction(fn ->
                # TODO repeatable read

                good = from t in Tweet,
                         join: u in User, on: t.user_id == u.id,
                         where: not ((is_nil(t.coordinates) and is_nil(t.place_id)) and (is_nil(u.description) or is_nil(u.location))),
                         select: u.id
                # dbg = good
                #       |> Repo.all()
                # IO.puts("#{length(dbg)} good tweets")
                # dbg = dbg
                #       |> Enum.uniq
                # IO.puts("#{length(dbg)} good users")

                bad = from t in Tweet,
                        join: u in User, on: t.user_id == u.id,
                        where: (is_nil(t.coordinates) and is_nil(t.place_id)) and (is_nil(u.description) or is_nil(u.location)),
                        select: u.id
                # dbg = bad
                #       |> Repo.all()
                # IO.puts("#{length(dbg)} bad tweets")
                # dbg = dbg
                #       |> Enum.uniq
                # IO.puts("#{length(dbg)} bad users")

                rm_users = from u in User,
                             where: u.id in subquery(except(bad, ^good))
                {cnt, nil} = rm_users
                             |> Repo.delete_all()

                ANSI.format([:black_background, ANSI.format([:blue, "[tweets] deleted #{cnt} users and their tweets"])])
                |> IO.puts

                rm_tweets = from t in Tweet,
                              join: u in User, on: t.user_id == u.id,
                              where: (is_nil(t.coordinates) and is_nil(t.place_id)) and (is_nil(u.description) or is_nil(u.location))
                              # no location info on the tweet and not enough identifying information from the user
                {cnt, nil} = rm_tweets
                             |> Repo.delete_all()

                ANSI.format([:black_background, ANSI.format([:green, "[tweets] deleted #{cnt} additional tweets"])])
                |> IO.puts

                [num_users_af] = num_users
                                 |> Repo.all()
                [num_tweets_af] = num_tweets
                                  |> Repo.all()

                ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced users by #{Float.round(((num_users_b4 - num_users_af) / num_users_b4) * 100, 2)}%"])])
                |> IO.puts

                ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced tweets by #{Float.round(((num_tweets_b4 - num_tweets_af) / num_tweets_b4) * 100, 2)}%"])])
                |> IO.puts

                {:ok}
              end)

              Repo.query!("vacuum full")

              %Postgrex.Result {
                rows: [[usage]]
              } = Repo.query!("select pg_database_size('#{System.get_env("DB_NAME")}')")

              usage_mb = div(usage, 1_048_576)

              ANSI.format([:white_background, ANSI.format([:black, "[tweets] db now at #{usage_mb} mb (from #{initial_usage_mb} mb)"])])
              |> IO.puts

              if usage_mb > @max_usage_mb do
                ANSI.format([:white_background, ANSI.format([:black, "[tweets] cleaning db... (layer 2)"])])
                |> IO.puts

                Repo.transaction(fn ->
                  good = from t in Tweet,
                           join: u in User, on: t.user_id == u.id,
                           where: not (is_nil(t.coordinates) and is_nil(t.place_id)),
                           select: u.id

                  bad = from t in Tweet,
                          join: u in User, on: t.user_id == u.id,
                          where: is_nil(t.coordinates) and is_nil(t.place_id),
                          select: u.id

                  rm_users = from u in User,
                               where: u.id in subquery(except(bad, ^good))
                  {cnt, nil} = rm_users
                               |> Repo.delete_all()

                  ANSI.format([:black_background, ANSI.format([:blue, "[tweets] deleted #{cnt} users and their tweets"])])
                  |> IO.puts

                  rm_tweets = from t in Tweet,
                                join: u in User, on: t.user_id == u.id,
                                where: is_nil(t.coordinates) and is_nil(t.place_id)
                  {cnt, nil} = rm_tweets
                               |> Repo.delete_all()

                  ANSI.format([:black_background, ANSI.format([:green, "[tweets] deleted #{cnt} additional tweets"])])
                  |> IO.puts

                  [num_users_af] = num_users
                                   |> Repo.all()
                  [num_tweets_af] = num_tweets
                                    |> Repo.all()

                  ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced users by #{Float.round(((num_users_b4 - num_users_af) / num_users_b4) * 100, 2)}%"])])
                  |> IO.puts

                  ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced tweets by #{Float.round(((num_tweets_b4 - num_tweets_af) / num_tweets_b4) * 100, 2)}%"])])
                  |> IO.puts

                  {:ok}
                end)

                Repo.query!("vacuum full")

                %Postgrex.Result {
                  rows: [[usage]]
                } = Repo.query!("select pg_database_size('#{System.get_env("DB_NAME")}')")

                usage_mb = div(usage, 1_048_576)

                ANSI.format([:white_background, ANSI.format([:black, "[tweets] db now at #{usage_mb} mb (from #{initial_usage_mb} mb)"])])
                |> IO.puts

                if usage_mb > @max_usage_mb do
                  ANSI.format([:white_background, ANSI.format([:black, "[tweets] cleaning db... (layer 3)"])])
                  |> IO.puts

                  Repo.transaction(fn ->
                    good = from t in Tweet,
                             join: u in User, on: t.user_id == u.id,
                             where: not is_nil(t.coordinates),
                             select: u.id

                    bad = from t in Tweet,
                            join: u in User, on: t.user_id == u.id,
                            where: is_nil(t.coordinates),
                            select: u.id

                    rm_users = from u in User,
                                 where: u.id in subquery(except(bad, ^good))
                    {cnt, nil} = rm_users
                                 |> Repo.delete_all()

                    ANSI.format([:black_background, ANSI.format([:blue, "[tweets] deleted #{cnt} users and their tweets"])])
                    |> IO.puts

                    rm_tweets = from t in Tweet,
                                  join: u in User, on: t.user_id == u.id,
                                  where: is_nil(t.coordinates)
                    {cnt, nil} = rm_tweets
                                 |> Repo.delete_all()

                    ANSI.format([:black_background, ANSI.format([:green, "[tweets] deleted #{cnt} additional tweets"])])
                    |> IO.puts

                    [num_users_af] = num_users
                                     |> Repo.all()
                    [num_tweets_af] = num_tweets
                                      |> Repo.all()

                    ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced users by #{Float.round(((num_users_b4 - num_users_af) / num_users_b4) * 100, 2)}%"])])
                    |> IO.puts

                    ANSI.format([:white_background, ANSI.format([:black, "[tweets] reduced tweets by #{Float.round(((num_tweets_b4 - num_tweets_af) / num_tweets_b4) * 100, 2)}%"])])
                    |> IO.puts

                    {:ok}
                  end)

                  Repo.query!("vacuum full")

                  %Postgrex.Result {
                    rows: [[usage]]
                  } = Repo.query!("select pg_database_size('#{System.get_env("DB_NAME")}')")

                  usage_mb = div(usage, 1_048_576)

                  ANSI.format([:white_background, ANSI.format([:black, "[tweets] db now at #{usage_mb} mb (from #{initial_usage_mb} mb)"])])
                  |> IO.puts

                  if usage_mb > @max_usage_mb do
                    ANSI.format([:black_background, ANSI.format([:white, "[tweets] db is full"])])
                    |> IO.puts

                    path = System.find_executable("pkill")
                    System.cmd(path, ["beam.smp"])

                    raise "db is full"
                  end
                end
              end
            end

            {insert_timer, {[], [], {[], [], [], []}, []}, {%{}, %{}, %{}}}
          else
            {insert_timer, inserts, lookups}
          end

        case cur do
          %Model.Tweet{} ->
            {_, {inserts, lookups}} = add_tweet!(cur, {inserts, lookups}, 0)

            {{cpt_timer, insert_timer}, inserts, lookups}
          %Model.DeletedTweet{} ->
            tweet_delete = cur.status

            tweet_delete = %{
              id: UUID.generate(),
              twitter_user_id: tweet_delete.user_id,
              twitter_tweet_id: tweet_delete.id,
              inserted_at: DateTime.utc_now
            }

            tweet_deletes = [tweet_delete | tweet_deletes]

            {{cpt_timer, insert_timer}, {tweets, tweet_deletes, users, places}, lookups}
          %Model.Limit{} ->
            ANSI.format([:black_background, ANSI.format([:yellow, "[tweets] got limit on #{cur.track}"])])
            |> IO.puts

            {{cpt_timer, insert_timer}, inserts, lookups}
          %Model.StallWarning{} ->
            ANSI.format([:black_background, ANSI.format([:yellow, "[tweets] got stall warning #{cur.code}, #{cur.message} (#{cur.percent_full})"])])
            |> IO.puts

            {{cpt_timer, insert_timer}, inserts, lookups}
          _ ->
            {{cpt_timer, insert_timer}, inserts, lookups}
        end
      rescue err ->
        IO.inspect(__STACKTRACE__)
        IO.inspect(err)

        ANSI.format([:red_background, ANSI.format([:black, "[tweets] cycle failed, attempting to continue..."])])
        |> IO.puts

        {{{0, DateTime.utc_now}, {DateTime.utc_now}}, {[], [], {[], [], [], []}, []}, {%{}, %{}, %{}}}
      end
    end)

    raise "stream stopped"
  end

  defp add_tweet!(cur, {{tweets, tweet_deletes, {users_w_both, users_wo_followers, users_wo_following, users_wo_both} = users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}, depth) do
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

    {inserts, lookups} = {{tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}

    {is_retweet, {retweeted_tweet_id, {{tweets, tweet_deletes, users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}}} =
      if tweet.raw_data[:retweeted_status] !== nil do
        result = tweet.raw_data.retweeted_status
                 |> Map.put(:raw_data, tweet.raw_data.retweeted_status)
                 |> add_tweet!({inserts, lookups}, depth + 1)

        {true, result}
      else
        {false, {nil, {inserts, lookups}}}
      end

    {inserts, lookups} = {{tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}

    {quoted_tweet_id, {{tweets, tweet_deletes, users, places} = inserts, {user_id_lookup, place_id_lookup, tweet_id_lookup} = lookups}} =
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
      inserted_at: DateTime.utc_now
    }}

    tweets = [tweet | tweets]

    {inner.id, {{tweets, tweet_deletes, users, places}, {user_id_lookup, place_id_lookup, tweet_id_lookup}}}
  end

  defp map_cast!(%{__struct__: _} = struct) do
    struct
    |> Map.from_struct()
  end

  defp map_cast!(%{} = map) do
    map
  end
end
