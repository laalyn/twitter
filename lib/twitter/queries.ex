defmodule Twitter.Queries do
  import Ecto.Query, warn: false
  alias Ecto.UUID
  alias Twitter.Repo

  alias Twitter.Users.User
  alias Twitter.Places.Place
  alias Twitter.Tweets.Tweet

  alias IO.ANSI

  @supported_languages ["af", "am", "ar", "ast", "az", "ba", "be", "bg", "bn", "br", "bs", "ca", "ca", "ceb", "cs", "cy", "da", "de", "el", "en", "es", "et", "fa", "ff", "fi", "fr", "fy", "ga", "gd", "gd", "gl", "gu", "ha", "he", "hi", "hr", "ht", "ht", "hu", "hy", "id", "ig", "ilo", "is", "it", "ja", "jv", "ka", "kk", "km", "km", "kn", "ko", "lb", "lb", "lg", "ln", "lo", "lt", "lv", "mg", "mk", "ml", "mn", "mr", "ms", "my", "ne", "nl", "nl", "no", "ns", "oc", "or", "pa", "pa", "pl", "ps", "ps", "pt", "ro", "ro", "ro", "ru", "sd", "si", "si", "sk", "sl", "so", "sq", "sr", "ss", "su", "sv", "sw", "ta", "th", "tl", "tn", "tr", "uk", "ur", "uz", "vi", "wo", "xh", "yi", "yo", "zh", "zu"]

  def countries!() do
    %Postgrex.Result{rows: countries} =
    "select country_code, array_agg(distinct country_name), count(*) from tweets\n" <>
    "join places on tweets.place_id = places.id\n" <>
    "where not country_code = ''\n" <>
    "group by country_code\n" <>
    "order by count desc\n" <>
    "limit 100"
    |> Repo.query!

    uuid = UUID.generate()

    {_, result} = Enum.reduce(countries, {1, %{}}, fn ([code, names, _], {i, acc}) ->
      ANSI.format([:green_background, ANSI.format([:black, "[queries] country #{i}/100"])])
      |> IO.puts

      locations = Enum.reduce(names, [], fn (cur, acc) ->
        if String.contains?(cur, "%") do
          ANSI.format([:black_background, ANSI.format([:yellow, "[queries] excluding '#{cur}'"])])
          |> IO.puts

          acc
        else
          ["%#{cur}%" | acc]
        end
      end)

      tweets_query = from t in Tweet,
                       left_join: p in Place, on: t.place_id == p.id,
                       join: u in User, on: t.user_id == u.id,
                       select: t,
                       where: (t.lang in @supported_languages and not is_nil(t.text)) and ((p.country_code == ^code) or fragment("(? ilike any (?))", u.location, ^locations)),
                       order_by: fragment("random()"),
                       limit: 1000 # TODO unlimited
      tweets = tweets_query
               |> Repo.all(timeout: :infinity)

      # agg = Enum.reduce(tweets, "", fn (cur, acc) ->
      #   acc <> cur <> "\n\n"
      # end)

      agg = Enum.map(tweets, fn (cur) ->
        %{
          lang: cur.lang,
          text: cur.text,
        }
      end)

      acc = acc
            |> Map.put(code, agg)

      file = File.open!("agg/#{uuid}countries-substep.json", [:write])
      file
      |> IO.binwrite(Jason.encode!(acc))
      File.close(file)

      {i + 1, acc}
    end)
    |> Jason.encode!

    file = File.open!("agg/#{uuid}countries.json")
    file
    |> IO.binwrite(result)
    File.close(file)

    :ok
  end
end
