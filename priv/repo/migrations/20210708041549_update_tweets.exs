defmodule Twitter.Repo.Migrations.UpdateTweets do
  use Ecto.Migration

  def change do
    alter table(:tweets) do
      add :translated, :text
    end
  end
end
