Logger.configure(level: :info)
ExUnit.start()

Application.put_env(
  :ecto_cassandra,
  :test_contact_points,
  String.split(System.get_env("CASSANDRA_CONTACT_POINTS") || "127.0.0.1", ",")
)

# Load support files
Code.require_file("./integration/schemas.exs", __DIR__)
Code.require_file("./integration/migration.exs", __DIR__)

alias EctoCassandra.Integration.TestRepo

Application.put_env(:ecto, TestRepo,
  adapter: EctoCassandra.Adapter,
  contact_points: Application.get_env(:ecto_cassandra, :test_contact_points),
  keyspace: "ecto_test",
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1
  ]
)

defmodule EctoCassandra.Integration.TestRepo do
  use Ecto.Repo, otp_app: :ecto
end

defmodule EctoCassandra.Assertions do
  import ExUnit.Assertions, only: [assert: 1]

  def cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} =
      Ecto.Query.Planner.prepare(query, operation, EctoCassandra.Adapter, counter)

    query = Ecto.Query.Planner.normalize(query, operation, EctoCassandra.Adapter, counter)
    apply(EctoCassandra, operation, [query])
  end

  defmacro assert_cql(query, operation \\ :all, cql) do
    quote do
      assert unquote(cql) == cql(unquote(query), unquote(operation))
    end
  end
end

{:ok, _} = Application.ensure_all_started(:ecto)
{:ok, _} = EctoCassandra.Adapter.ensure_all_started(TestRepo, :temporary)

# Load up the repository, start it, and run migrations
_ = EctoCassandra.Adapter.storage_down(TestRepo.config())
:ok = EctoCassandra.Adapter.storage_up(TestRepo.config())

{:ok, _pid} = TestRepo.start_link()

:ok = Ecto.Migrator.up(TestRepo, 0, EctoCassandra.Integration.Migration, log: false)

Process.flag(:trap_exit, true)
