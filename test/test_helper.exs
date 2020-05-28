Logger.configure(level: :info)
ExUnit.start()

alias EctoCassandra.Integration.TestRepo

# Application.put_env(:ecto_cassandra, TestRepo,
#   adapter: EctoCassandra.Adapter,
#   contact_points: Application.get_env(:ecto_cassandra, :test_contact_points),
#   keyspace: "ecto_test",
#   replication: [
#     class: "SimpleStrategy",
#     replication_factor: 1
#   ]
# )

# Code.e
:ok = Application.ensure_loaded(:ecto_cassandra)
Application.get_all_env(:ecto_cassandra)

{:ok, _} = Application.ensure_all_started(:cassandra)
{:ok, _} = Application.ensure_all_started(:ecto)
{:ok, _} = Application.ensure_all_started(:ecto_cassandra)
# IO.inspect(TestRepo.config())
{:ok, _} = EctoCassandra.Adapter.ensure_all_started(TestRepo, :temporary)

# Load up the repository, start it, and run migrations
_ = EctoCassandra.Adapter.storage_down(TestRepo.config())
:ok = EctoCassandra.Adapter.storage_up(TestRepo.config())

{:ok, _pid} = TestRepo.start_link()

:ok = Ecto.Migrator.up(TestRepo, 0, EctoCassandra.Integration.Migration, log: false)



Process.flag(:trap_exit, true)
