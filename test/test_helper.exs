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
:ok = Application.ensure_loaded(:ecto_xandra)
Application.get_all_env(:ecto_xandra)

# {:ok, _} = Application.ensure_all_started(:cassandra)
{:ok, _} = Application.ensure_all_started(:ecto)
{:ok, _} = Application.ensure_all_started(:ecto_sql)
{:ok, _} = Application.ensure_all_started(:ecto_xandra)
# IO.inspect(TestRepo.config())
{:ok, _} = EctoXandra.Adapter.ensure_all_started(TestRepo, :temporary)

IO.inspect(TestRepo.config())
# Load up the repository, start it, and run migrations
_ = EctoXandra.Adapter.storage_down(TestRepo.config())
Process.sleep(300)
:ok = EctoXandra.Adapter.storage_up(TestRepo.config())
Process.sleep(300)

IO.puts("start test repo")
{:ok, _pid} = TestRepo.start_link()
Process.sleep(300)

IO.puts("Run migrations")
# :ok = Ecto.Migrator.up(TestRepo, 0, EctoCassandra.Integration.Migration)
IO.puts("Run the Tests!")

Process.flag(:trap_exit, true)
