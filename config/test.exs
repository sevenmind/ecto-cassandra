import Config
require Logger

config :ecto_cassandra,
  test_contact_points:
    String.split(System.get_env("CASSANDRA_CONTACT_POINTS") || "127.0.0.1", ",")

config :ecto, EctoCassandra.Integration.TestRepo,
  adapter: EctoCassandra.Adapter,
  contact_points: Application.get_env(:ecto_cassandra, :test_contact_points),
  keyspace: "ecto_test",
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1
  ]
