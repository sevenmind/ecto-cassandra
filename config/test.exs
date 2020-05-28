import Config
require Logger

config :ecto_cassandra,
  ecto_repos: [EctoCassandra.Integration.TestRepo],
  test_contact_points:
    String.split(System.get_env("CASSANDRA_CONTACT_POINTS") || "127.0.0.1", ",")

config :ecto_cassandra, EctoCassandra.Integration.TestRepo,
  adapter: EctoCassandra.Adapter,
  contact_points: ["cassandra"],
  keyspace: "ecto_test",
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1
  ]
