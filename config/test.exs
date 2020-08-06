import Config
require Logger

config :ecto_xandra,
  ecto_repos: [EctoCassandra.Integration.TestRepo]

# config :ecto_cassandra, EctoCassandra.Integration.TestRepo,
#   adapter: EctoCassandra.Adapter2,
#   contact_points: ["cassandra"],
#   keyspace: "ecto_test",
#   replication: [
#     class: "SimpleStrategy",
#     replication_factor: 1
#   ]

config :ecto_xandra, EctoCassandra.Integration.TestRepo,
  adapter: EctoXandra.Adapter,
  nodes: ["cassandra"],
  keyspace: "ecto_test",
  replication: [
    class: "SimpleStrategy",
    replication_factor: 1
  ],
  autodiscovery: false,
  pool_size: 5
