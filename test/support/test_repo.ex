defmodule EctoCassandra.Integration.TestRepo do
  use Ecto.Repo, otp_app: :ecto_cassandra, adapter: EctoCassandra.Adapter2
end
