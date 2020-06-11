defmodule EctoCassandra.Integration.TestRepo do
  use Ecto.Repo, otp_app: :ecto_app, adapter: EctoXandra.Adapter
end
