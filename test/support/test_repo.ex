defmodule EctoCassandra.Integration.TestRepo do
  use Ecto.Repo, otp_app: :ecto_xandra, adapter: EctoXandra.Adapter
end
