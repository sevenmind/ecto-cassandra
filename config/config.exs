import Config
require Logger

# config :micro_segmentation,
#   ecto_repos: [MicroSegmentation.Repo]

Logger.info("Loading mix config for #{Mix.env()}")

if File.exists?("#{Mix.env()}.exs") do
  import_config "#{Mix.env()}.exs"
end
