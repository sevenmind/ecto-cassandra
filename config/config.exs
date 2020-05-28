import Config
require Logger

if File.exists?("config/#{Mix.env()}.exs") do
  Logger.info("Loading mix config for #{Mix.env()}")
  import_config "#{Mix.env()}.exs"
end
