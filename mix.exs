defmodule EctoCassandra.Mixfile do
  use Mix.Project

  def project,
    do: [
      app: :ecto_cassandra,
      version: "1.1.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.travis": :test
      ],
      source_url: "https://github.com/cafebazaar/ecto-cassandra",
      description: "Cassandra Adapter for Ecto",
      package: package(),
      deps: deps()
    ]

  def application,
    do: [
      applications: [:logger, :cassandra]
    ]

  defp deps,
    do: [
      {:ecto, "~> 3.0"},
      {:ecto_sql, "~> 3.0"},
      {:cassandra, github: "sevenmind/elixir-cassandra"},
      {:excoveralls, "~> 0.6", only: :test},
      {:ex_doc, "~> 0.18", only: :dev},
      {:lz4, "~> 0.2.2", hex: :lz4_erl, override: true}
    ]

  defp package,
    do: [
      licenses: ["Apache 2.0"],
      maintainers: ["Ali Rajabi", "Hassan Zamani"],
      links: %{
        "Github" => "https://github.com/cafebazaar/ecto-cassandra"
      },
      files: ~w(mix.exs lib README.md LICENSE.md)
    ]

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
