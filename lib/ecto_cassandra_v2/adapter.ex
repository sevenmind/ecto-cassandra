defmodule EctoCassandra.Adapter2 do
  @moduledoc """
  Adapter module for Cassandra.

  It uses `Cassandra` for communicating to the database.


  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :ecto_cassandra

  # And provide a custom storage implementation
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  # @default_maintenance_database "postgres"

  @impl true
  def dumpers(:utc_datetime, _type), do: [&to_naive/1]
  def dumpers(:naive_datetime, _type), do: [&to_naive/1]
  def dumpers(_primitive, type), do: [type]

  ## Storage API

  @impl true
  def storage_up(options) do
    options = Keyword.put(options, :on_coordinator, true)

    command =
      options
      |> Keyword.put(:if_not_exists, true)
      |> EctoCassandra.create_keyspace()

    case run_query(command, options) do
      {:ok, _} ->
        :ok

      {:error, %{postgres: %{code: :duplicate_database}}} ->
        {:error, :already_up}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp concat_if(content, nil, _fun), do: content
  defp concat_if(content, value, fun), do: content <> " " <> fun.(value)

  @impl true
  def storage_down(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    command = "DROP DATABASE \"#{database}\""
    maintenance_database = Keyword.get(opts, :maintenance_database, @default_maintenance_database)
    opts = Keyword.put(opts, :database, maintenance_database)

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{postgres: %{code: :invalid_catalog_name}}} ->
        {:error, :already_down}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    maintenance_database = Keyword.get(opts, :maintenance_database, @default_maintenance_database)
    opts = Keyword.put(opts, :database, maintenance_database)

    check_database_query =
      "SELECT datname FROM pg_catalog.pg_database WHERE datname = '#{database}'"

    case run_query(check_database_query, opts) do
      {:ok, %{num_rows: 0}} -> :down
      {:ok, %{num_rows: _num_rows}} -> :up
      other -> {:error, other}
    end
  end

  @impl true
  def supports_ddl_transaction? do
    true
  end

  @impl true
  def structure_dump(default, config) do
    table = config[:migration_source] || "schema_migrations"

    with {:ok, versions} <- select_versions(table, config),
         {:ok, path} <- pg_dump(default, config),
         do: append_versions(table, versions, path)
  end

  defp select_versions(table, config) do
    case run_query(~s[SELECT version FROM public."#{table}" ORDER BY version], config) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &hd/1)}
      {:error, %{postgres: %{code: :undefined_table}}} -> {:ok, []}
      {:error, _} = error -> error
    end
  end

  defp pg_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    File.mkdir_p!(Path.dirname(path))

    case run_with_cmd("pg_dump", config, [
           "--file",
           path,
           "--schema-only",
           "--no-acl",
           "--no-owner",
           config[:database]
         ]) do
      {_output, 0} ->
        {:ok, path}

      {output, _} ->
        {:error, output}
    end
  end

  defp append_versions(_table, [], path) do
    {:ok, path}
  end

  defp append_versions(table, versions, path) do
    sql =
      ~s[INSERT INTO public."#{table}" (version) VALUES ] <>
        Enum.map_join(versions, ", ", &"(#{&1})") <> ~s[;\n\n]

    File.open!(path, [:append], fn file ->
      IO.write(file, sql)
    end)

    {:ok, path}
  end

  @impl true
  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")

    args = [
      "--quiet",
      "--file",
      path,
      "-vON_ERROR_STOP=1",
      "--single-transaction",
      config[:database]
    ]

    case run_with_cmd("psql", config, args) do
      {_output, 0} -> {:ok, path}
      {output, _} -> {:error, output}
    end
  end

  ## Helpers

  defp run_query(cql, opts) do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:cassandra)

    host =
      opts |> Keyword.get(:contact_points) |> Enum.random()

    opts =
      opts
      # from postgrex, to remove
      |> Keyword.drop([:name, :log, :pool, :pool_size])
      |> Keyword.put(:backoff_type, :stop)
      |> Keyword.put(:max_restarts, 0)

    task =
      Task.Supervisor.async_nolink(Ecto.Adapters.SQL.StorageSupervisor, fn ->
        Cassandra.Connection.run_query(host, cql, opts)
      end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:error, error}} ->
        {:error, error}

      {:ok, result} ->
        {:ok, result}

      {:exit, {%{__struct__: struct} = error, _}}
      when struct in [Cassandra.ConnectionError, DBConnection.Error] ->
        {:error, error}

      {:exit, reason} ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end

  defp run_with_cmd(cmd, opts, opt_args) do
    unless System.find_executable(cmd) do
      raise "could not find executable `#{cmd}` in path, " <>
              "please guarantee it is available before running ecto commands"
    end

    env = [{"PGCONNECT_TIMEOUT", "10"}]

    env =
      if password = opts[:password] do
        [{"PGPASSWORD", password} | env]
      else
        env
      end

    args = []
    args = if username = opts[:username], do: ["-U", username | args], else: args
    args = if port = opts[:port], do: ["-p", to_string(port) | args], else: args

    host = opts[:hostname] || System.get_env("PGHOST") || "localhost"
    args = ["--host", host | args]
    args = args ++ opt_args
    System.cmd(cmd, args, env: env, stderr_to_stdout: true)
  end

  # --- NOT FROM POSTGREX ---

  defp to_naive(%NaiveDateTime{} = datetime), do: {:ok, datetime}
  defp to_naive(%DateTime{} = datetime), do: {:ok, DateTime.to_naive(datetime)}
  defp to_naive(_), do: :error

   ### Ecto.Adapter Callbacks ###

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      defmodule CassandraRepo do
        use Cassandra
      end

      defdelegate execute(statement, options), to: CassandraRepo
      # defdelegate ddl_logs(results), to: EctoCassandra.Adapter

      def __cassandra_repo__, do: CassandraRepo
    end
  end

end
