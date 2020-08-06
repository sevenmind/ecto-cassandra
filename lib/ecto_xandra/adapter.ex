defmodule EctoXandra.Adapter do
  @moduledoc """
  Adapter module for Cassandra.

  It uses `Cassandra` for communicating to the database.


  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL,
    driver: :ecto_xandra

  alias EctoXandra.Adapter.Connection

  require Logger
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
    Logger.info("STORAGE UP")
    options = Keyword.put(options, :on_coordinator, true)

    # IO.inspect(options)

    command =
      options
      |> Keyword.put(:if_not_exists, true)
      |> EctoCassandra.create_keyspace()

    case run_query(command, options) do
      {:ok, %CQL.Result.Void{}} ->
        {:error, :already_up}

      {:ok, _} ->
        :ok

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @impl true
  def storage_down(options) do
    Logger.info("STORAGE DOWN")
    options = Keyword.put(options, :on_coordinator, true)
    # IO.inspect(options)

    cql =
      options
      |> Keyword.put(:if_exists, true)
      |> EctoCassandra.drop_keyspace()

    case run_query(cql, options) do
      {:ok, %CQL.Result.Void{}} ->
        {:error, :already_down}

      {:ok, _} ->
        :ok

      error ->
        {:error, Exception.message(error)}
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(opts) do
    Logger.warn("storage_status is not implemented")
    # database =
    #   Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    # maintenance_database = Keyword.get(opts, :maintenance_database, @default_maintenance_database)
    # opts = Keyword.put(opts, :database, maintenance_database)

    # # TODO!!
    # check_database_query =
    #   "SELECT datname FROM pg_catalog.pg_database WHERE datname = '#{database}'"

    # case run_query(check_database_query, opts) do
    #   {:ok, %{num_rows: 0}} -> :down
    #   {:ok, %{num_rows: _num_rows}} -> :up
    #   other -> {:error, other}
    # end
  end

  # @impl true
  # def structure_dump(default, config) do
  #   table = config[:migration_source] || "schema_migrations"

  #   with {:ok, versions} <- select_versions(table, config),
  #        {:ok, path} <- pg_dump(default, config),
  #        do: append_versions(table, versions, path)
  # end

  # defp select_versions(table, config) do
  #   case run_query(~s[SELECT version FROM public."#{table}" ORDER BY version], config) do
  #     {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &hd/1)}
  #     {:error, %{postgres: %{code: :undefined_table}}} -> {:ok, []}
  #     {:error, _} = error -> error
  #   end
  # end

  # defp pg_dump(default, config) do
  #   path = config[:dump_path] || Path.join(default, "structure.sql")
  #   File.mkdir_p!(Path.dirname(path))

  #   case run_with_cmd("pg_dump", config, [
  #          "--file",
  #          path,
  #          "--schema-only",
  #          "--no-acl",
  #          "--no-owner",
  #          config[:database]
  #        ]) do
  #     {_output, 0} ->
  #       {:ok, path}

  #     {output, _} ->
  #       {:error, output}
  #   end
  # end

  # defp append_versions(_table, [], path) do
  #   {:ok, path}
  # end

  # defp append_versions(table, versions, path) do
  #   sql =
  #     ~s[INSERT INTO public."#{table}" (version) VALUES ] <>
  #       Enum.map_join(versions, ", ", &"(#{&1})") <> ~s[;\n\n]

  #   File.open!(path, [:append], fn file ->
  #     IO.write(file, sql)
  #   end)

  #   {:ok, path}
  # end

  # @impl true
  # def structure_load(default, config) do
  #   path = config[:dump_path] || Path.join(default, "structure.sql")

  #   args = [
  #     "--quiet",
  #     "--file",
  #     path,
  #     "-vON_ERROR_STOP=1",
  #     "--single-transaction",
  #     config[:database]
  #   ]
  # Ecto

  defdelegate all(query), to: Connection
  defdelegate update_all(query), to: Connection
  defdelegate delete_all(query), to: Connection
  ## Helpers

  defp run_query(cql, opts) do
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:xandra)

    host = opts |> Keyword.get(:nodes) |> Enum.random()

    opts =
      opts
      |> Keyword.take([:authentication, :keyspace])
      |> Keyword.put(:nodes, [host])

    task =
      Task.Supervisor.async_nolink(Ecto.Adapters.SQL.StorageSupervisor, fn ->
        {:ok, conn} = Xandra.start_link(opts)
        res = Xandra.execute(conn, cql)
        GenServer.stop(conn)

        res
      end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:error, error}} ->
        {:error, error}

      {:ok, result} ->
        {:ok, result}

      {:exit, {%{__struct__: struct} = error, _}}
      when struct in [DBConnection.Error] ->
        {:error, error}

      {:exit, reason} ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end

  # --- NOT FROM POSTGREX ---

  defp to_naive(%NaiveDateTime{} = datetime), do: {:ok, datetime}
  defp to_naive(%DateTime{} = datetime), do: {:ok, DateTime.to_naive(datetime)}
  defp to_naive(_), do: :error

  ### Ecto.Adapter.Migrations Callbacks ###options

  # def execute_ddl(meta, definitions, options) do
  #   ddl_logs =
  #     definitions
  #     |> Connection.execute_ddl()
  #     |> List.wrap()
  #     |> Enum.map(&Connection.execute(@conn, &1, [], options))
  #     |> Enum.flat_map(&Connection.ddl_logs/1)

  #   {:ok, ddl_logs}
  # end

  @doc false
  @impl true
  def supports_ddl_transaction?, do: false
end
