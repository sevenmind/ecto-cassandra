defmodule EctoCassandra.Adapter do
  @moduledoc """
  Ecto Adapter for Apache Cassandra.

  It uses `cassandra` for communicating to the database
  """

  use EctoCassandra.Adapter.Base

  # @behaviour Ecto.Adapter
  # @behaviour Ecto.Adapter.Migration
  @behaviour Ecto.Adapter.Storage
  # @behaviour Ecto.Adapter.Queryable
  # @behaviour Ecto.Adapter.Schema

  @host_tries 3
  # @conn Cassandra.Session
  @pool_opts [:timeout, :pool, :pool_size, :migration_lock] ++
               [:queue_target, :queue_interval, :ownership_timeout]

  ### Ecto.Adapter callbacks
  def init(config) do
    log = Keyword.get(config, :log, :debug)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry = {config[:repo], log, telemetry_prefix ++ [:query]}

    config = adapter_config(config)
    opts = Keyword.take(config, @pool_opts)
    meta = %{telemetry: telemetry, opts: opts}

    repo = Keyword.get(config, :repo)

    {:ok, child_spec(repo, config), meta}
  end

  ### Ecto.Adapter.Migration Callbacks ###

  @doc false

  def execute_ddl(meta, definitions, options) do
    options = Keyword.put(options, :on_coordinator, true)
    # cql = EctoCassandra.ddl(definitions)

    # IO.inspect([repo])
    # IO.inspect([definitions])
    # IO.inspect([options])

    # case exec_and_log(repo, cql, options) do
    #   %CQL.Result.SchemaChange{} -> :ok
    #   %CQL.Result.Void{} -> :ok
    #   error -> raise error
    # end

    repo = Map.get(meta, :repo)

    ddl_logs =
      definitions
      |> EctoCassandra.ddl()
      |> List.wrap()
      |> Enum.map(&repo.execute(&1, options))
      |> Enum.flat_map(&repo.ddl_logs/1)

    {:ok, ddl_logs}
  end

  @doc false
  def supports_ddl_transaction?, do: false

  ### Ecto.Adapter.Storage Callbacks ###

  @doc false
  def storage_up(options) do
    options = Keyword.put(options, :on_coordinator, true)

    cql =
      options
      |> Keyword.put(:if_not_exists, true)
      |> EctoCassandra.create_keyspace()

    case run_query(cql, options) do
      %CQL.Result.SchemaChange{change_type: "CREATED", target: "KEYSPACE"} ->
        :ok

      %CQL.Result.Void{} ->
        {:error, :already_up}

      error ->
        {:error, Exception.message(error)}
    end
  end

  @doc false
  def storage_down(options) do
    options = Keyword.put(options, :on_coordinator, true)

    cql =
      options
      |> Keyword.put(:if_exists, true)
      |> EctoCassandra.drop_keyspace()

    case run_query(cql, options) do
      %CQL.Result.SchemaChange{change_type: "DROPPED", target: "KEYSPACE"} ->
        :ok

      %CQL.Result.Void{} ->
        {:error, :already_down}

      error ->
        {:error, Exception.message(error)}
    end
  end

  ### Ecto.Adapter Callbacks ###

  @doc false
  defmacro __before_compile__(_env) do
    quote location: :keep do
      defmodule CassandraRepo do
        use Cassandra
      end

      defdelegate execute(statement, options), to: CassandraRepo
      # defdelegate ddl_logs(results), to: EctoCassandra.Adapter

      def __cassandra_repo__, do: CassandraRepo

      # def ddl_logs
    end
  end

  @doc false
  def child_spec(repo, options) do
    import Supervisor.Spec
    supervisor(repo.__cassandra_repo__, [options])
  end

  @doc false
  def ensure_all_started(_repo, _type) do
    Application.ensure_all_started(:cassandra)
  end

  @doc false
  def execute(repo, %{fields: fields} = meta, query, params, process, options) do
    [cql, options] = super(repo, meta, query, params, process, options)

    case exec_and_log(repo, cql, options) do
      %CQL.Result.Rows{rows_count: count, rows: rows} ->
        {count, Enum.map(rows, &process_row(&1, fields, process))}

      %CQL.Result.Void{} ->
        :ok

      error ->
        raise error
    end
  end

  @doc false
  def insert(repo, meta, fields, on_conflict, autogenerate, options) do
    args = super(repo, meta, fields, on_conflict, autogenerate, options)
    apply(&exec/4, args)
  end

  @doc false
  def insert_all(repo, meta, header, list, on_conflict, returning, options) do
    args = super(repo, meta, header, list, on_conflict, returning, options)
    apply(&exec/4, args)
  end

  @doc false
  def update(repo, meta, fields, filters, returning, options) do
    args = super(repo, meta, fields, filters, returning, options)
    apply(&exec/3, args)
  end

  @doc false
  def delete(repo, meta, filters, options) do
    args = super(repo, meta, filters, options)
    apply(&exec/3, args)
  end

  ### Helpers ###

  defp run_query(cql, options) do
    options
    |> Keyword.get(:contact_points, [])
    |> List.duplicate(@host_tries)
    |> List.flatten()
    |> Stream.map(&Cassandra.Connection.run_query(&1, cql, options))
    |> Stream.reject(&match?(%Cassandra.ConnectionError{}, &1))
    |> Enum.to_list()
    |> Enum.take(1)
    |> case do
      [result] -> result
      [] -> raise RuntimeError, "connections refused"
    end
  end

  defp exec(repo, cql, options, on_conflict \\ :error) do
    case exec_and_log(repo, cql, options) do
      %CQL.Result.Void{} ->
        {:ok, []}

      %CQL.Result.Rows{rows_count: 1, rows: [[false | _]], columns: ["[applied]" | _]} ->
        if on_conflict == :nothing do
          {:ok, []}
        else
          {:error, :stale}
        end

      %CQL.Result.Rows{} ->
        {:ok, []}

      error ->
        raise error
    end
  end

  defp exec_and_log(repo, cql, options) do
    if Keyword.get(options, :log, true) do
      repo.execute(cql, Keyword.put(options, :log, &log(repo, cql, &1)))
    else
      repo.execute(cql, Keyword.delete(options, :log))
    end
  end

  defp log(repo, cql, entry) do
    %{
      connection_time: query_time,
      decode_time: decode_time,
      pool_time: queue_time,
      result: result,
      query: query
    } = entry

    repo.__log__(%Ecto.LogEntry{
      query_time: query_time,
      decode_time: decode_time,
      queue_time: queue_time,
      result: log_result(result),
      params: Map.get(query, :values, []),
      query: String.Chars.to_string(cql)
    })
  end

  defp log_result({:ok, _query, res}), do: {:ok, res}
  defp log_result(other), do: other

  defp adapter_config(config) do
    if Keyword.has_key?(config, :pool_timeout) do
      message = """
      :pool_timeout option no longer has an effect and has been replaced with an improved queuing system.
      See \"Queue config\" in DBConnection.start_link/2 documentation for more information.
      """

      IO.warn(message)
    end

    config
    |> Keyword.delete(:name)
    |> Keyword.update(:pool, DBConnection.ConnectionPool, &normalize_pool/1)
  end

  defp normalize_pool(pool) do
    if Code.ensure_loaded?(pool) && function_exported?(pool, :unboxed_run, 2) do
      DBConnection.Ownership
    else
      pool
    end
  end

  def ddl_logs(result) do
    # case result do
    #   error
    # end
    %{messages: messages} = result

    for message <- messages do
      %{message: message, severity: severity} = message

      {severity, message, []}
    end
  end
end
