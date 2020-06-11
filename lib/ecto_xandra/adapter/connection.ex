defmodule EctoXandra.Adapter.Connection do
  require Logger

  @behaviour Ecto.Adapters.SQL.Connection

  @impl true
  def child_spec(config) do
    import Supervisor.Spec
    # repo = Keyword.get(config, :repo)
    IO.inspect(config)

    keyspace = config[:keyspace]

    config =
      config
      |> Keyword.put(:after_connect, &Xandra.execute(&1, "USE #{keyspace}"))

    supervisor(Xandra.Cluster, [config])
  end

  # SQL.Connection Callbacks
  def prepare_execute(cluster, name, statement, params, options) do
    Xandra.Cluster.run(cluster, fn conn ->
      # "conn" is the pool of connections for a specific node.
      prepared = Xandra.prepare!(conn, statement)
      Xandra.execute(conn, prepared, params, options)
    end)
    |> case do
      {:ok, res} ->
        {:ok, statement, xandra_res_transform(res)}

      other ->
        other
    end

    # Xandra.Cluster.prepare_execute
  end

  def execute(conn, prepared, params, options) do
    Xandra.Cluster.execute(conn, prepared, params, options)
    |> case do
      {:ok, %Xandra.Void{}} ->
        {:ok, prepared, []}

      {:ok, res} ->
        {:ok, prepared, xandra_res_transform(res)}

      other ->
        other
    end
  end

  # defdelegate execute(cluster, query, params, opts), to: Xandra.Cluster
  defdelegate prepare(cluster, query, params, opts), to: Xandra.Cluster
  # def execute(conn, cached, params, options)
  # def stream(conn, statement, params, options)
  # def to_constraints(exception, options)

  defdelegate all(query), to: EctoCassandra
  defdelegate update_all(query), to: EctoCassandra
  defdelegate delete_all(query), to: EctoCassandra

  def insert(prefix, table, headers, rows, on_conflict, returning) do
    insert(prefix, table, rows, [], [], headers)
  end

  # def update(prefix, table, fields, filters, returning)
  # def delete(prefix, table, filters, returning)
  # def table_exists_query(table)

  @impl true
  def query(conn, query, params, opts) do
    Xandra.Cluster.execute(conn, query, params, opts)
  end

  @impl true
  def execute_ddl(definitions) do
    EctoCassandra.ddl(definitions)
  end

  def ddl_logs({:ok, log}), do: ddl_logs(log)
  def ddl_logs({:ok, _, log}), do: ddl_logs(log)

  @impl true
  def ddl_logs(%Xandra.SchemaChange{
        effect: effect,
        options: options,
        target: target,
        tracing_id: tracing_id
      }) do
    [
      {:info, "#{target} #{options[:keyspace]}.#{options[:subject]}, #{effect}",
       [tracing_id: tracing_id]}
    ]
  end

  def ddl_logs(%Xandra.SetKeyspace{
        keyspace: keyspace,
        tracing_id: tracing_id
      }) do
    [
      {:info, "using keyspace #{keyspace}", [tracing_id: tracing_id]}
    ]
  end

  def ddl_logs(%Xandra.Void{}), do: []
  def ddl_logs(%Xandra.Error{} = err), do: [{:warn, err.message, []}]

  def ddl_logs(%{messages: messages}) do
    for message <- messages do
      %{message: message, severity: severity} = message

      {severity, message, []}
    end
  end

  def ddl_logs({:error, message}), do: [{:error, message, []}]
  def ddl_logs(_), do: []

  defp xandra_res_transform(%Xandra.Void{}), do: %{rows: [], num_rows: 0}

  defp xandra_res_transform(%Xandra.Page{} = res),
    do:
      res
      |> Map.from_struct()
      |> Map.put(:rows, res.content)
      |> Map.put(:num_rows, length(res.content || []))

  defp xandra_res_transform(res), do: res
end
