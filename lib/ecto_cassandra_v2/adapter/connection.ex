defmodule EctoCassandra.Adapter2.Connection do
  require Logger

  @behaviour Ecto.Adapters.SQL.Connection

  @impl true
  def child_spec(config) do
    import Supervisor.Spec
    repo = Keyword.get(config, :repo)

    supervisor(repo.__cassandra_repo__, [config])
  end

  # SQL.Connection Callbacks
  # def prepare_execute(conn, name, statement, params, options)
  # def execute(conn, cached, params, options)
  # def stream(conn, statement, params, options)
  # def to_constraints(exception, options)
  # def all(query)
  # def update_all(query)
  # def delete_all(query)
  # def insert(prefix, table, header, rows, on_conflict, returning)
  # def update(prefix, table, fields, filters, returning)
  # def delete(prefix, table, filters, returning)
  # def table_exists_query(table)

  @impl true
  def query(conn, query, params, opts) do
    Logger.info("QUERY")
    IO.inspect(conn, label: "conn")

    IO.inspect(Process.info(conn, [:initial_call, :binary, :links, :dictionary]),
      label: "conn info"
    )

    IO.inspect(query, label: "query")
    IO.inspect(params, label: "params")
    IO.inspect(opts, label: "opts")
    # IO.inspect(Process.info(conn))
    IO.inspect(Supervisor.which_children(conn))

    # Pull Repo name from
    repo =
      conn
      |> Process.info(:dictionary)
      |> List.wrap()
      |> get_in([:dictionary, :"$initial_call"])
      |> elem(1)

    # conn
    # |>

    # pool = conn
    # |> Supervisor.which_children()
    # |> hd
    # |> (fn {_, pid, _, _} -> pid end).()
    # |> Supervisor.which_children()
    # |> hd
    # |> (fn {_, pid, _, _} -> pid end).()
    # |> IO.inspect

    session = Module.concat(repo, Cassandra.Session)
    # pool = conn.pool
    Cassandra.Session.execute(session, query, opts)
    # IO.inspect([conn, sql, params, opts])
    # Postgrex.query(conn, sql, params, opts)
  end

  @impl true
  def execute_ddl(definitions) do
    EctoCassandra.ddl(definitions)
    |> List.wrap()
  end

  @impl true
  def ddl_logs(result) do
    IO.inspect(result)
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
