defmodule EctoCassandra.Adapter2.Connection do
  @behaviour Ecto.Adapters.SQL.Connection

  def child_spec(config) do
    import Supervisor.Spec
    repo = Keyword.get(config, :repo)

    supervisor(repo.__cassandra_repo__, [config])
  end

  @impl true
  def execute_ddl(definitions) do
    EctoCassandra.ddl(definitions)
  end


  @impl true
  def query(conn, query, _params, opts) do
    IO.inspect(Process.info(conn))
    IO.inspect(Supervisor.which_children(conn))
    Cassandra.Session.execute(conn, query, opts)
    # IO.inspect([conn, sql, params, opts])
    # Postgrex.query(conn, sql, params, opts)
  end

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
