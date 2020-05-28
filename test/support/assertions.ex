defmodule EctoCassandra.Assertions do
  import ExUnit.Assertions, only: [assert: 1]

  def cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} =
      Ecto.Query.Planner.prepare(query, operation, EctoCassandra.Adapter, counter)

    query = Ecto.Query.Planner.normalize(query, operation, EctoCassandra.Adapter, counter)
    apply(EctoCassandra, operation, [query])
  end

  defmacro assert_cql(query, operation \\ :all, cql) do
    quote do
      assert unquote(cql) == cql(unquote(query), unquote(operation))
    end
  end
end
