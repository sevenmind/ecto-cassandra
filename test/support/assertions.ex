defmodule EctoXandra.Assertions do
  import ExUnit.Assertions, only: [assert: 1]

  def cql(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.plan(query, operation, EctoXandra.Adapter)

    {query, _select} = Ecto.Query.Planner.normalize(query, operation, EctoXandra.Adapter, counter)

    apply(EctoXandra.Adapter, operation, [query])
  end

  defmacro assert_cql(query, operation \\ :all, cql) do
    quote location: :keep do
      assert unquote(cql) == cql(unquote(query), unquote(operation))
    end
  end
end
