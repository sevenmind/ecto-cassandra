defmodule EctoCassandra.Query do
  @types [
    :ascii,
    :bigint,
    :blob,
    :boolean,
    :counter,
    :date,
    :decimal,
    :double,
    :float,
    :inet,
    :int,
    :smallint,
    :text,
    :time,
    :timestamp,
    :timeuuid,
    :tinyint,
    :uuid,
    :varchar,
    :varint
  ]

  @cassandra_keys [
    :if,
    :allow_filtering,
    :using
  ]

  defmacro __using__([]) do
    quote location: :keep do
      import Ecto.Query, except: [from: 1, from: 2]
      import EctoCassandra.Query
    end
  end

  defmacro from(expr, kw \\ []) do
    cassandra_kw = Keyword.take(kw, @cassandra_keys)
    ecto_kw = Keyword.drop(kw, @cassandra_keys)

    quote location: :keep do
      unquote(expr)
      |> Ecto.Query.from(unquote(ecto_kw))
      |> Map.merge(Enum.into(unquote(cassandra_kw), %{}))
    end
  end

  defmacro token(fields) when is_list(fields) do
    marks = Enum.map_join(fields, ", ", fn _ -> "?" end)
    quote location: :keep, do: fragment(unquote("token(#{marks})"), unquote_splicing(fields))
  end

  defmacro token(field) do
    quote location: :keep, do: fragment("token(?)", unquote(field))
  end

  defmacro cast(field, type) when type in @types do
    fragment = "cast(? as #{Atom.to_string(type)})"
    quote location: :keep, do: fragment(unquote(fragment), unquote(field))
  end

  defmacro uuid do
    quote location: :keep, do: fragment("uuid()")
  end

  defmacro now do
    quote location: :keep, do: fragment("now()")
  end

  defmacro min_timeuuid(time) do
    quote location: :keep, do: fragment("minTimeuuid(?)", unquote(time))
  end

  defmacro max_timeuuid(time) do
    quote location: :keep, do: fragment("maxTimeuuid(?)", unquote(time))
  end

  defmacro to_date(time) do
    quote location: :keep, do: fragment("toDate(?)", unquote(time))
  end

  defmacro to_timestamp(time) do
    quote location: :keep, do: fragment("toTimestamp(?)", unquote(time))
  end

  defmacro to_unix_timestamp(time) do
    quote location: :keep, do: fragment("toUnixTimestamp(?)", unquote(time))
  end

  defmacro as_blob(field, type) when type in @types do
    fragment = "#{Atom.to_string(type)}AsBlob(?)"
    quote location: :keep, do: fragment(unquote(fragment), unquote(field))
  end

  defmacro contains(field, value) do
    quote location: :keep, do: fragment("? CONTAINS ?", unquote(field), unquote(value))
  end
end
