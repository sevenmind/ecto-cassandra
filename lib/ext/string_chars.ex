defimpl String.Chars, for: Cassandra.Statement do
  def to_string(%Cassandra.Statement{query: query}), do: query
end

defimpl String.Chars, for: Xandra.Simple do
  def to_string(%Xandra.Simple{statement: statement}), do: statement
end

defimpl String.Chars, for: Xandra.Prepared do
  def to_string(%Xandra.Prepared{statement: statement}), do: statement
end

defimpl String.Chars, for: Xandra.Error do
  def to_string(%Xandra.Error{message: message}), do: message
end
