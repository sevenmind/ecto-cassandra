defmodule CQL.Result.Prepared do
  import CQL.Decoder

  alias CQL.MetaData

  defstruct [
    :id,
    :metadata,
    :result_metadata,
  ]

  def decode(buffer) do
    {data, _rest} = unpack buffer,
      id:              &short_bytes/1,
      metadata:        &MetaData.decode(&1, true)

    struct(__MODULE__, data)
  end
end