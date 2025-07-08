defmodule ExDbTest do
  use ExUnit.Case
  doctest ExDb

  test "greets the world" do
    assert ExDb.hello() == :world
  end
end
