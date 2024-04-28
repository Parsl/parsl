defmodule EICTest do
  use ExUnit.Case
  doctest EIC

  test "greets the world" do
    assert EIC.hello() == :world
  end
end
