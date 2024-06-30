defmodule EIC.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixirchange,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      mod: {EIC, []},
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:erlzmq, "~> 4.1", hex: :erlzmq_dnif},
      {:json, "~> 1.4.1", hex: :json},
      # TODO: this can only unpickle, which will be a problem when we want to make dictionary structures...
      # {:unpickler, "~> 0.1.0"},
      # ... so lets try this that hasn't been touched in a decade and needs some hacking
      {:pickle, path: "./pickle"},
      {:inert, path: "./inert"}

      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
