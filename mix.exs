defmodule Gnat.Mixfile do
  use Mix.Project

  @source_url "https://github.com/mmmries/gnat"
  @version "1.3.0"

  def project do
    [
      app: :gnat,
      version: @version,
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      package: package(),
      propcheck: [counter_examples: "test/counter_examples"],
      dialyzer: [ignore_warnings: ".dialyzer_ignore.exs"],
      deps: deps(),
      docs: [
        main: "readme",
        logo: "nats-icon-color.svg",
        source_ref: "v#{@version}",
        source_url: @source_url,
        extras: ["README.md", "CHANGELOG.md"]
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :ssl]]
  end

  defp deps do
    [
      {:benchee, "~> 1.0", only: :dev},
      {:cowlib, "~> 2.0"},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.22", only: :dev},
      {:jason, "~> 1.1"},
      {:nimble_parsec, "~> 0.5 or ~> 1.0"},
      {:nkeys, "~> 0.2"},
      {:propcheck, "~> 1.0", only: :test},
      {:telemetry, "~> 0.4"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      description: "A nats client in pure elixir. Resiliance, Performance, Ease-of-Use.",
      licenses: ["MIT"],
      links: %{
        "Changelog" => "#{@source_url}/blob/master/CHANGELOG.md",
        "Github" => @source_url
      },
      maintainers: [
        "Jon Carstens",
        "Devin Christensen",
        "Dave Hackett",
        "Steve Newell",
        "Michael Ries",
        "Garrett Thornburg",
        "Masahiro Tokioka"
      ]
    ]
  end
end
