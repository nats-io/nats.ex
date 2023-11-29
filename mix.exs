defmodule Gnat.Mixfile do
  use Mix.Project

  @source_url "https://github.com/nats-io/nats.ex"
  @version "1.8.0"

  def project do
    [
      app: :gnat,
      version: @version,
      elixir: "~> 1.10",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      package: package(),
      propcheck: [counter_examples: "test/counter_examples"],
      dialyzer: [ignore_warnings: ".dialyzer_ignore.exs"],
      deps: deps(),
      docs: docs()
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
      {:ex_doc, "~> 0.27.3", only: :dev},
      {:jason, "~> 1.1"},
      {:connection, "~> 1.1"},
      {:broadway, "~> 1.0", optional: true},
      {:nimble_parsec, "~> 0.5 or ~> 1.0"},
      {:nkeys, "~> 0.2"},
      {:propcheck, "~> 1.0", only: :test},
      {:telemetry, "~> 0.4 or ~> 1.0"}
    ]
  end

  defp docs do
    [
      main: "readme",
      logo: "nats-icon-color.svg",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "docs/js/introduction/overview.md",
        "docs/js/introduction/getting_started.md",
        "docs/js/guides/managing.md",
        "docs/js/guides/push_based_consumer.md",
        "docs/js/guides/broadway.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [
        "JetStream Introduction": Path.wildcard("docs/js/introduction/*.md"),
        "JetStream Guides": Path.wildcard("docs/js/guides/*.md")
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      description: "A nats client in pure elixir. Resilience, Performance, Ease-of-Use.",
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
        "Masahiro Tokioka",
        "Kevin Hoffman"
      ]
    ]
  end
end
