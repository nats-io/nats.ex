defmodule Gnat.Mixfile do
  use Mix.Project

  def project do
    [
      app: :gnat,
      version: "0.3.4",
      elixir: "~> 1.4",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      package: package(),
      deps: deps(),
      docs: [
        main: "readme",
        logo: "gnat.png",
        extras: ["README.md"],
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :ssl]]
  end

  defp deps do
    [
      {:benchee, "~> 0.6.0", only: :dev},
      {:ex_doc, "~> 0.15", only: :dev},
      {:poison, "~> 3.0"},
    ]
  end

  defp package do
    [
      description: "A nats client in pure elixir. Resiliance, Performance, Ease-of-Use.",
      licenses: ["MIT"],
      links: %{
        "Github" => "https://github.com/mmmries/gnat",
        "Docs" => "https://hexdocs.pm/gnat",
      },
      maintainers: ["Jon Carstens", "Devin Christensen", "Dave Hackett","Steve Newell", "Michael Ries", "Garrett Thornburg", "Masahiro Tokioka"],
    ]
  end
end
