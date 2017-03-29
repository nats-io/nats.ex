defmodule Gnat.Mixfile do
  use Mix.Project

  def project do
    [app: :gnat,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:benchee, "~> 0.6.0", only: :dev},
      {:ex_doc, "~> 0.15", only: :dev},
      {:poison, "~> 3.0"},
    ]
  end
end
