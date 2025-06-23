defmodule CorroClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :corro_client,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      name: "CorroClient",
      source_url: "https://github.com/your_org/corro_client"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:req, "~> 0.5.10"},
      {:jason, "~> 1.2"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Elixir client library for interacting with Corrosion database clusters"
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/your_org/corro_client"},
      maintainers: ["Your Name"]
    ]
  end

  defp docs do
    [
      main: "CorroClient",
      extras: ["README.md"]
    ]
  end
end
