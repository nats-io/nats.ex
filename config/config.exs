import Config

if Mix.env() == :test do
  config :gnat, Gnat.SupervisorTest.MyApp.Gnat,
    connection_settings: [
      %{}
    ]
end
