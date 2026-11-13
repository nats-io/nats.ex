defmodule Gnat.Supervisor do
  defmacro __using__(opts) do
    quote do
      @mod __MODULE__

      use Supervisor

      def start_link(init_arg) do
        Supervisor.start_link(__MODULE__, init_arg)
      end

      @impl true
      def init(_init_arg) do
        otp_app = unquote(opts)[:otp_app] || raise ArgumentError, "otp_app option is required"
        config = Application.get_env(otp_app, @mod, [])

        children = [
          {Gnat.ConnectionSupervisor, connection_opts(config)},
          consumer_supervisor(config)
        ]
        |> List.flatten()

        Supervisor.init(children, strategy: :one_for_one)
      end

      defp connection_opts(config) do
        %{
          name: @mod,
          connection_settings: Keyword.get(config, :connection_settings)
        }
      end

      defp consumer_supervisor(config) do
        case Keyword.get(config, :topics) do
          nil -> []
          topics when is_list(topics) -> [{Gnat.ConsumerSupervisor, topics}]
          _ -> raise ArgumentError, "Invalid :topics option. Expected a list of topics."
        end
      end

      ## Functions that forward to Gnat connection

      def active_subscriptions() do
        Gnat.active_subscriptions(@mod)
      end

      def pub(topic, message, opts \\ []) do
        Gnat.pub(@mod, topic, message, opts)
      end

      def request(topic, body, opts \\ []) do
        Gnat.request(@mod, topic, body, opts)
      end

      def request_multi(topic, body, opts \\ []) do
        Gnat.request_multi(@mod, topic, body, opts)
      end

      def server_info() do
        Gnat.server_info(@mod)
      end

      def sub(subscriber, topic, opts \\ []) do
        Gnat.sub(@mod, subscriber, topic, opts)
      end

      def unsub(sid, opts \\ []) do
        Gnat.unsub(@mod, sid, opts)
      end
    end
  end
end
