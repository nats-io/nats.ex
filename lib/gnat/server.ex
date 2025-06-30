defmodule Gnat.Server do
  require Logger

  @moduledoc """
  A behavior for acting as a server for nats messages.

  You can use this behavior in your own module and then use the `Gnat.ConsumerSupervisor` to listen for and respond to nats messages.

  ## Example

      defmodule MyApp.RpcServer do
        use Gnat.Server

        def request(%{body: _body}) do
          {:reply, "hi"}
        end

        # defining an error handler is optional, the default one will just call Logger.error for you
        def error(%{gnat: gnat, reply_to: reply_to}, _error) do
          Gnat.pub(gnat, reply_to, "Something went wrong and I can't handle your request")
        end
      end
  """

  @doc """
  Called when a message is received from the broker
  """
  @callback request(message :: Gnat.message()) :: :ok | {:reply, iodata()} | {:error, term()}

  @doc """
  Called when an error occured during the `request/1`

  If your `request/1` function returned `{:error, term}`, then the `term` you returned will be passed as the second argument.
  If an exception was raised during your `request/1` function, then the exception will be passed as the second argument.
  If your `request/1` function returned something other than the supported return types, then its return value will be passed as the second argument.
  """
  @callback error(message :: Gnat.message(), error :: term()) :: :ok | {:reply, iodata()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Gnat.Server

      def error(_message, error) do
        require Logger

        Logger.error(
          "Gnat.Server encountered an error while handling a request: #{inspect(error)}",
          type: :gnat_server_error,
          error: error
        )
      end

      defoverridable error: 2
    end
  end

  # The functions below are not documented because they are used internally to run
  # the callback modules

  @doc false
  def execute(module, message) do
    try do
      case apply(module, :request, [message]) do
        :ok -> :done
        {:reply, data} -> send_reply(message, data)
        {:error, error} -> execute_error(module, message, error)
        other -> execute_error(module, message, other)
      end
    rescue
      e ->
        execute_error(module, message, e)
    end
  end

  @doc false
  defp execute_error(module, message, error) do
    try do
      case apply(module, :error, [message, error]) do
        :ok ->
          :done

        {:reply, data} ->
          send_reply(message, data)

        other ->
          Logger.error(
            "error handler for #{module} returned something unexpected: #{inspect(other)}",
            type: :gnat_server_error
          )
      end
    rescue
      e ->
        Logger.error(
          "error handler for #{module} encountered an error: #{inspect(e)}",
          type: :gnat_server_error
        )
    end
  end

  @doc false
  def send_reply(%{gnat: gnat, reply_to: return_address}, iodata)
      when is_binary(return_address) do
    Gnat.pub(gnat, return_address, iodata)
  end

  def send_reply(_other, _iodata) do
    Logger.error(
      "Could not send reply because no reply_to was provided with the original message",
      type: :gnat_server_error
    )
  end
end
