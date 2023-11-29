# this 'with' statement only defines this module if Broadway is available
with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Jetstream.Acknowledger do
    @moduledoc false
    alias Broadway.Acknowledger

    @behaviour Acknowledger

    @typedoc """
    Acknowledgement data to be placed in `Broadway.Message`.
    """
    @type ack_data :: %{
            :reply_to => String.t(),
            optional(:on_failure) => ack_option,
            optional(:on_success) => ack_option
          }

    @typedoc """
    An acknowledgement action.

    ## Options

    * `ack` - Acknowledges a message was completely handled.

    * `nack` - Signals that the message will not be processed now and will be redelivered.

    * `term` - Tells the server to stop redelivery of a message without acknowledging it.
    """
    @type ack_option :: :ack | :nack | :term

    @type ack_ref :: reference()

    @type t :: %__MODULE__{
            connection_name: String.t(),
            on_failure: ack_option,
            on_success: ack_option
          }

    @enforce_keys [:connection_name]
    defstruct [:connection_name, on_failure: :nack, on_success: :ack]

    @doc """
    Initializes the acknowledger.

    ## Options

    * `connection_name` - The name of Gnat process or Gnat connection supervisor.

    * `on_success` - The action to perform on successful messages. Defaults to `:ack`.

    * `on_failure` - The action to perform on unsuccessful messages. Defaults to `:nack`.
    """
    @spec init(opts :: keyword()) :: {:ok, ack_ref()} | {:error, message :: binary()}
    def init(opts) do
      with {:ok, on_success} <- validate(opts, :on_success, :ack),
           {:ok, on_failure} <- validate(opts, :on_failure, :nack) do
        state = %__MODULE__{
          connection_name: opts[:connection_name],
          on_success: on_success,
          on_failure: on_failure
        }

        ack_ref = make_ref()
        put_config(ack_ref, state)

        {:ok, ack_ref}
      end
    end

    defp put_config(reference, state) do
      :persistent_term.put({__MODULE__, reference}, state)
    end

    @doc """
    Returns an `acknowledger` to be put in `Broadway.Message`.
    """
    @spec builder(ack_ref()) :: (String.t() -> {__MODULE__, ack_ref(), ack_data()})
    def builder(ack_ref) do
      &{__MODULE__, ack_ref, %{reply_to: &1}}
    end

    def get_config(reference) do
      :persistent_term.get({__MODULE__, reference})
    end

    @impl Acknowledger
    def ack(ack_ref, successful, failed) do
      config = get_config(ack_ref)

      # Creating maps where actions (`:ack`, `:term`, ...) are the keys and lists of reply
      # subjects are the values (the default `on_success` and `on_failure` action can be
      # modified for individual messages).
      success_actions = group_reply_topics_by_actions(successful, :on_success, config)
      failure_actions = group_reply_topics_by_actions(failed, :on_failure, config)

      # Merge the maps of actions and acknowledge messages. At this point we don't care which
      # messages have succeeded and which have failed, we only care about the action (`:ack`,
      # `:term`, ...) which can be same for a succeeded and a failed message.
      success_actions
      |> Map.merge(failure_actions, fn _, success_reply_subjects, failure_reply_subjects ->
        success_reply_subjects ++ failure_reply_subjects
      end)
      |> ack_messages(config)

      :ok
    end

    defp group_reply_topics_by_actions(messages, key, config) do
      Enum.group_by(messages, &message_action(&1, key, config), &extract_reply_to/1)
    end

    defp message_action(%{acknowledger: {_, _, ack_data}}, key, config) do
      Map.get_lazy(ack_data, key, fn -> default_action_from_config(key, config) end)
    end

    defp default_action_from_config(:on_success, %{on_success: action}), do: action
    defp default_action_from_config(:on_failure, %{on_failure: action}), do: action

    defp extract_reply_to(message) do
      {_, _, %{reply_to: reply_to}} = message.acknowledger
      reply_to
    end

    defp ack_messages(actions_and_reply_topics, config) do
      Enum.each(actions_and_reply_topics, fn {action, reply_topics} ->
        reply_topics
        |> Enum.each(&apply_ack_func(action, &1, config.connection_name))
      end)
    end

    defp apply_ack_func(:ack, reply_to, connection_name) do
      Gnat.Jetstream.ack(%{gnat: connection_name, reply_to: reply_to})
    end

    defp apply_ack_func(:nack, reply_to, connection_name) do
      Gnat.Jetstream.nack(%{gnat: connection_name, reply_to: reply_to})
    end

    defp apply_ack_func(:term, reply_to, connection_name) do
      Gnat.Jetstream.ack_term(%{gnat: connection_name, reply_to: reply_to})
    end

    @impl Acknowledger
    def configure(_ack_ref, ack_data, options) do
      options = assert_valid_config!(options)
      ack_data = Map.merge(ack_data, Map.new(options))
      {:ok, ack_data}
    end

    defp assert_valid_config!(options) do
      Enum.map(options, fn
        {:on_success, value} -> {:on_success, validate_option!(:on_success, value)}
        {:on_failure, value} -> {:on_failure, validate_option!(:on_failure, value)}
        {other, _value} -> raise ArgumentError, "unsupported option #{inspect(other)}"
      end)
    end

    defp validate(opts, key, default) when is_list(opts) do
      validate_option(key, opts[key] || default)
    end

    defp validate_option(action, value) when action in [:on_success, :on_failure] do
      case validate_action(value) do
        {:ok, result} ->
          {:ok, result}

        :error ->
          {:error, "#{inspect(value)} is not a valid #{inspect(action)} option"}
      end
    end

    defp validate_option(_, value), do: {:ok, value}

    defp validate_option!(key, value) do
      case validate_option(key, value) do
        {:ok, value} -> value
        {:error, message} -> raise ArgumentError, message
      end
    end

    defp validate_action(action) when action in [:ack, :nack, :term], do: {:ok, action}
    defp validate_action(_), do: :error
  end
end
