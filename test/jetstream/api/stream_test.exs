defmodule Gnat.Jetstream.API.StreamTest do
  use Gnat.Jetstream.ConnCase
  alias Gnat.Jetstream.API.Stream

  @moduletag with_gnat: :gnat

  test "listing and creating, and deleting streams" do
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)

    stream = %Stream{name: "LIST_TEST", subjects: ["STREAM_TEST"]}
    {:ok, response} = Stream.create(:gnat, stream)
    assert response.config == stream

    assert response.state == %{
             bytes: 0,
             consumer_count: 0,
             first_seq: 0,
             first_ts: ~U[0001-01-01 00:00:00Z],
             last_seq: 0,
             last_ts: ~U[0001-01-01 00:00:00Z],
             messages: 0,
             deleted: nil,
             lost: nil,
             num_deleted: nil,
             num_subjects: nil,
             subjects: nil
           }

    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert "LIST_TEST" in streams

    assert :ok = Stream.delete(:gnat, "LIST_TEST")
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)
  end

  test "list/2 includes multiple streams" do
    stream = %Stream{name: "LIST_SUBJECT_TEST_ONE", subjects: ["LIST_SUBJECT_TEST.subject1"]}
    {:ok, _response} = Stream.create(:gnat, stream)
    stream = %Stream{name: "LIST_SUBJECT_TEST_TWO", subjects: ["LIST_SUBJECT_TEST.subject2"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert "LIST_SUBJECT_TEST_ONE" in streams
    assert "LIST_SUBJECT_TEST_TWO" in streams
    assert :ok = Stream.delete(:gnat, "LIST_SUBJECT_TEST_ONE")
    assert :ok = Stream.delete(:gnat, "LIST_SUBJECT_TEST_TWO")
  end

  test "list/2 can filter by subject" do
    stream = %Stream{name: "LIST_SUBJECT_TEST_ONE", subjects: ["LIST_SUBJECT_TEST.subject1"]}
    {:ok, _response} = Stream.create(:gnat, stream)
    stream = %Stream{name: "LIST_SUBJECT_TEST_TWO", subjects: ["LIST_SUBJECT_TEST.subject2"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    {:ok, %{streams: [stream]}} = Stream.list(:gnat, subject: "LIST_SUBJECT_TEST.subject2")
    assert stream == "LIST_SUBJECT_TEST_TWO"
    assert :ok = Stream.delete(:gnat, "LIST_SUBJECT_TEST_ONE")
    assert :ok = Stream.delete(:gnat, "LIST_SUBJECT_TEST_TWO")
  end

  test "list/2 includes accepts offset" do
    stream = %Stream{name: "LIST_OFFSET_TEST_ONE", subjects: ["LIST_OFFSET_TEST.subject1"]}
    {:ok, _response} = Stream.create(:gnat, stream)
    stream = %Stream{name: "LIST_OFFSET_TEST_TWO", subjects: ["LIST_OFFSET_TEST.subject2"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    {:ok, %{streams: streams}} = Stream.list(:gnat)
    num_no_offset = Enum.count(streams)

    {:ok, %{streams: streams}} = Stream.list(:gnat, offset: 1)
    num_with_offset = Enum.count(streams)

    # Checking offset functionality without a strict pattern match to keep this
    # test passing even if another test forgets to delete a stream after it's done
    assert num_no_offset - num_with_offset == 1
    assert :ok = Stream.delete(:gnat, "LIST_OFFSET_TEST_ONE")
    assert :ok = Stream.delete(:gnat, "LIST_OFFSET_TEST_TWO")
  end

  test "create stream with discard_new_per_subject: true" do
    stream = %Stream{name: "DISCARD_NEW_PER_SUBJECT_TEST",
                     subjects: ["STREAM_TEST"],
                     max_msgs_per_subject: 1,
                     discard_new_per_subject: true,
                     discard: :new}
    assert {:ok, _response} = Stream.create(:gnat, stream)

    assert {:ok, _} = Gnat.request(:gnat, "STREAM_TEST", "first message")
    assert {:ok, response} =
      Stream.get_message(:gnat, "DISCARD_NEW_PER_SUBJECT_TEST", %{
            last_by_subj: "STREAM_TEST"})
      %{
        data: "first message",
        hdrs: nil,
        subject: "STREAM_TEST",
        time: %DateTime{}
      } = response

    assert {:ok, _} = Gnat.request(:gnat, "STREAM_TEST", "second message")
    assert {:ok, response} =
      Stream.get_message(:gnat, "DISCARD_NEW_PER_SUBJECT_TEST", %{
            last_by_subj: "STREAM_TEST"})
      %{
        data: "first message",
        hdrs: nil,
        subject: "STREAM_TEST",
        time: %DateTime{}
      } = response

    Stream.purge(:gnat, "DISCARD_NEW_PER_SUBJECT_TEST")

    assert {:ok, _} = Gnat.request(:gnat, "STREAM_TEST", "second message")
    assert {:ok, response} =
      Stream.get_message(:gnat, "DISCARD_NEW_PER_SUBJECT_TEST", %{
            last_by_subj: "STREAM_TEST"})
      %{
        data: "second message",
        hdrs: nil,
        subject: "STREAM_TEST",
        time: %DateTime{}
      } = response

      assert :ok = Stream.delete(:gnat, "DISCARD_NEW_PER_SUBJECT_TEST")
  end

  test "updating a stream" do
    stream = %Stream{name: "UPDATE_TEST", subjects: ["STREAM_TEST"]}
    assert {:ok, _response} = Stream.create(:gnat, stream)
    updated_stream = %Stream{name: "UPDATE_TEST", subjects: ["STREAM_TEST", "NEW_SUBJECT"]}
    assert {:ok, response} = Stream.update(:gnat, updated_stream)
    assert response.config.subjects == ["STREAM_TEST", "NEW_SUBJECT"]
    assert :ok = Stream.delete(:gnat, "UPDATE_TEST")
  end

  test "failed deletes" do
    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Stream.delete(:gnat, "NaN")
  end

  test "getting stream info" do
    stream = %Stream{name: "INFO_TEST", subjects: ["INFO_TEST.*"]}
    assert {:ok, _response} = Stream.create(:gnat, stream)

    assert {:ok, response} = Stream.info(:gnat, "INFO_TEST")
    assert response.config == stream

    assert response.state == %{
             bytes: 0,
             consumer_count: 0,
             first_seq: 0,
             first_ts: ~U[0001-01-01 00:00:00Z],
             last_seq: 0,
             last_ts: ~U[0001-01-01 00:00:00Z],
             messages: 0,
             deleted: nil,
             lost: nil,
             num_deleted: nil,
             num_subjects: nil,
             subjects: nil,
           }

    assert :ok = Stream.delete(:gnat, "INFO_TEST")
  end

  test "creating a stream with non-standard settings" do
    stream = %Stream{
      name: "ARGS_TEST",
      subjects: ["ARGS_TEST.*"],
      retention: :workqueue,
      duplicate_window: 100_000_000,
      storage: :memory,
      compression: "s2"
    }

    assert {:ok, %{config: result}} = Stream.create(:gnat, stream)
    assert result.name == "ARGS_TEST"
    assert result.duplicate_window == 100_000_000
    assert result.retention == :workqueue
    assert result.storage == :memory
    assert result.compression == "s2"
    assert :ok = Stream.delete(:gnat, "ARGS_TEST")
  end

  test "validating stream names" do
    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test.periods", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test>greater", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} = Stream.create(:gnat, %Stream{name: "test_star*", subjects: ["foo"]})
    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} =
             Stream.create(:gnat, %Stream{name: "test-space ", subjects: ["foo"]})

    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"

    assert {:error, reason} = Stream.create(:gnat, %Stream{name: "\ttest-tab", subjects: ["foo"]})
    assert reason == "invalid name: cannot contain '.', '>', '*', spaces or tabs"
  end

  describe "get_message/3" do
    test "error if both seq and last_by_subj are used" do
      assert {:error, reason} = Stream.get_message(:gnat, "foo", %{seq: 1, last_by_subj: "bar"})
      assert reason == "To get a message you must use only one of `seq` or `last_by_subj`"
    end

    test "decodes message data" do
      stream = %Stream{name: "GET_MESSAGE_TEST", subjects: ["GET_MESSAGE_TEST.foo"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert {:ok, _} = Gnat.request(:gnat, "GET_MESSAGE_TEST.foo", "hi there")

      assert {:ok, response} =
               Stream.get_message(:gnat, "GET_MESSAGE_TEST", %{
                 last_by_subj: "GET_MESSAGE_TEST.foo"
               })

      %{
        data: "hi there",
        hdrs: nil,
        subject: "GET_MESSAGE_TEST.foo",
        time: %DateTime{}
      } = response

      assert is_number(response.seq)

      assert :ok = Stream.delete(:gnat, "GET_MESSAGE_TEST")
    end

    test "decodes message data with headers" do
      stream = %Stream{
        name: "GET_MESSAGE_TEST_WITH_HEADERS",
        subjects: ["GET_MESSAGE_TEST_WITH_HEADERS.bar"]
      }

      assert {:ok, _response} = Stream.create(:gnat, stream)

      assert {:ok, %{body: _body}} =
               Gnat.request(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS.bar", "hi there",
                 headers: [{"foo", "bar"}]
               )

      assert {:ok, response} =
               Stream.get_message(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS", %{
                 last_by_subj: "GET_MESSAGE_TEST_WITH_HEADERS.bar"
               })

      assert response.hdrs =~ "foo: bar"

      assert :ok = Stream.delete(:gnat, "GET_MESSAGE_TEST_WITH_HEADERS")
    end
  end

  describe "purge/2" do
    test "clears the stream" do
      stream = %Stream{name: "PURGE_TEST", subjects: ["PURGE_TEST.foo"]}
      assert {:ok, _response} = Stream.create(:gnat, stream)
      assert {:ok, _} = Gnat.request(:gnat, "PURGE_TEST.foo", "hi there")

      assert :ok = Stream.purge(:gnat, "PURGE_TEST")

      assert {:error, %{"description" => description}} =
               Stream.get_message(:gnat, "PURGE_TEST", %{
                 last_by_subj: "PURGE_TEST.foo"
               })

      assert description in ["no message found", "stream store EOF"]

      assert :ok = Stream.delete(:gnat, "PURGE_TEST")
    end
  end
end
