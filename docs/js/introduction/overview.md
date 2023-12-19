# Overview

[Jetstream](https://docs.nats.io/nats-concepts/jetstream) is a distributed persistence system
built-in to [NATS](https://nats.io/). It provides a streaming system that lets you capture streams
of events from various sources and persist these into persistent stores, which you can immediately
or later replay for processing.

This library exposes interfaces for publishing, consuming and managing Jetstream services. It builds
on top of [Gnat](https://hex.pm/packages/gnat), the officially supported Elixir client for NATS.

* [Let's get Jetstream up and running](./getting_started.md)
* [Using Broadway with Jetstream](../guides/broadway.md)
* [Pull Consumer API](`Gnat.Jetstream.PullConsumer`)
* [Create, update and delete Jetstream streams and consumers via Elixir](../guides/managing.md)