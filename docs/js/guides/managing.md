# Managing Streams and Consumers

Jetstream provides a JSON API for managing streams and consumers.
This library exposes this API via interactions with the `Jetstream.Api.Stream` and `Jetstream.Api.Consumer` modules.

These modules act as native wrappers for the API and do not attempt to simplify any of the common use-cases.
As this library matures we may introduce a separate layer of functions to handle these scenarios, but for now our aim is to provide full access to the Jetstream API.