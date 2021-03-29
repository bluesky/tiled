# Notes

## Why are the subpackages split up like this?

There are ``array`` modules in three different subpackages. Why?

Dependency management. The ``structures`` subpackage contains the structure
schema and the serializers and deserializers, which are used by both server and
client. (At the moment, only the server uses serializers and only the client
uses deserializers, but in the future one can imagine both using both.) The
``readers`` are used server-side only. The ``client`` subpackage is, of course,
client-side only.

## What's involved in adding support for a new structure?

* Add a route to the server.
* Make a Client for the structure family and add it to
  ``CatalogClient.DEFAULT_STRUCTURE_CLIENT_DISPATCH`` or pass in a
  ``structure_clients`` in ``CatalogClient.from_uri(...)``.
* Register serializers and deserializers. Technically this is an implementation
  detail of the new server route and Client; it did not *have* to be used.
