class BlueskyEventStreamMixin:
    "Convenience methods used by the server- and client-side"

    def __repr__(self):
        return f"<{type(self).__name__} {set(self)!r} stream_name={self.metadata['stream_name']!r}>"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]

    def read(self):
        """
        Shortcut for reading the 'data' (as opposed to timestamps or config).

        That is:

        >>> stream.read()

        is equivalent to

        >>> stream["data"].read()
        """
        return self["data"].read()


class BlueskyRunMixin:
    "Convenience methods used by the server- and client-side"

    def __repr__(self):
        return (
            f"<{type(self).__name__} "
            f"{set(self)!r} "
            f"uid={self.metadata['start']['uid']!r}"
            ">"
        )
