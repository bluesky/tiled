"""
These objects express high-level queries and translate them (when possible)
into concrete queries for specific storage backends.
"""


class Text:
    def __init__(self, text):
        self._text = text
