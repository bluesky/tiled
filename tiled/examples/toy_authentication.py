"""
See tiled/example_configurations/toy_authentication for
server configuration that runs this example.
"""
import numpy
import secrets

from tiled.utils import SpecialUsers
from tiled.readers.array import ArrayAdapter
from tiled.catalogs.in_memory import Catalog, SimpleAccessPolicy


# Specify which entries each user is allowed to use.
# SpecialUsers.public is a sentinel that means anyone can access.
access_policy = SimpleAccessPolicy(
    {
        SpecialUsers.public: ["A"],
        "alice": ["A", "B"],
        "bob": ["A", "C"],
        "cara": SimpleAccessPolicy.ALL,
    }
)
# Make a Catalog with a couple arrays in it.
catalog = Catalog(
    {
        "A": ArrayAdapter.from_array(10 * numpy.ones((10, 10))),
        "B": ArrayAdapter.from_array(20 * numpy.ones((10, 10))),
        "C": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
        "D": ArrayAdapter.from_array(30 * numpy.ones((10, 10))),
    },
    access_policy=access_policy,
)


class DictionaryAuthenticator:
    "For demo purposes only!"

    def __init__(self, users_to_passwords):
        self._users_to_passwords = users_to_passwords

    def authenticate(self, username: str, password: str):
        true_password = self._users_to_passwords.get(username)
        if not true_password:
            # Username is not valid.
            return
        if secrets.compare_digest(true_password, password):
            return username