"""
This stores the user passwords *in code* as a toy example.
Do not use this pattern for anything other than exploration.
"""
import numpy

from tiled.utils import SpecialUsers
from tiled.readers.array import ArrayAdapter
from tiled.server.main import serve_catalog
from tiled.catalogs.in_memory import Catalog, SimpleAccessPolicy


# Define our "users".
fake_users_db = {
    "alice": "secret",
    "bob": "secret",
    "cara": "secret",
}
# Specify which entries each user is allowed to use.
# SpecialUsers.public is a sentinel that means anyone can access.
access_policy = SimpleAccessPolicy(
    {
        SpecialUsers.public: ["A"],
        "alice": ["A", "B"],
        "bob": ["C", "D"],
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
        if self._users_to_passwords.get(username) == password:
            return username
        return False


def main():
    # TODO Don't run this from here...use config once we have config....
    import uvicorn

    authenticator = DictionaryAuthenticator(fake_users_db)
    app = serve_catalog(catalog, authenticator=authenticator)
    uvicorn.run(app)


if __name__ == "__main__":
    main()
