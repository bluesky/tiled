import secrets

from .utils import modules_available


class DummyAuthenticator:
    """
    For test and demo purposes only!

    Accept any username and any password.

    """

    def authenticate(self, username: str, password: str):
        return username


class DictionaryAuthenticator:
    """
    For test and demo purposes only!

    Check passwords from a dictionary of usernames mapped to passwords.
    """

    def __init__(self, users_to_passwords):
        self._users_to_passwords = users_to_passwords

    def authenticate(self, username: str, password: str):
        true_password = self._users_to_passwords.get(username)
        if not true_password:
            # Username is not valid.
            return
        if secrets.compare_digest(true_password, password):
            return username


class PAMAuthenticator:
    def __init__(self, service="login"):
        if not modules_available("pamela"):
            raise ModuleNotFoundError(
                "This PAMAuthenticator requires the module 'pamela' to be installed."
            )
        # TODO Try to open a PAM session.
        self.service = service

    def authenticate(self, username: str, password: str):
        import pamela

        try:
            pamela.authenticate(username, password, service=self.service)
        except pamela.PAMError:
            # Authentication failed.
            return
        else:
            return username
