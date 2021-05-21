class DummyAuthenticator:
    def authenticate(self, username: str, password: str):
        return username
