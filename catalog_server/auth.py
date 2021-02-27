from functools import wraps


class AuthenticationRequired(Exception):
    pass


def authenticated(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        if (self.access_policy is not None) and (self.authenticated_identity is None):
            raise AuthenticationRequired(f"Access policy on {self} is {self.access_policy}.")
        return method(self, *args, **kwargs)

    return inner
