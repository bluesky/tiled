from sqlalchemy.orm import DeclarativeBase


# Everything imports this so we put it in its own module to
# avoid circular imports.
class Base(DeclarativeBase):
    pass
