from sqlalchemy.orm import declarative_base

# Everything imports this so we put it in its own module to
# avoid circular imports.
Base = declarative_base()
