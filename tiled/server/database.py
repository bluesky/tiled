from fastapi import Depends
from sqlalchemy.ext.declarative import declarative_base

from .settings import get_settings

Base = declarative_base()


def get_db(settings=Depends(get_settings)):
    "A FastAPI 'dependency'"
    db = settings.SessionLocal()
    try:
        yield db
    finally:
        db.close()
