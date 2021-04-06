from datetime import datetime, timedelta
from secrets import token_hex
from typing import Optional

from fastapi import Depends, APIRouter, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

from passlib.context import CryptContext

from pydantic import BaseModel

# The TILED_SERVER_SECRET_KEY may be a single key or a ;-separated list of
# keys to support key rotation. The first key will be used for encryption. Each
# key will be tried in turn for decryption.
SECRET_KEYS = os.environ.get("TILED_SERVER_SECRET_KEY", token_hex(32)).split(";")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# This is the hash of the string "secret":
HASHED_DEFAULT_PASSWORD = "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW"


fake_users_db = {
    "alice": HASHED_DEFAULT_PASSWORD,
    "bob": HASHED_DEFAULT_PASSWORD,
    "cara": HASHED_DEFAULT_PASSWORD,
    "public": HASHED_DEFAULT_PASSWORD,
}


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
jwt_router = APIRouter()


def verify_password(plain_password, hashed_password):

    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):

    return pwd_context.hash(password)


def get_hashed_password(db, username: str):
    if username in db:
        return db[username]


def authenticate_user(fake_db, username: str, password: str):

    hashed_password = get_hashed_password(fake_db, username)

    if not hashed_password:

        return False

    if not verify_password(password, hashed_password):

        return False

    return username


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEYS[0], algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    for secret_key in SECRET_KEYS:
        try:
            payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise credentials_exception
            token_data = TokenData(username=username)
            break
        except JWTError:
            # Try the next key in the key rotation.
            continue
    else:
        raise credentials_exception
    if fake_users_db.get(token_data.username, None) is None:
        raise credentials_exception
    return username


@jwt_router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    username = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@jwt_router.get("/users/me/", response_model=str)
async def read_users_me(current_user: str = Depends(get_current_user)):
    return current_user


@jwt_router.get("/users/me/items/")
async def read_own_items(current_user: str = Depends(get_current_user)):
    return [{"item_id": "Foo", "owner": current_user.username}]
