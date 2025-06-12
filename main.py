from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import Optional
from databases import Database
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from models.users import User, UserUpdate

load_dotenv()
app = FastAPI()

SECRET_KEY = os.getenv("SECRET_KEY","ZxYKjXE57sXrsqXB8P0Snufz7NlZAJVN5CMUGvV5b2rQ4ergp8")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

DATABASE_URL = os.getenv("POSTGRES_URL")
engine = create_engine(DATABASE_URL)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


async def get_user(username: str):
    query = "SELECT * FROM users WHERE username = :username"
    user = await engine.fetch_one(query=query, values={"username": username})
    return user


async def authenticate_user(username: str, password: str):
    user = await get_user(username)
    if not user:
        return False
    if not verify_password(password, user["hashed_password"]):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Bearer"}
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = await get_user(username)
    if user is None:
        raise credentials_exception
    return user


@app.on_event("startup")
async def startup():
    await engine.connect()


@app.on_event("shutdown")
async def shutdown():
    await engine.disconnect()


@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/register")
async def register_user(user: User):
    query = "SELECT * FROM users WHERE username = :username"
    existing_user = await engine.fetch_one(query=query, values={"username": user.username})
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")
    hashed_password = get_password_hash(user.password)
    insert_query = "INSERT INTO users (username, hashed_password) VALUES (:username, :hashed_password)"
    await engine.execute(query=insert_query, values={"username": user.username, "hashed_password": hashed_password})
    return {"message": "User registered successfully"}

@app.get("/users")
async def get_all_users(current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized to perform this action")

    query = "SELECT id, username, is_blocked FROM users"
    users = await engine.fetch_all(query=query)
    return {"users": users}


@app.put("/users/{user_id}")
async def edit_user(user_id: int, user_update: UserUpdate, current_user: dict = Depends(get_current_user)):
    if current_user["id"] != user_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized to edit this user")

    updated_values = {}
    if user_update.username:
        updated_values["username"] = user_update.username
    if user_update.password:
        updated_values["hashed_password"] = get_password_hash(user_update.password)
    if user_update.role:
        updated_values["role"] = user_update.role
    if user_update.is_blocked:
        updated_values["is_blocked"] = user_update.is_blocked

    if not updated_values:
        raise HTTPException(status_code=400, detail="No valid fields provided for update")

    update_query = "UPDATE users SET " + ", ".join(
        [f"{key} = :{key}" for key in updated_values.keys()]
    ) + " WHERE id = :user_id"
    updated_values["user_id"] = user_id
    await engine.execute(query=update_query, values=updated_values)
    return {"message": "User updated successfully"}


@app.put("/users/{user_id}/block")
async def block_user(user_id: int, current_user: dict = Depends(get_current_user)):
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Not authorized to perform this action")

    update_query = "UPDATE users SET is_blocked = :is_blocked WHERE id = :user_id"
    await engine.execute(query=update_query, values={"user_id": user_id, "is_blocked": True})
    return {"message": "User blocked successfully"}
