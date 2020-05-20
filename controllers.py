import hashlib
import uuid
from models import User, UserInDB, RoomInDB, MessageInDB, TokenPayload
import jwt

# from bson import ObjectId
from mongodb import get_nosql_db, AsyncIOMotorClient
from config import MONGODB_DB_NAME, SECRET_KEY, JWT_TOKEN_PREFIX, ACCESS_TOKEN_EXPIRE_MINUTES
from datetime import datetime, timedelta

from starlette.status import HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND
from starlette.exceptions import HTTPException
from jwt import PyJWTError

from fastapi import Depends, Header


async def create_user(request, collection):
    salt = uuid.uuid4().hex
    hashed_password = hashlib.sha512(request.password.encode("utf-8") + salt.encode("utf-8")).hexdigest()

    user = {}
    user["username"] = request.username
    user["salt"] = salt
    user["hashed_password"] = hashed_password
    dbuser = UserInDB(**user).dict()
    row = await collection.insert_one(dbuser)
    dbuser["id"] = row.inserted_id
    return dbuser


async def get_user(name, collection=None) -> UserInDB:
    if collection is None:
        client = await get_nosql_db()
        db = client[MONGODB_DB_NAME]
        collection = db.users

    row = await collection.find_one({"username": name})
    if row is not None:
        return UserInDB(**row).dict()
    else:
        return None


def verify_password(plain_password_w_salt, hashed_password):
    checked_password = hashlib.sha512(plain_password_w_salt.encode("utf-8")).hexdigest()
    return checked_password == hashed_password


async def insert_room(username, room_name, collection):
    room = {}
    room["room_name"] = room_name
    user = await get_user(username)
    room["members"] = [user] if user is not None else []
    # room = Room(**room)
    dbroom = RoomInDB(**room)
    response = await collection.insert_one(dbroom.dict())
    return {"id_inserted": str(response.inserted_id)}


async def get_rooms():
    client = await get_nosql_db()
    db = client[MONGODB_DB_NAME]
    collection = db.rooms
    rows = collection.find()
    row_list = []
    async for row in rows:
        row_list.append(RoomInDB(**row))
    return row_list


async def get_room(room_name) -> RoomInDB:
    client = await get_nosql_db()
    db = client[MONGODB_DB_NAME]
    collection = db.rooms
    row = await collection.find_one({"room_name": room_name})
    if row is not None:
        return RoomInDB(**row)
    else:
        return None


async def insert_message(message, sender, room_name):
    client = await get_nosql_db()
    db = client[MONGODB_DB_NAME]
    rooms_collection = db.rooms
    # messages_collection = db.messages

    message_body = {}
    sender_in_db = await get_user(sender)
    message_body["user"] = sender_in_db
    message_body["content"] = message
    dbmessage = MessageInDB(**message_body)
    # message_upload = await messages_collection.insert_one(dbmessage.dict())
    room_update = await rooms_collection.update_one(
        {"room_name": room_name}, {"$push": {"messages": dbmessage.dict()}, "$set": {"last_pinged": datetime.utcnow()}},
    )
    status_message = {
        # "message_id_inserted": str(message_upload.inserted_id),
        "room_updated": str(room_update.raw_result),
    }
    write_notification(status_message)
    return status_message


def write_notification(message):
    with open("messages.log", mode="a+") as _file:
        content = f"{datetime.utcnow()}: {message}\n"
        _file.write(content)


def create_access_token(*, data: dict):
    expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire, "sub": "access"})
    encoded_jwt = jwt.encode(to_encode, str(SECRET_KEY), algorithm="HS256")
    return encoded_jwt


def _get_auth_token(authorization: str = Header(...)):
    token_prefix, token = authorization.split(" ")
    if token_prefix != JWT_TOKEN_PREFIX:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid authorization type")
    return token


async def _get_current_user(
    db: AsyncIOMotorClient = Depends(get_nosql_db), token: str = Depends(_get_auth_token)
) -> User:
    try:
        payload = jwt.decode(token, str(SECRET_KEY), algorithms=["HS256"])
        token_data = TokenPayload(**payload)
    except PyJWTError:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials")

    dbuser = await get_user(token_data.username)
    if not dbuser:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="User not found")

    user = User(**dbuser.dict(), token=token)
    return user
