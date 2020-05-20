import logging
import pymongo
import pydantic
import json

from pydantic import BaseModel
from collections import defaultdict

from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, WebSocket, Request, Depends, BackgroundTasks
from fastapi.templating import Jinja2Templates

from starlette.websockets import WebSocketDisconnect
from starlette.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException
from starlette.status import HTTP_201_CREATED, HTTP_400_BAD_REQUEST

from config import MONGODB_DB_NAME
from mongodb import close_mongo_connection, connect_to_mongo, get_nosql_db, AsyncIOMotorClient
from controllers import (
    create_user,
    get_user,
    verify_password,
    insert_room,
    get_rooms,
    get_room,
    insert_message,
    create_access_token,
)
from models import UserInResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # can alter with time
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


class Notifier:
    """
        Manages chat room sessions and members along with message routing
    """

    def __init__(self):
        self.connections: dict = defaultdict(dict)
        self.generator = self.get_notification_generator()

    async def get_notification_generator(self):
        while True:
            message = yield
            msg = message["message"]
            room_name = message["room_name"]
            await self._notify(msg, room_name)

    def get_members(self, room_name):
        try:
            return self.connections[room_name]
        except Exception:
            return None

    async def push(self, msg: str, room_name: str = None):
        message_body = {"message": msg, "room_name": room_name}
        await self.generator.asend(message_body)

    async def connect(self, websocket: WebSocket, room_name: str):
        await websocket.accept()
        if self.connections[room_name] == {} or len(self.connections[room_name]) == 0:
            self.connections[room_name] = []
        self.connections[room_name].append(websocket)
        print(f"CONNECTIONS : {self.connections[room_name]}")

    def remove(self, websocket: WebSocket, room_name: str):
        self.connections[room_name].remove(websocket)
        print(f"CONNECTION REMOVED\nREMAINING CONNECTIONS : {self.connections[room_name]}")

    async def _notify(self, message: str, room_name: str):
        living_connections = []
        while len(self.connections[room_name]) > 0:
            # Looping like this is necessary in case a disconnection is handled
            # during await websocket.send_text(message)
            websocket = self.connections[room_name].pop()
            await websocket.send_text(message)
            living_connections.append(websocket)
        self.connections[room_name] = living_connections


notifier = Notifier()


@app.on_event("startup")
async def startup_event():
    await connect_to_mongo()
    client = await get_nosql_db()
    db = client[MONGODB_DB_NAME]
    try:
        await notifier.generator.asend(None)

        await db.create_collection("users")
        await db.create_collection("rooms")
        # await db.create_collection("messages") # no need, messages are embedded into rooms
        user_collection = db.users
        room_collection = db.rooms
        await user_collection.create_index("username", name="username", unique=True)
        await room_collection.create_index("room_name", name="room_name", unique=True)
    except pymongo.errors.CollectionInvalid as e:
        logging.info(e)
        pass


@app.on_event("shutdown")
async def shutdown_event():
    await close_mongo_connection()


# after registration
@app.get("/")
async def homepage(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/login", tags=["main view"])
async def get_main(request: Request, token: str, dbuser: dict):
    return templates.TemplateResponse("main.html", {"request": request, "user": UserInResponse(**dbuser, token=token)})


class RegisterRequest(BaseModel):
    username: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


@app.put("/register", tags=["authentication"], status_code=HTTP_201_CREATED)
async def register_user(request: RegisterRequest, client: AsyncIOMotorClient = Depends(get_nosql_db)):
    try:
        db = client[MONGODB_DB_NAME]
        collection = db.users
        dbuser = await create_user(request, collection)
        token = create_access_token(data={"username": dbuser["username"]})
        print(f"REGISTER: {token}")
        return UserInResponse(**dbuser, token=token)
        # return get_main(request, token, dbuser)
    except pydantic.error_wrappers.ValidationError as e:
        return e
    except pymongo.errors.DuplicateKeyError as e:
        return {"error": "username already exists", "verbose": e}


@app.put("/login", tags=["authentication"])
async def login_user(request: RegisterRequest, client: AsyncIOMotorClient = Depends(get_nosql_db)):
    db = client[MONGODB_DB_NAME]
    collection = db.users
    dbuser = await get_user(request.username, collection=collection)
    if not dbuser or not verify_password((request.password + dbuser["salt"]), dbuser["hashed_password"]):
        raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="Incorrect email or password")
    else:
        token = create_access_token(data={"username": dbuser["username"]})
        print(f"LOGIN: {token}")
        return UserInResponse(**dbuser, token=token)
        # return get_main(request, token, dbuser)


# controller routes
@app.get("/{room_name}/{user_name}")
async def get(request: Request, room_name, user_name):
    return templates.TemplateResponse(
        "chat_room.html", {"request": request, "room_name": room_name, "user_name": user_name}
    )


@app.websocket("/ws/{room_name}/{user_name}")
async def websocket_endpoint(websocket: WebSocket, room_name, user_name, background_tasks: BackgroundTasks):
    await notifier.connect(websocket, room_name)
    try:
        while True:
            data = await websocket.receive_text()
            d = json.loads(data)
            d["room_name"] = room_name
            print(f"UPDATING DB with: {d}")
            # background_tasks.add_task(insert_message, d["message"], d["sender"], d["room_name"])

            room_members = notifier.get_members(room_name) if notifier.get_members(room_name) is not None else []
            if websocket not in room_members:
                print("SENDER NOT IN ROOM MEMBERS: RECONNECTING")
                await notifier.connect(websocket, room_name)

            await insert_message(d["message"], d["sender"], d["room_name"])
            await notifier.push(f"{data}", room_name)
    except WebSocketDisconnect:
        notifier.remove(websocket, room_name)


class RoomCreateRequest(BaseModel):
    username: str
    room_name: str


@app.put("/create_room")
async def create_room(request: RoomCreateRequest, client: AsyncIOMotorClient = Depends(get_nosql_db)):
    db = client[MONGODB_DB_NAME]
    collection = db.rooms
    res = await insert_room(request.username, request.room_name, collection)
    return res


@app.get("/rooms")
async def get_all_rooms(client: AsyncIOMotorClient = Depends(get_nosql_db)):
    rooms = await get_rooms()
    return rooms


@app.get("/room/{room_name}")
async def get_single_room(room_name):
    room = await get_room(room_name)
    return room
