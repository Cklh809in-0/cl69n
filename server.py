#!/usr/bin/env python3
"""
Discord-like Chat Server
Requirements: pip install websockets
Run: python server.py
"""

import asyncio
import json
import websockets
import hashlib
import time
from datetime import datetime

# ── In-memory storage (replace with DB in production) ──────────────────────
users_db = {
    "admin":   {"password": hashlib.sha256(b"admin123").hexdigest(),  "avatar_color": "#5865F2"},
    "alice":   {"password": hashlib.sha256(b"alice123").hexdigest(),  "avatar_color": "#57F287"},
    "bob":     {"password": hashlib.sha256(b"bob123").hexdigest(),    "avatar_color": "#FEE75C"},
    "charlie": {"password": hashlib.sha256(b"charlie123").hexdigest(),"avatar_color": "#ED4245"},
}

rooms = {
    "general":   {"name": "general",   "topic": "Nơi tán gẫu chung"},
    "random":    {"name": "random",    "topic": "Chuyện linh tinh"},
    "tech":      {"name": "tech",      "topic": "Thảo luận kỹ thuật"},
    "announcements": {"name": "announcements", "topic": "Thông báo quan trọng"},
}

# room_name -> [{"username","text","ts","avatar_color"}]
message_history: dict[str, list] = {r: [] for r in rooms}

# websocket -> {"username", "avatar_color", "room"}
connected_clients: dict = {}


def now_ts():
    return datetime.now().strftime("%H:%M")


def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


async def broadcast_room(room: str, message: dict, exclude=None):
    """Send message to all clients in a room."""
    targets = [
        ws for ws, info in connected_clients.items()
        if info["room"] == room and ws != exclude
    ]
    if targets:
        payload = json.dumps(message)
        await asyncio.gather(*[ws.send(payload) for ws in targets], return_exceptions=True)


async def broadcast_presence(room: str):
    """Send updated member list to all in room."""
    members = [
        {"username": info["username"], "avatar_color": info["avatar_color"]}
        for ws, info in connected_clients.items()
        if info["room"] == room
    ]
    await broadcast_room(room, {"type": "presence", "room": room, "members": members})


async def handle_client(websocket):
    client_info = None
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")

            # ── LOGIN ─────────────────────────────────────────────────────
            if mtype == "login":
                username = msg.get("username", "").strip().lower()
                password = msg.get("password", "")
                user = users_db.get(username)

                if not user or user["password"] != hash_pw(password):
                    await websocket.send(json.dumps({
                        "type": "login_error",
                        "message": "Tên đăng nhập hoặc mật khẩu không đúng."
                    }))
                    continue

                # Default room
                room = "general"
                client_info = {
                    "username": username,
                    "avatar_color": user["avatar_color"],
                    "room": room,
                }
                connected_clients[websocket] = client_info

                # Send login success + room list + history
                await websocket.send(json.dumps({
                    "type": "login_ok",
                    "username": username,
                    "avatar_color": user["avatar_color"],
                    "rooms": [
                        {"name": r, "topic": rooms[r]["topic"]}
                        for r in rooms
                    ],
                    "current_room": room,
                    "history": message_history[room],
                }))

                # Notify others
                await broadcast_room(room, {
                    "type": "system",
                    "text": f"{username} vừa tham gia #{room}",
                    "ts": now_ts(),
                }, exclude=websocket)

                await broadcast_presence(room)

            # ── JOIN ROOM ─────────────────────────────────────────────────
            elif mtype == "join_room" and client_info:
                new_room = msg.get("room")
                if new_room not in rooms:
                    continue

                old_room = client_info["room"]

                # Leave old room
                await broadcast_room(old_room, {
                    "type": "system",
                    "text": f"{client_info['username']} đã rời #{old_room}",
                    "ts": now_ts(),
                }, exclude=websocket)
                await broadcast_presence(old_room)

                # Join new room
                client_info["room"] = new_room
                connected_clients[websocket] = client_info

                await websocket.send(json.dumps({
                    "type": "room_joined",
                    "room": new_room,
                    "topic": rooms[new_room]["topic"],
                    "history": message_history[new_room],
                }))

                await broadcast_room(new_room, {
                    "type": "system",
                    "text": f"{client_info['username']} vừa vào #{new_room}",
                    "ts": now_ts(),
                }, exclude=websocket)

                await broadcast_presence(new_room)

            # ── CHAT MESSAGE ──────────────────────────────────────────────
            elif mtype == "message" and client_info:
                text = msg.get("text", "").strip()
                if not text or len(text) > 2000:
                    continue

                room = client_info["room"]
                chat_msg = {
                    "type": "message",
                    "username": client_info["username"],
                    "avatar_color": client_info["avatar_color"],
                    "text": text,
                    "ts": now_ts(),
                    "room": room,
                }

                # Save history (keep last 100 per room)
                message_history[room].append(chat_msg)
                if len(message_history[room]) > 100:
                    message_history[room].pop(0)

                # Broadcast to everyone in room (including sender)
                await broadcast_room(room, chat_msg)
                await websocket.send(json.dumps(chat_msg))

            # ── TYPING ────────────────────────────────────────────────────
            elif mtype == "typing" and client_info:
                room = client_info["room"]
                await broadcast_room(room, {
                    "type": "typing",
                    "username": client_info["username"],
                    "room": room,
                }, exclude=websocket)

    except websockets.exceptions.ConnectionClosedError:
        pass
    finally:
        if websocket in connected_clients:
            info = connected_clients.pop(websocket)
            room = info["room"]
            await broadcast_room(room, {
                "type": "system",
                "text": f"{info['username']} đã ngắt kết nối",
                "ts": now_ts(),
            })
            await broadcast_presence(room)


async def main():
    print("╔══════════════════════════════════════╗")
    print("║   Discord Chat Server  •  port 8765  ║")
    print("╠══════════════════════════════════════╣")
    print("║  Test accounts:                      ║")
    print("║   admin  / admin123                  ║")
    print("║   alice  / alice123                  ║")
    print("║   bob    / bob123                    ║")
    print("║   charlie/ charlie123                ║")
    print("╚══════════════════════════════════════╝")
    async with websockets.serve(handle_client, "localhost", 8765):
        await asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    asyncio.run(main())
