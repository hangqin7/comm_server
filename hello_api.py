import os
import json
import datetime
import pytz
import configparser
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy import Table, MetaData, insert
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
# from sqlalchemy.ext.declarative import declarative_base
from typing import Dict

# ---------------------------------------------------------------------
# Load database configuration from ini file
config = configparser.ConfigParser()
config.read('./db_config.ini')

DB_HOST = config['mysql']['host']
DB_USER = config['mysql']['user']
DB_PASS = config['mysql']['password']
DB_NAME = config['mysql']['database']
DB_TABLE = config['mysql']['table']

# Build the SQLAlchemy database URL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=3, max_overflow=2, pool_recycle=3600)

# Timezone
cet = pytz.timezone("Europe/Stockholm")


def store_data_in_rds(data_dict: dict, table_name) -> bool:
    meta = MetaData()
    table = Table(table_name, meta, autoload_with=engine)
    conn = engine.connect()
    trans = conn.begin()
    try:
        # data_dict["timestamp"] = datetime.datetime.now(cet).strftime("%Y-%m-%d %H:%M:%S")
        # Use the keys of data_dict as column names.
        # stmt = insert(table).values(**data_dict)
        stmt = table.insert().values({'data': data_dict})
        conn.execute(stmt)
        trans.commit()
        print("[SUCCESS] Data inserted successfully.")
        return True
    except Exception as e:
        trans.rollback()
        print(f"[ERROR] Database error: {e}")
        return False
    finally:
        conn.close()

# ---------------------------------------------------------------------
# In-memory connection manager for WebSocket connections
class ConnectionManager:
    def __init__(self):
        # Maps client types ('local', 'online') to WebSocket objects
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, client_type: str, websocket: WebSocket):
        # Remove the duplicate accept here.
        self.active_connections[client_type] = websocket

    def disconnect(self, client_type: str):
        if client_type in self.active_connections:
            del self.active_connections[client_type]

    async def send_message(self, client_type: str, message: dict):
        ws = self.active_connections.get(client_type)
        if ws:
            await ws.send_text(json.dumps(message))


manager = ConnectionManager()

# ---------------------------------------------------------------------
# FastAPI application instance
app = FastAPI()

# WebSocket Endpoint for communication
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WS server:
    clientType: {'local_admin1d', 'local_admin', 'local_datalogger', 'online_admin'}
    actions: {'check_health'/'message': all clients, 'command': local_admin, online_admin, 'streaming':local_datalogger}
    For local_datalogger: store data into RDS Database
    For local_admin and online_admin: Setup immediate communication

    A message should be like
    message = {
            "clientType": "online_admin",  # or 'local_admin', local_datalogger', 'local_admin1d'
            "action": "command",  # or 'streaming', 'check_health'
            "data": {"command": command}  # or {data_pack}
        }
    """
    # Check for API key in query parameters or headers.
    api_key = websocket.query_params.get("api_key") or websocket.headers.get("x-api-key")
    expected_key = "service-must**on**1216!"  # Ideally, load this from a secure config or environment variable.
    if not api_key or api_key != expected_key:
        # Close the connection with an appropriate close code (e.g., 1008 for policy violation).
        await websocket.close(code=1008)
        return

    # Accept the WebSocket connection once.
    await websocket.accept()

    # Wait for the client to send its initialization payload.
    try:
        init_msg = await websocket.receive_text()
        init_data = json.loads(init_msg)
        client_type = init_data.get("clientType")
        if client_type not in ("local_admin1d", "local_admin", "online_admin", "local_datalogger"):
            await websocket.close(code=1003)
            return
    except Exception as e:
        await websocket.close(code=1003)
        return

    # Now simply store the connection.
    await manager.connect(client_type, websocket)
    print(f"[INFO] {client_type} client connected.")
    await manager.send_message(client_type, {"status": "CONNECTED", "message": f"{client_type} client connected"})

    try:
        while True:
            msg_text = await websocket.receive_text()
            message = json.loads(msg_text)
            action = message.get("action")
            data = message.get("data")

            if action == "streaming" and client_type == "local_datalogger":  # data streaming
                # Data streaming from local app: store in RDS.
                store_data_in_rds(data['bms'], 'bms_data')
                success = store_data_in_rds(data['ems'], 'ems_data')
                response = {
                    "status": "OK" if success else "ERROR",
                    "message": "Data stored in RDS" if success else "Failed to store data"
                }
                await manager.send_message("local_datalogger", response)

            elif action == "command":

                if client_type == "online_admin":
                    if "local_admin" in manager.active_connections:
                        await manager.send_message("local_admin", data)
                        response = {"status": "OK", "message": "Command forwarded to local app"}
                        await manager.send_message("online_admin", response)
                    else:
                        response = {"status": "ERROR", "message": "No local app connected"}
                    await manager.send_message("online_admin", response)

                elif client_type == "local_admin" or "local_admin1d":
                    if "online_admin" in manager.active_connections:
                        await manager.send_message("online_admin", data)
                        response = {"status": "OK", "message": "Command forwarded to online app"}
                        await manager.send_message(client_type, response)
                    else:
                        response = {"status": "ERROR", "message": "No online app connected"}
                        await manager.send_message(client_type, response)

            elif action == "check_access" and client_type == "online_admin":
                if "local_admin" in manager.active_connections:
                    response = {"status": "enabled", "message": "Local app listening"}
                    await manager.send_message("online_admin", response)
                else:
                    response = {"status": "disabled", "message": "Remote control not allowed"}
                    await manager.send_message("online_admin", response)

            elif action == "request" and client_type == "online_admin":
                if "local_admin1d" in manager.active_connections:
                    response = {"request": "Online app request permission"}
                    await manager.send_message("local_admin1d", response)

            elif action == "check_health":
                # print("[WARN] Unknown action or unsupported operation.")
                response = {"status": "OK", "message": f"waiting actions from {client_type}"}
                await manager.send_message(client_type, response)

    except WebSocketDisconnect:
        manager.disconnect(client_type)
        print(f"[INFO] {client_type} client disconnected.")



# ---------------------------------------------------------------------
# Optional REST endpoint for health-check
@app.get("/health")
def health_check():
    return JSONResponse(content={"status": "ok"})
