import os
import json
import datetime
import configparser
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, declarative_base
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
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base for model definitions
Base = declarative_base()

# Define a model for the incoming data using the table name from the ini file
class DataEntry(Base):
    __tablename__ = DB_TABLE
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    data = Column(MySQLJSON)  # Storing the incoming payload as JSON

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

def store_data_in_rds(data_dict: dict) -> bool:
    """
    Stores the provided dictionary as a new DataEntry in the database.
    """
    # Add a timestamp (the model will also add one by default if not provided)
    data_dict["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session: Session = SessionLocal()
    try:
        new_entry = DataEntry(data=data_dict)
        session.add(new_entry)
        session.commit()
        print("[SUCCESS] Data inserted successfully.")
        return True
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Database error: {e}")
        return False
    finally:
        session.close()

# ---------------------------------------------------------------------
# In-memory connection manager for WebSocket connections
class ConnectionManager:
    def __init__(self):
        # Maps client types ('local', 'online') to WebSocket objects
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, client_type: str, websocket: WebSocket):
        await websocket.accept()
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
    # Wait for the client to send its initialization payload
    try:
        init_msg = await websocket.receive_text()
        init_data = json.loads(init_msg)
        client_type = init_data.get("clientType")
        if client_type not in ("local", "online"):
            raise HTTPException(status_code=400, detail="Invalid clientType")
    except Exception as e:
        await websocket.close(code=1003)
        return

    await manager.connect(client_type, websocket)
    print(f"[INFO] {client_type} client connected.")

    try:
        while True:
            msg_text = await websocket.receive_text()
            message = json.loads(msg_text)
            action = message.get("action")
            data = message.get("data", {})

            if action == "data" and client_type == "local":
                # Data streaming from local app: store in RDS
                success = store_data_in_rds(data)
                response = {
                    "status": "OK" if success else "ERROR",
                    "message": "Data stored in RDS" if success else "Failed to store data"
                }
                await manager.send_message("local", response)

            elif action == "command":
                # Command sending: if local command, forward to online, and vice versa.
                # Enforce that local commands have priority.
                if client_type == "online":
                    # Online app sends a command; forward it if local is connected.
                    if "local" in manager.active_connections:
                        await manager.send_message("local", data)
                        response = {"status": "OK", "message": "Command forwarded to local app"}
                    else:
                        response = {"status": "ERROR", "message": "No local app connected"}
                    await manager.send_message("online", response)
                elif client_type == "local":
                    # Local app sends a command; forward to online if connected.
                    if "online" in manager.active_connections:
                        await manager.send_message("online", data)
                        response = {"status": "OK", "message": "Command forwarded to online app"}
                        await manager.send_message("local", response)
                    else:
                        response = {"status": "ERROR", "message": "No online app connected"}
                        await manager.send_message("local", response)
            else:
                print("[WARN] Unknown action or unsupported operation.")
    except WebSocketDisconnect:
        manager.disconnect(client_type)
        print(f"[INFO] {client_type} client disconnected.")

# ---------------------------------------------------------------------
# Optional REST endpoint for health-check
@app.get("/health")
def health_check():
    return JSONResponse(content={"status": "ok"})
