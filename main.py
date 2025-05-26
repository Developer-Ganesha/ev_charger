import asyncio
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
import httpx
import websockets
from ocpp.routing import on
from ocpp.v16 import ChargePoint as OcppChargePoint
from ocpp.v16.enums import RegistrationStatus
from ocpp.v16 import call_result
import threading

# Initialize FastAPI app
app = FastAPI(title="EV Charger API")

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ev-charger")

# WebSocket clients
charging_ws_clients = set()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    charging_ws_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        charging_ws_clients.remove(websocket)

# Charger state
charger_status = {
    "charger_id": "EV123",
    "status": "Available",
    "current_session": None,
    "charging_progress": {
        "battery_level": 0,
        "estimated_time_left": 0,
        "charging_power": 7.2
    }
}

# Store connected charger ID
connected_charger_id  = {"EV123", "EV456", "EV789"}

# Charging task
charging_task = None

# WebSocket message broadcast
async def notify_clients(message: str):
    to_remove = []
    for ws in charging_ws_clients:
        try:
            await ws.send_text(message)
        except Exception:
            to_remove.append(ws)
    for ws in to_remove:
        charging_ws_clients.remove(ws)

# Simulate charging
async def simulate_charging():
    try:
        while charger_status["status"] == "Charging":
            await asyncio.sleep(5)

            level = charger_status["charging_progress"]["battery_level"]

            if level >= 100:
                charger_status["charging_progress"]["battery_level"] = 100
                charger_status["charging_progress"]["estimated_time_left"] = 0
                await notify_clients("Charging Complete: 100%")
                break

            charger_status["charging_progress"]["battery_level"] += 2
            charger_status["charging_progress"]["estimated_time_left"] = int(
                (100 - charger_status["charging_progress"]["battery_level"]) * 0.5
            )

            logger.info(f"Charging... {charger_status['charging_progress']['battery_level']}%")

    except asyncio.CancelledError:
        logger.info("Charging simulation cancelled.")

# OCPP ChargePoint
class ChargePoint(OcppChargePoint):
    @on("BootNotification")
    async def on_boot_notification(self, charge_point_model, charge_point_vendor, **kwargs):
        logger.info(f"BootNotification from {self.id}: {charge_point_model}, {charge_point_vendor}")
        return call_result.BootNotificationPayload(
            current_time=datetime.utcnow().isoformat(),
            interval=10,
            status=RegistrationStatus.accepted
        )

    @on("Heartbeat")
    async def on_heartbeat(self):
        logger.info(f"Heartbeat received from {self.id}")
        return call_result.HeartbeatPayload(current_time=datetime.utcnow().isoformat())

# OCPP server thread
async def ocpp_server(websocket, path):
    charge_point_id = path.strip("/")
    cp = ChargePoint(charge_point_id, websocket)
    await cp.start()

def start_ocpp_server():
    async def run():
        server = await websockets.serve(ocpp_server, "0.0.0.0", 9000, subprotocols=["ocpp1.6"])
        logger.info("OCPP Server running on ws://0.0.0.0:9000")
        await server.wait_closed()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())

threading.Thread(target=start_ocpp_server, daemon=True).start()

# --------------------- API Endpoints ---------------------

@app.post("/connect-charger")
async def connect_charger(charger_id: str ):
    global connected_charger_id
    if charger_id != charger_status["charger_id"]:
        return {
            "status": "False",
            "message": f"Unknown charger ID: {charger_id}"
        }

    connected_charger_id = charger_id
    logger.info(f"Charger {charger_id} connected successfully.")
    return {
        "status": "True",
        "charger_id": charger_id,
        "message": "Charger connected successfully.",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/start-session")
async def start_session(charger_id: str):
    global charging_task
    if connected_charger_id != charger_id:
        return{"status":"False", "message":"Charger not connected or ID Mismatch"}

    if charger_status["status"] == "Charging":
         return{"status":"False", "message":"Charger is already in use"}

    charger_status["status"] = "Charging"
    charger_status["current_session"] = {
        "charger_id": charger_id,
        "start_time": datetime.utcnow().isoformat()
    }
    charger_status["charging_progress"]["battery_level"] = 0
    charger_status["charging_progress"]["estimated_time_left"] = 50

    charging_task = asyncio.create_task(simulate_charging())
    logger.info(f"Started charging session for charger {charger_id}")
    return {"status":"True","message": "Charging started", "session": charger_status["current_session"]}

@app.get("/charging-ui-status")
async def get_ui_status(request: Request):
    session = charger_status["current_session"]
    battery = charger_status["charging_progress"]["battery_level"]
    estimated_left = charger_status["charging_progress"]["estimated_time_left"]

    if not session:
        return {
            "status": "False",
            "message": "No active session"
        }

    start_time = datetime.fromisoformat(session["start_time"])
    now = datetime.utcnow()
    session_duration_min = int((now - start_time).total_seconds() // 60)
    cost = round(battery * 0.20, 2)
    status_label = "Charging Full" if battery == 100 else "Charging"

    return {
        "status":"True",
        "status_lab": status_label,
        "battery_level": battery,
        "estimated_time_left": "Fully charged" if battery == 100 else f"{estimated_left} min approx",
        "session_duration": f"{session_duration_min} min ago",
        "charging_cost": f"${cost:.2f}",
        "station_name": "EV Station A1",
        "map_location": {
            "lat": 28.6139,
            "lon": 77.2090
        }
    }

@app.post("/stop-session")
async def stop_session():
    global charging_task
    if charger_status["status"] != "Charging":
        return{"status":"False","message":"No active session to stop"}
    if charging_task:
        charging_task.cancel()
        charging_task = None

    session = charger_status["current_session"]
    final_level = charger_status["charging_progress"]["battery_level"]
    cost = round(final_level * 0.20, 2)

    charger_status["status"] = "Available"
    charger_status["current_session"] = None
    charger_status["charging_progress"]["estimated_time_left"] = 0

    logger.info(f"Stopped charging session for charger {session['charger_id']}")
    return {"status":"True",
        "message": "Charging stopped",
        "last_session": session,
        "final_battery_level": final_level,
        "charging_cost": f"${cost:.2f}"
    }

@app.get("/get-charger-third-party-data")
async def get_charger_metadata():
    try:
        timeout = httpx.Timeout(10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get("https://jsonplaceholder.typicode.com/posts")
            response.raise_for_status()
            data = response.json()
            logger.info("Fetched third-party charger metadata.")
            return data
    except httpx.RequestError as e:
        logger.error(f"Request error: {e}")
        raise HTTPException(status_code=500, detail=f"Request error: {e}")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP error: {e.response.text}")
    except httpx.TimeoutException as e:
        logger.error(f"Timeout error: {e}")
        raise HTTPException(status_code=408, detail="Request timed out")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error occurred")
