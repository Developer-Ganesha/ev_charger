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

app = FastAPI(title="EV Charger API")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ev-charger")

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

charger_status = {}
charging_tasks = {}

async def notify_clients(message: str):
    to_remove = []
    for ws in charging_ws_clients:
        try:
            await ws.send_text(message)
        except Exception:
            to_remove.append(ws)
    for ws in to_remove:
        charging_ws_clients.remove(ws)

async def simulate_charging(charger_id: str):
    try:
        while charger_status[charger_id]["status"] == "Charging":
            await asyncio.sleep(5)
            level = charger_status[charger_id]["charging_progress"]["battery_level"]

            if level >= 100:
                charger_status[charger_id]["charging_progress"]["battery_level"] = 100
                charger_status[charger_id]["charging_progress"]["estimated_time_left"] = 0
                await notify_clients(f"Charging Complete for {charger_id}: 100%")
                break

            charger_status[charger_id]["charging_progress"]["battery_level"] += 0.2
            charger_status[charger_id]["charging_progress"]["estimated_time_left"] = int(
                (100 - charger_status[charger_id]["charging_progress"]["battery_level"]) * 0.2
            )

            logger.info(f"{charger_id} Charging... {charger_status[charger_id]['charging_progress']['battery_level']}%")

    except asyncio.CancelledError:
        logger.info(f"Charging simulation cancelled for {charger_id}.")

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

@app.post("/connect-charger")
async def connect_charger(charger_id: str):
    if charger_id not in charger_status:
        charger_status[charger_id] = {
            "status": "Available",
            "current_session": None,
            "charging_progress": {
                "battery_level": 0,
                "estimated_time_left": 0,
                "charging_power": 7.2
            }
        }
    logger.info(f"Charger {charger_id} connected successfully.")
    return {
        "status": "True",
        "charger_id": charger_id,
        "message": "Charger connected successfully.",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/start-session")
async def start_session(charger_id: str):
    if charger_id not in charger_status:
        return {"status": "False", "message": "Charger not connected"}

    if charger_status[charger_id]["status"] == "Charging":
        return {"status": "False", "message": "Charger already in use"}

    charger_status[charger_id]["status"] = "Charging"
    charger_status[charger_id]["current_session"] = {
        "charger_id": charger_id,
        "start_time": datetime.utcnow().isoformat()
    }

    charger_status[charger_id]["charging_progress"].setdefault("battery_level", 0)
    charger_status[charger_id]["charging_progress"]["estimated_time_left"] = 50

    task = asyncio.create_task(simulate_charging(charger_id))
    charging_tasks[charger_id] = task

    logger.info(f"Started charging session for charger {charger_id}")
    return {"status": "True", "message": "Charging started", "session": charger_status[charger_id]["current_session"]}

@app.get("/charging-ui-status")
async def get_ui_status():
    for charger_id, data in charger_status.items():
        if data["current_session"]:
            session = data["current_session"]
            battery = data["charging_progress"]["battery_level"]
            estimated_left = data["charging_progress"]["estimated_time_left"]

            start_time = datetime.fromisoformat(session["start_time"])
            now = datetime.utcnow()
            session_duration_min = int((now - start_time).total_seconds() // 60)
            cost = round(battery * 0.10, 1)
            status_label = "Charging Full" if battery == 100 else "Charging"

            return {
                "status": "True",
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

    return {"status": "False", "message": "No active session"}

@app.post("/stop-session")
async def stop_session():
    for charger_id, data in charger_status.items():
        if data["status"] == "Charging":
            task = charging_tasks.get(charger_id)
            if task:
                task.cancel()
                charging_tasks.pop(charger_id, None)

            session = data["current_session"]
            final_level = data["charging_progress"]["battery_level"]
            cost = round(final_level * 0.20, 2)

            data["status"] = "Available"
            data["current_session"] = None
            data["charging_progress"]["estimated_time_left"] = 0

            logger.info(f"Stopped charging session for charger {charger_id}")
            return {
                "status": "True",
                "message": "Charging stopped",
                "last_session": session,
                "final_battery_level": final_level,
                "charging_cost": f"${cost:.2f}"
            }

    return {"status": "False", "message": "No active session to stop"}

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