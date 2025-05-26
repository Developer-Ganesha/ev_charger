import asyncio
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
import httpx
import websockets
from ocpp.routing import on
from ocpp.v16 import ChargePoint as OcppChargePoint
from ocpp.v16.enums import RegistrationStatus
from ocpp.v16 import call_result
import threading

# -------------------- Logging Setup --------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ev-charger")

# -------------------- FastAPI App --------------------
app = FastAPI()

# -------------------- Charger Status --------------------
charger_status = {
    "charger_id": "EV123",
    "status": "Available",
    "current_session": None,
    "charging_progress": {
        "battery_level": 0,
        "estimated_time_left": 0,  # in minutes
        "charging_power": 7.2  # kW (for simulation)
    }
}

charging_task = None

# -------------------- Charging Simulation --------------------
async def simulate_charging():
    try:
        while charger_status["status"] == "Charging":
            await asyncio.sleep(5)

            level = charger_status["charging_progress"]["battery_level"]

            if level >= 100:
                charger_status["charging_progress"]["battery_level"] = 100
                charger_status["charging_progress"]["estimated_time_left"] = 0
                break

            charger_status["charging_progress"]["battery_level"] += 2
            charger_status["charging_progress"]["estimated_time_left"] = int(
                (100 - charger_status["charging_progress"]["battery_level"]) * 0.5
            )

            logger.info(f"Charging... {charger_status['charging_progress']['battery_level']}%")

        charger_status["status"] = "Available"
        charger_status["current_session"] = None

    except asyncio.CancelledError:
        logger.info("Charging simulation cancelled.")

# -------------------- OCPP ChargePoint Class --------------------
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

# -------------------- OCPP Server --------------------
async def ocpp_server(websocket, path):
    charge_point_id = path.strip("/")
    cp = ChargePoint(charge_point_id, websocket)
    await cp.start()

def start_ocpp_server():
    async def run():
        server = await websockets.serve(
            ocpp_server, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]
        )
        logger.info("OCPP Server running on ws://0.0.0.0:9000")
        await server.wait_closed()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())

ocpp_thread = threading.Thread(target=start_ocpp_server, daemon=True)
ocpp_thread.start()

# -------------------- FastAPI Endpoints --------------------
@app.get("/")
def home():
    return {"message": "EV Charger Simulator Running"}

@app.post("/start-session")
async def start_session(charger_id: str):
    global charging_task
    if charger_status["status"] == "Charging":
        raise HTTPException(status_code=400, detail="Charger is already in use")

    charger_status["status"] = "Charging"
    charger_status["current_session"] = {
        "charger_id": charger_id,
        "start_time": datetime.utcnow().isoformat()
    }
    charger_status["charging_progress"]["battery_level"] = 0
    charger_status["charging_progress"]["estimated_time_left"] = 50  # example

    charging_task = asyncio.create_task(simulate_charging())

    logger.info(f"Started charging session for charger {charger_id}")
    return {"message": "Charging started", "session": charger_status["current_session"]}

@app.post("/stop-session")
async def stop_session():
    global charging_task
    if charger_status["status"] != "Charging":
        raise HTTPException(status_code=400, detail="No active session to stop")

    if charging_task:
        charging_task.cancel()
        charging_task = None

    session = charger_status["current_session"]
    final_level = charger_status["charging_progress"]["battery_level"]

    charger_status["status"] = "Available"
    charger_status["current_session"] = None
    charger_status["charging_progress"]["estimated_time_left"] = 0

    logger.info(f"Stopped charging session for charger {session['charger_id']}")
    return {
        "message": "Charging stopped",
        "last_session": session,
        "final_battery_level": final_level
    }

@app.get("/charging-ui-status")
async def get_ui_status(request: Request):
    if charger_status["status"] != "Charging" or not charger_status["current_session"]:
        return {
            "status": "Available",
            "message": "No active session"
        }

    start_time_str = charger_status["current_session"]["start_time"]
    start_time = datetime.fromisoformat(start_time_str)
    now = datetime.utcnow()
    session_duration_min = int((now - start_time).total_seconds() // 60)

    battery = charger_status["charging_progress"]["battery_level"]
    estimated_left = charger_status["charging_progress"]["estimated_time_left"]
    
    # Simulated cost: $0.20 per 1% charge
    cost = round(battery * 0.20, 2)

    return {
        "status": "Charging",
        "battery_level": battery,
        "estimated_time_left": f"{estimated_left} min approx",
        "session_duration": f"{session_duration_min} min ago",
        "charging_cost": f"${cost:.2f}",
        "station_name": "EV Station A1",
        "map_location": {
            "lat": 28.6139, "lon": 77.2090  # sample coords
        }
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
