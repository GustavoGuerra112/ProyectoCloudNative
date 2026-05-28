from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from celery import Celery
import redis
import json
import uuid
import asyncio

app = FastAPI(title="Analizador de Texto Distribuido")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

celery_app = Celery(
    "tasks",
    broker="redis://redis:6379/0",  
    backend="redis://redis:6379/0",  
)


@app.post("/analyze")
async def analyze_text(data: dict):
    """
    Recibe el texto del frontend.
    Crea un ID único para la tarea.
    La envía a Redis para que un worker la procese.
    """
    text = data.get("text", "")
    task_id = str(uuid.uuid4())  

    redis_client.set(f"task:{task_id}:status", "pending")
    redis_client.set(f"task:{task_id}:text", text)

    celery_app.send_task("worker.analyze_text", args=[task_id, text])

    return {"task_id": task_id, "status": "pending"}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    """
    El frontend se conecta aquí y recibe actualizaciones en tiempo real.
    SSE mantiene la conexión abierta y envía eventos cuando cambia el estado.
    """
    async def event_generator():
        while True:
            status = redis_client.get(f"task:{task_id}:status")
            result = redis_client.get(f"task:{task_id}:result")

            event_data = {
                "task_id": task_id,
                "status": status or "unknown",
                "result": json.loads(result) if result else None,
            }
            yield f"data: {json.dumps(event_data)}\n\n"

            if status in ["completed", "error"]:
                break

            await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/")
async def health():
    return {"status": "ok", "message": "Backend funcionando"}
