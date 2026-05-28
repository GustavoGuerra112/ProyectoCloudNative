from celery import Celery
import redis
import json
import time

app = Celery(
    "worker",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0",
)

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


@app.task(name="worker.analyze_text")
def analyze_text(task_id: str, text: str):
    """
    Esta función la ejecuta el worker cuando recibe una tarea de Redis.
    Pasos:
      1. Marcar tarea como "processing"
      2. Contar palabras, caracteres, oraciones
      3. Simular trabajo con time.sleep (en la vida real sería ML, etc.)
      4. Guardar resultado en Redis
      5. Marcar tarea como "completed"
    """
    try:
        redis_client.set(f"task:{task_id}:status", "processing")
        print(f"[Worker] Procesando tarea {task_id[:8]}...")

        time.sleep(2)

        words = text.split()
        sentences = [s.strip() for s in text.replace("!", ".").replace("?", ".").split(".") if s.strip()]
        unique_words = list(set(word.lower().strip(".,;:!?") for word in words))

        top_words = sorted(unique_words, key=len, reverse=True)[:5]

        result = {
            "total_characters": len(text),
            "total_characters_no_spaces": len(text.replace(" ", "")),
            "total_words": len(words),
            "total_sentences": len(sentences),
            "unique_words": len(unique_words),
            "average_word_length": round(
                sum(len(w) for w in words) / len(words), 2
            ) if words else 0,
            "top_longest_words": top_words,
            "preview": text[:100] + "..." if len(text) > 100 else text,
        }

        redis_client.set(f"task:{task_id}:result", json.dumps(result))

        redis_client.set(f"task:{task_id}:status", "completed")
        print(f"[Worker] ✅ Tarea {task_id[:8]} completada. Palabras: {len(words)}")

        return result

    except Exception as e:
        redis_client.set(f"task:{task_id}:status", "error")
        redis_client.set(f"task:{task_id}:result", json.dumps({"error": str(e)}))
        print(f"[Worker] ❌ Error en tarea {task_id[:8]}: {e}")
        raise
