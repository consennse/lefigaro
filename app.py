from fastapi import FastAPI
import main
from logger import log_buffer

app = FastAPI()

@app.post("/run")
def run():
    portal = "le_figaro"

    log_buffer.clear()  # reset logs

    main.run_pipeline(portal)

    return {
        "status": "success",
        "logs": log_buffer
    }