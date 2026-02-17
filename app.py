from fastapi import FastAPI
import main

app = FastAPI()

@app.post("/run")
def run():
    try:
        main.run_pipeline()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
