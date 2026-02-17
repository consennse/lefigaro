from fastapi import FastAPI, Body
import main

app = FastAPI()

@app.post("/run")
def run(body: dict = Body(default={})):
    try:
        main.run_pipeline()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
