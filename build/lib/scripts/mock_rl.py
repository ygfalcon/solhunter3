from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True, "component": "mock_rl"}


class Obs(BaseModel):
    state: dict | None = None


@app.post("/decide")
def decide(obs: Obs):
    # Always return a benign "hold" decision in paper mode
    return {"action": "hold", "confidence": 0.5}
