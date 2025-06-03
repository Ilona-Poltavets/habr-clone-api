from fastapi import FastAPI, Request, Body, APIRouter

app=FastAPI()


@app.get("/login")
def login(request: Request):
    return {"message": "Login page"}

@app.get("/register")
def register(request: Request):
    return {"message": "Register page"}

@app.get("/status")
def status(request: Request):
    return {"message": "Fastapi running!"}