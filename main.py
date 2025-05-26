from fastapi import FastAPI, Request, Body, APIRouter

app=FastAPI()




@app.get("/status")
def status(request: Request):
    return {"message": "Fastapi running!"}