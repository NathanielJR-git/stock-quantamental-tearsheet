from fastapi import FastAPI


app = FastAPI()


@app.get("/api/tearsheet-data")
async def get_tearsheet_data():
    ...
    
    
@app.get("/api/chart-data/{ticker}")
async def get_tearsheet_data(ticker: str):
    ...