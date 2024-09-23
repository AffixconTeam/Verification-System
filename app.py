from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from main import verify_user  # Import the verification logic
import uvicorn

app = FastAPI()

# Define a Pydantic model for the input
class UserData(BaseModel):
    first_name: str
    middle_name: str
    sur_name: str
    dob: str
    address_line1: str
    suburb: str
    state: str
    postcode: str
    mobile: str
    email: str

# Define the verification endpoint
@app.post("/verify/")
def verify(data: UserData):
    # Call the verification logic from main.py
    try:
        result = verify_user(data.dict())  # Pass the data as a dictionary
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to the Data Verification API"}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)