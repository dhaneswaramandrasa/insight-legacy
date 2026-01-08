# schemas/StoringRequest.py

from pydantic import BaseModel

class StoringRequest(BaseModel):
    store_id: int
    flag: int
