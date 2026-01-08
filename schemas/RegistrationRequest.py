# schemas/registration.py

from pydantic import BaseModel
from typing import List

class RegistrationRequest(BaseModel):
    store_id: int

# Kelas kedua untuk store_id sebagai array of integers
class RegistrationRequestList(BaseModel):
    store_id: List[int]