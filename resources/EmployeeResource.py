from pydantic import BaseModel

# Pydantic model for the employee data
class EmployeeData(BaseModel):
    id: int
    name: str
    email: str

# Pydantic model for the response
class EmployeeResourceList(BaseModel):
    data: list[EmployeeData]
    status: int
    error: int

class EmployeeResourceSingle(BaseModel):
    data: EmployeeData
    status: int
    error: int