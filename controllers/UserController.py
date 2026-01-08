from fastapi import Request, Depends, HTTPException
from resources.EmployeeResource import EmployeeData, EmployeeResourceList, EmployeeResourceSingle

class UserController:
    @staticmethod
    async def GetUser(connection):
        cursor = connection.cursor()
        cursor.execute("SELECT id, name, email FROM ci_user LIMIT 10")
        result = cursor.fetchall()

        data = [{"id": row[0], "name": row[1], "email": row[2]} for row in result]
        response_data = [EmployeeData(**row) for row in data]
        cursor.close()
        response = EmployeeResourceList(data=response_data, status=200, error=0)
        return response  # Return a single instance of EmployeeData

    @staticmethod
    async def GetUserData(request: Request, connection):
        cursor = connection.cursor()
        body = await request.json()
        id = body.get("id")

        if id is None:
            raise HTTPException(status_code=400, detail="id is required")

        query = "SELECT id, name, email FROM ci_user WHERE id = %s"
        cursor.execute(query, (id,))
        result = cursor.fetchone()

        if result is None:
            raise HTTPException(status_code=404, detail="Data not found")
        
        response_data = {"id": result[0], "name": result[1], "email": result[2]}
        response = EmployeeResourceSingle(data=response_data, status=200, error=0)
        cursor.close()
        return response  # Return a single instance of EmployeeData
