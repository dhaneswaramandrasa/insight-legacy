# from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

class RegistrationDataInsightResource:
    @staticmethod
    def response(data=None, message="Request processed successfully", status_code=200, error_code=0):
        # json_data = jsonable_encoder(data)
        data_array = [{
            "is_datainsight" : data[1],
            "description" : "Active" if data[1] == 1 else "Inactive"
        }]
        return JSONResponse(
            content={
                "data": data_array,
                "status": status_code,
                "message": message,
                "error": error_code
            },
            status_code=status_code
        )
