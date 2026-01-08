import json
import math
# import logging
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

class BugsnagMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, bugsnag_client):
        super().__init__(app)
        self.bugsnag_client = bugsnag_client

    async def dispatch(self, request: Request, call_next):
        try:
            # Panggil request berikutnya
            response = await call_next(request)

            # Coba untuk memproses body respons (jika JSON)
            try:
                # Ambil body respons (misalnya dalam bentuk JSON)
                response_body = b""
                async for chunk in response.body_iterator:
                    response_body += chunk

                # Decode dari byte ke string dan parse menjadi JSON
                json_response = response_body.decode()
                response_data = json.loads(json_response)

                # Bersihkan NaN dalam JSON dengan mengganti menjadi nilai valid (misalnya None)
                cleaned_response_data = self.clean_nan_in_data(response_data)

                # Serialisasi ulang data JSON yang sudah dibersihkan
                return JSONResponse(content=cleaned_response_data, status_code=response.status_code)

            except (ValueError, json.JSONDecodeError):
                # Jika respons bukan JSON, kembalikan tanpa modifikasi
                return response

        except Exception as e:
            # Kirim error ke Bugsnag dan kembalikan respons error
            self.bugsnag_client.notify(
                e,
                meta_data={
                    "request": {
                        "url": str(request.url),
                        "method": request.method,
                        "endpoint": request.scope['path'],
                        "headers": dict(request.headers)
                    }
                }
            )
            # Kembalikan respons error yang ramah pengguna
            return JSONResponse(
                status_code=500, 
                content={
                    "status_code": 500,
                    "message": "An unexpected error occurred. Our team has been notified and is working on a fix.",
                    "detail": str(e)
                }
            )

    def clean_nan_in_data(self, data):
        """Bersihkan NaN dan Infinity dalam struktur data."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    data[key] = None  # Ganti NaN atau infinity dengan None (akan menjadi null dalam JSON)
                elif isinstance(value, (dict, list)):
                    self.clean_nan_in_data(value)  # Rekursif untuk struktur nested
        elif isinstance(data, list):
            for i, value in enumerate(data):
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    data[i] = None  # Ganti NaN atau infinity dalam list
                elif isinstance(value, (dict, list)):
                    self.clean_nan_in_data(value)  # Rekursif untuk struktur nested
        return data
