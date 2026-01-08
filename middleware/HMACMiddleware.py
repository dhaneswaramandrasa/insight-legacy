import hmac
import hashlib
import os
import json
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SECRET_KEY = os.getenv("SECRET_HMAC")

def verify_hmac(hmac_signature: str, data: bytes, secret_key: str) -> bool:
    expected_signature = hmac.new(secret_key.encode(), data, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected_signature, hmac_signature)

class HMACMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Pengecualian untuk URL path yang tidak memiliki prefix '/api'
        if not request.url.path.startswith('/api'):
            return await call_next(request)
        
        hmac_signature = request.headers.get("X-HMAC-Signature")
        if not hmac_signature:
            return JSONResponse(status_code=401, content={"status": 401, "error": 1, "message": "Headers X-HMAC-Signature is required"})
        
        # Mengambil data yang akan diverifikasi berdasarkan jenis konten permintaan
        if "application/json" in request.headers.get("content-type", ""):
            body = await request.body()
            data = json.loads(body.decode())
        elif "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
            data = await request.form()
            data = {k: v for k, v in data.items()}
        elif request.query_params:
            data = "&".join([f"{k}={v}" for k, v in request.query_params.items()])
        else:
            data = b""  # Jika tidak ada data, gunakan data kosong
        
        # Mengonversi data menjadi string JSON dengan urutan yang konsisten
        if isinstance(data, dict):
            # compact_json_str = json.dumps(data, separators=(',', ':'), sort_keys=True)
            compact_json_str = json.dumps(data, separators=(',', ':'))
        else:
            compact_json_str = data

        if isinstance(compact_json_str, bytes):
            compact_json_str = compact_json_str.decode('utf-8')  # atau encoding lain yang se
        
        if not verify_hmac(hmac_signature, compact_json_str.encode(), SECRET_KEY):
            return JSONResponse(status_code=401, content={"status": 401, "error": 1, "message": "HMAC signature mismatch"})

        response = await call_next(request)
        return response
