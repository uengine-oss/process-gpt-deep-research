"""Server entrypoint (uvicorn main:app)."""
import os

import uvicorn
from src.app import app as fastapi_app

app = fastapi_app

if __name__ == "__main__":
    port = int(os.getenv("PORT", "3000"))
    uvicorn.run("src.app:app", host="0.0.0.0", port=port)
