import os
from pathlib import Path

import uvicorn


if __name__ == "__main__":
    current_dir = Path(__file__).resolve().parent
    os.chdir(current_dir)
    uvicorn.run("main:app", host="0.0.0.0", port=9000, reload=False)
