#!/usr/bin/env python3
import sys
import os
import uvicorn
import pyspark

# Set environment for Docker - use PySpark from pip
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["PATH"] = f"{os.environ['SPARK_HOME']}/bin:{os.environ['PATH']}"

# Import and run the FastAPI app
sys.path.insert(0, '/app/api_services')

if __name__ == "__main__":
    uvicorn.run("asim_test:app", host="0.0.0.0", port=8003, reload=False, loop="asyncio")
