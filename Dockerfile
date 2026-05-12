FROM python:3.12-slim

WORKDIR /app

# Install system dependencies if required
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and alembic config first to cache the pip install step
COPY requirements.txt alembic.ini ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend source code into the container
# Since backend is a package (backend.main), we copy it into /app/backend
COPY . ./backend

# Expose the API port
EXPOSE 8080

# Command to run the application using the package path
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
