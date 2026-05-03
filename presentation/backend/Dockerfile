FROM python:3.11-slim

WORKDIR /app

# Copy all dependencies
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Expose to port 8000
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "athena_api:app", "--host", "0.0.0.0", "--port", "8000"]