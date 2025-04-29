# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependencies and source code
COPY requirements.txt .
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create download directory
RUN mkdir -p downloads

# Run the analyzer
CMD ["python", "main.py"]
