# Use an official Python runtime
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the publisher script
COPY publisher.py .

# Run the publisher
CMD ["python", "publisher.py"]
