FROM python:3.9-slim

WORKDIR /app

# Install kafka-python with admin support
RUN pip install kafka-python

# Copy the producer script
COPY producer.py .

# Run the producer
CMD ["python", "producer.py"]
