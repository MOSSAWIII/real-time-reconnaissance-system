FROM python:3.9-slim

WORKDIR /app

# Install kafka-python
RUN pip install kafka-python

# Copy the consumer script
COPY consumer.py .

# Run the consumer
CMD ["python", "consumer.py"]
