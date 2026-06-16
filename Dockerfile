# Use a lightweight version of Python
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your actual Python script into the container
COPY main.py .

# Command to run your script when the container starts
CMD ["python", "main.py"]