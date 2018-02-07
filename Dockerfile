# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory
WORKDIR /home/app/webapp

# Copy the current directory contents into the container
ADD . /home/app/webapp

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
#ENV NAME World

# Run app.py when the container launches
WORKDIR /home/app/webapp/src
CMD ["python", "app.py"]
