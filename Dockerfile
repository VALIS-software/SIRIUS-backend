# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Install Ensemble data for pyensemble package, this is quite slow!
RUN pyensembl install --release 76 --species homo_sapiens

# We configured a persistent disk and mount as read-only at /pd
# But pyensembl want both read/write to cache
# The source code view.py has been changed so it will try to copy /pd into /cache
# ENV PYENSEMBL_CACHE_DIR /cache

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
#ENV FLASK_APP main.py

# Run app.py when the container launches
WORKDIR /app
CMD ["python", "run.py"]
