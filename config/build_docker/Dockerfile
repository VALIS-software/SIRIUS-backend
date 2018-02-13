# Use an official Python runtime as a parent image
FROM tiangolo/uwsgi-nginx-flask:python3.6

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# We use our own entrypoint that will generate Nginx additional configs
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
# This file will be inherited by newly-built images to keep the functionality
ENTRYPOINT ["/entrypoint.sh"]

# Clean up WORKDIR
RUN rm -r *

# The supervisord has been configured to start Nginx and uWSGI
CMD ["/usr/bin/supervisord"]
