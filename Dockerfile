# Use my own runtime as a parent image
FROM yudongdev/uwsgi-nginx-flask:sirius

# Set the working directory (cd)
WORKDIR /app

# Copy the current directory contents into the container
COPY ./app /app

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
ENV FLASK_APP main.py

# We configured a persistent disk and mount as read-only at /pd
# But pyensembl want both read/write to cache
# The source code view.py has been changed so it will try to copy /pd into /cache
ENV PYENSEMBL_CACHE_DIR /cache

# Nginx
ENV INDEX_PATH /app/app/valis-dist
ENV STATIC_PATH /app/app/valis-dist/static
ENV STATIC_URL /static
ENV LISTEN_PORT 5000

# Run app.py when the container launches
WORKDIR /app/app
CMD ["/start.sh"]
