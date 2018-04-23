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
ENV INDEX_PATH /app/sirius/valis-dist
ENV STATIC_PATH /app/sirius/valis-dist/static
ENV STATIC_URL /static
ENV LISTEN_PORT 5000



# Setup home environment

RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    unzip \
    git \
    && apt-get clean \
    && apt-get purge -y \
    && rm -rf /bar/lib/apt/lists*


# Optional components to enable (defaults to empty).
ARG enable
# Release version number of TileDB to install.
ARG version=1.2.2
# Release version number of TileDB-Py to install.
ARG pyversion=0.1.1

RUN mkdir /tiledb
RUN apt-get install sudo

RUN adduser --disabled-password --gecos '' tiledb
RUN adduser tiledb sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
RUN chown -R tiledb: /tiledb
RUN chown -R tiledb: /usr/local/lib/
USER tiledb

# Install TileDB
RUN wget -P /tiledb https://github.com/TileDB-Inc/TileDB/archive/${version}.tar.gz \
    && tar xzf /tiledb/${version}.tar.gz -C /tiledb \
    && rm /tiledb/${version}.tar.gz \
    && cd /tiledb/TileDB-${version} \
    && mkdir build \
    && cd build \
    && ../scripts/install-deps.sh --enable=${enable} \
    && ../bootstrap --prefix=/usr/local --enable=${enable} \
    && make \
    && make examples \
    && make install

# Install Python bindings
RUN wget -P /tmp https://bootstrap.pypa.io/get-pip.py \
    && python3 /tmp/get-pip.py \
    && rm /tmp/get-pip.py \
    && cd /tiledb \
    && pip download tiledb==${pyversion} \
    && tar xzf /tiledb/tiledb-${pyversion}.tar.gz -C /tiledb \
    && rm /tiledb/tiledb-${pyversion}.tar.gz \
    && cd /tiledb/tiledb-${pyversion} \
    && pip install -r requirements_dev.txt \
    && pip install .

ENV LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

# Run app.py when the container launches
WORKDIR /app/sirius
CMD ["/start.sh"]
