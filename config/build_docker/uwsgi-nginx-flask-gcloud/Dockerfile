# Use an official Python runtime as a parent image
FROM tiangolo/uwsgi-nginx-flask:python3.6

# setup google cloud
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y -qq --no-install-recommends wget unzip openssh-client python-openssl vim
ENV CLOUDSDK_PYTHON_SITEPACKAGES 1
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.zip
RUN unzip google-cloud-sdk.zip
RUN rm google-cloud-sdk.zip
RUN google-cloud-sdk/install.sh --usage-reporting=true --path-update=true --bash-completion=true --rc-path=/.bashrc --additional-components app-engine-java app-engine-python app kubectl alpha beta gcd-emulator pubsub-emulator cloud-datastore-emulator app-engine-go bigtable
RUN mkdir .ssh
ENV PATH /opt/google-cloud-sdk/bin:$PATH

# install bedtools
RUN apt-get install bedtools

# install tiledb
RUN apt-get install -y sudo cmake
RUN wget -P tiledb https://github.com/TileDB-Inc/TileDB/archive/1.3.0.tar.gz \
    && tar xzf tiledb/1.3.0.tar.gz -C tiledb \
    && rm tiledb/1.3.0.tar.gz \
    && cd tiledb/TileDB-1.3.0 \
    && mkdir build && cd build \
    && ../bootstrap --prefix=/usr/local \
    && make \
    && make install-tiledb
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Install fuzzyset using a separate command
RUN pip install git+https://github.com/axiak/fuzzyset.git#egg=fuzzyset

# We use our own entrypoint that will generate Nginx additional configs
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
# This file will be inherited by newly-built images to keep the functionality
ENTRYPOINT ["/entrypoint.sh"]

# Clean up WORKDIR
RUN rm -r *

# The supervisord has been configured to start Nginx and uWSGI
CMD ["/usr/bin/supervisord"]
