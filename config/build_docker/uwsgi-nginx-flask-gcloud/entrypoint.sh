#! /usr/bin/env bash
set -e
# Get the maximum upload file size for Nginx, default to 0: unlimited
USE_NGINX_MAX_UPLOAD=${NGINX_MAX_UPLOAD:-0}
# Generate Nginx config for maximum upload file size
echo "client_max_body_size $USE_NGINX_MAX_UPLOAD;" > /etc/nginx/conf.d/upload.conf

# Get the URL for static files from the environment variable
USE_STATIC_URL=${STATIC_URL:-'/static'}
# Get the absolute path of the static files from the environment variable
USE_STATIC_PATH=${STATIC_PATH:-'/app/static'}
# Get the listen port for Nginx, default to 80
USE_LISTEN_PORT=${LISTEN_PORT:-80}

# If AUTH_BASIC_USER_FILE is set, add auth_basic to index page
AUTH=""
if [ -v NGINX_USE_AUTH ] ; then
    AUTH=$'auth_basic "Restricted Content";\n\tauth_basic_user_file /etc/nginx/.htpasswd;'
fi

# Generate Nginx config first part using the environment variables
echo "server {
    listen ${USE_LISTEN_PORT};
    uwsgi_read_timeout 3600;
    root /;
    location / {
        try_files \$uri @app;
        $AUTH
    }
    location @app {
        include uwsgi_params;
        uwsgi_pass unix:///tmp/uwsgi.sock;
    }
    location $USE_STATIC_URL {
        alias $USE_STATIC_PATH;
    }" > /etc/nginx/conf.d/nginx.conf

# If INDEX_PATH is set, serve / with $INDEX_PATH/index.html directly (or the static URL configured)
if [ -v INDEX_PATH ] ; then
echo "    location = / {
        index $INDEX_PATH/index.html;
    }" >> /etc/nginx/conf.d/nginx.conf
fi

# Finish the Nginx config file
echo "}" >> /etc/nginx/conf.d/nginx.conf

exec "$@"
