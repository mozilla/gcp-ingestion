ARG PYTHON_VERSION=3.8

# build requirements in separate stage because it requires gcc and libc-dev
FROM python:${PYTHON_VERSION}-slim
WORKDIR /app
RUN apt-get update && apt-get install -qqy gcc libc-dev
COPY requirements.txt /app/
COPY bin/include/common.sh /app/bin/include/
COPY bin/build /app/bin/
ENV VENV=false
RUN bin/build

FROM python:${PYTHON_VERSION}-slim
WORKDIR /app
# As of debian 10 there is no wrk package available for debian, so this relies
# on relud/wrk to build wrk from source on debian 10:
# https://github.com/relud/docker-wrk/blob/master/Dockerfile
COPY --from=relud/wrk:4.1.0-buster /usr/local/bin/wrk /usr/local/bin/wrk
COPY --from=0 /usr/local /usr/local
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD exec gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class sanic.worker.GunicornWorker \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
