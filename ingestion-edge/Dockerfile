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
RUN echo 'deb http://deb.debian.org/debian buster-backports main' >> /etc/apt/sources.list && \
  apt-get update && \
  apt-get install -qqy --target-release buster-backports wrk
COPY --from=0 /usr/local /usr/local
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD exec gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class sanic.worker.GunicornWorker \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
