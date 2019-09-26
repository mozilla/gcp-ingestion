FROM python:3.7-stretch
WORKDIR /app
RUN apt-get update && apt-get install -y wrk
COPY constraints.txt requirements.txt /app/
COPY bin/include/common.sh /app/bin/include/
COPY bin/build /app/bin/
ENV VENV=false
RUN bin/build
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD exec gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class sanic.worker.GunicornWorker \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
