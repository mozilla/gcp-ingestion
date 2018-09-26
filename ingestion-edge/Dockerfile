FROM python:3.7
WORKDIR /app
COPY requirements.txt /app
RUN pip install --upgrade --no-cache-dir -r requirements.txt
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD $CMD_PREFIX gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class ${GUNICORN_WORKER_CLASS:-sync} \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
