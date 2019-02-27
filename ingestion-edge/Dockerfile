FROM python:3.7
WORKDIR /app
RUN apt-get update && apt-get install -y wrk
COPY constraints.txt requirements.txt /app/
COPY bin/test /app/bin/
ENV VENV=false
RUN bin/test --build --no-pytest
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD exec gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class sanic.worker.GunicornWorker \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
