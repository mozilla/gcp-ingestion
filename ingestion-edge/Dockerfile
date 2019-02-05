FROM python:3.7
WORKDIR /app
RUN apt-get update && apt-get install -y wrk
COPY constraints.txt requirements.txt /app/
RUN pip install --upgrade --no-cache-dir -r requirements.txt
# remove after merge and release of https://github.com/mozilla-services/python-dockerflow/pull/27
RUN pip install --no-index git+git://github.com/relud/python-dockerflow@master#egg=dockerflow[sanic]
COPY . /app
ENV HOST=0.0.0.0 PORT=8000
CMD exec gunicorn \
  --bind "$HOST:$PORT" \
  --log-file - \
  --worker-class sanic.worker.GunicornWorker \
  --max-requests ${GUNICORN_MAX_REQUESTS:-0} \
  ingestion_edge.wsgi:app
