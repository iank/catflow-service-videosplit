FROM python:3.10 as base

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir .

EXPOSE 5057

CMD ["catflow-service-videosplit"]
