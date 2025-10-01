FROM python:3.12.9-slim

# System setup
RUN apt-get update && apt-get install -y build-essential gcc

RUN mkdir /FraudOut
COPY . /FraudOut/
WORKDIR /FraudOut/

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
RUN pip install poetry
RUN poetry sync --no-root