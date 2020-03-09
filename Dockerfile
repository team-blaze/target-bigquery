FROM python:3.8-buster

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /target_bigquery
LABEL python_version=python

RUN mkdir -p /app-config
ARG SERVICE_ACCOUNT_JSON_BASE64
RUN echo $SERVICE_ACCOUNT_JSON_BASE64 | base64 --decode > /app-config/service_account.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app-config/service_account.json"

RUN pip install pytest

COPY Makefile requirements.txt ./

RUN pip install -r requirements.txt

ARG GOOGLE_PROJECT_ID
ENV GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID}

COPY . .

CMD ["pytest"]
