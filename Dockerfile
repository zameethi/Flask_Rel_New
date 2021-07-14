FROM python:3.8-slim

RUN apt-get update && \
  apt-get -y install gcc libxml2 sqlite3

RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN  poetry config virtualenvs.create false \
  && poetry install \
  && pip uninstall --yes poetry

COPY . .

EXPOSE 5000

CMD gunicorn -w 4 -b 0.0.0.0:5000 main:app
