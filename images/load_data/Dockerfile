FROM python:3.9

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY load_data.py ./
COPY configs.py ./
COPY query.sql ./
ENTRYPOINT ./load_data.py

