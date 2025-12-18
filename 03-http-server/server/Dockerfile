FROM python:3.12-slim

WORKDIR /http/server

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "server.py"]
