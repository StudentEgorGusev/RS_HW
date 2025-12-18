FROM python:3.12-slim

WORKDIR /grpc-messenger

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . messenger

RUN python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. messenger/proto/messenger.proto

ENTRYPOINT ["python", "-m", "messenger.client.client"]
