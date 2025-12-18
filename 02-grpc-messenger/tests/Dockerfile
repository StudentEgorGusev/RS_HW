FROM python:3.12-alpine

COPY tests/requirements.txt .
RUN pip install -r requirements.txt

RUN apk add curl
RUN curl -sSL "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_x86_64.tar.gz" | tar -xz -C /usr/local/bin

COPY messenger/proto/messenger.proto messenger/proto/messenger.proto
COPY tests/*.py tests/

ENTRYPOINT ["python3", "tests/main.py"]
