FROM docker:dind

RUN apk add --no-cache bash go

WORKDIR /hw
COPY . tests
RUN cd tests && go mod download && go build .
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["timeout", "-k", "10", "900", "entrypoint.sh"]