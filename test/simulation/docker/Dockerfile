FROM alpine:3.20

RUN apk add --no-cache \
    bash \
    zsh \
    iproute2 \
    iputils \
    socat \
    curl \
    coreutils \
    procps \
    ca-certificates

WORKDIR /workspace

COPY bin/croc-linux /usr/local/bin/croc
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY tc_shape.sh /usr/local/bin/tc_shape.sh

RUN chmod +x /usr/local/bin/croc /usr/local/bin/entrypoint.sh /usr/local/bin/tc_shape.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["sleep", "infinity"]
