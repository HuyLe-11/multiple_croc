FROM alpine:3.20
RUN apk add --no-cache bash iproute2 iputils coreutils procps curl ca-certificates
COPY croc-base croc-fix tc_shape.sh sampler.sh /usr/local/bin/
RUN sed -i 's/\r$//' /usr/local/bin/tc_shape.sh /usr/local/bin/sampler.sh && chmod +x /usr/local/bin/croc-base /usr/local/bin/croc-fix /usr/local/bin/tc_shape.sh /usr/local/bin/sampler.sh
WORKDIR /data
CMD ["sleep", "infinity"]
