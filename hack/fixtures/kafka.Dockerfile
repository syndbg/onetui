FROM apache/kafka:4.2.0
USER root
RUN apk add --no-cache krb5 krb5-server
USER appuser
