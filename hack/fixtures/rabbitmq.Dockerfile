FROM rabbitmq:4.3.5-management
RUN apt update && apt install -y --no-install-recommends openssl curl python3-pika && rm -rf /var/lib/apt/lists/*
