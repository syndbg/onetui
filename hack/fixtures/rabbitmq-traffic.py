"""Keep disposable connection, channel and consumer metrics visible."""

import json
import time

import pika


connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host="rabbitmq",
        credentials=pika.PlainCredentials("fixture-admin", "fixture-admin-only"),
        client_properties={"connection_name": "onetui-fixture-traffic"},
        heartbeat=30,
        blocked_connection_timeout=5,
    )
)
channel = connection.channel()
channel.queue_declare(queue="demo_activity", durable=True)
channel.basic_consume(
    queue="demo_activity",
    on_message_callback=lambda _channel, _method, _properties, _body: None,
    auto_ack=True,
    consumer_tag="onetui-fixture-consumer",
)
sequence = 0
while connection.is_open:
    channel.basic_publish(
        exchange="",
        routing_key="demo_activity",
        body=json.dumps({"sequence": sequence, "message": "Synthetic fixture event"}).encode(),
    )
    print(f"RabbitMQ fixture sequence={sequence}", flush=True)
    sequence += 1
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        connection.process_data_events(time_limit=1)
