# RabbitMQ

Inspect overview metrics, nodes, virtual hosts, queues, exchanges, bindings, connections, channels, consumers and policies. Message inspection, publishing and administration are not supported.

## Configuration

Enable the RabbitMQ management plugin, then configure its HTTP API endpoint:

```toml
[connections.rabbit]
kind = "rabbitmq"
url = "https://rabbit.example.com:15671"
username_env = "RABBITMQ_USERNAME"
password_env = "RABBITMQ_PASSWORD"
```

Use an origin URL without `/api`. HTTPS verifies certificates; HTTP is allowed only on loopback. Use `onetui schema --datasource rabbitmq` for settings, including private CA support.

Give the account the `monitoring` tag and access to the required virtual hosts. Empty `configure`, `write` and `read` permission patterns (`^$`) allow metadata inspection without application reads or writes. See [RabbitMQ management permissions](https://www.rabbitmq.com/docs/management#permissions).

## Browse

Open a virtual host to narrow its resources. Enter on a row exposes the full returned details. Missing metrics remain NULL. The views reflect the configured management endpoint's observations.

Try `local_rabbitmq` in the [demo fixtures](../hack/README.md).
