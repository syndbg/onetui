# RabbitMQ

OneTUI reads the [management HTTP API](https://www.rabbitmq.com/docs/http-api-reference). The management plugin must be enabled. It does not use AMQP or retrieve messages.

```toml
[connections.rabbit]
kind = "rabbitmq"
url = "https://rabbit.example.com:15671"
username_env = "RABBITMQ_USERNAME"
password_env = "RABBITMQ_PASSWORD"
# ca_file = "/path/to/ca.pem"
```

Use `onetui schema --datasource rabbitmq` for settings and resources. The URL is an origin, without `/api`. HTTPS verifies the hostname and system trust roots, or the supplied PEM CA file. HTTP is allowed only on loopback. Redirects are not followed.

Give the account the `monitoring` tag and access to the required virtual hosts. Empty `configure`, `write` and `read` permission patterns (`^$`) permit metadata inspection without application reads or writes. See [management permissions](https://www.rabbitmq.com/docs/management#permissions). OneTUI preserves native permission errors.

## Browse

The resource menu offers overview, nodes, virtual hosts, queues, exchanges, bindings, connections, channels, consumers and policies. Open a virtual host to narrow these lists. Tables show common fields. Enter opens the row, including `details` with all JSON returned by that endpoint. Missing metrics remain NULL.

Pages contain at most 100 rows. Responses and displayed pages are limited to 1 MiB. Lists use server pagination when available and bounded local pages otherwise. `n` and `p` move between pages. Refresh rereads current state. These are not snapshots, and node information is reported by the configured management endpoint rather than checked by connecting to each node.

There are no message-get/requeue requests, subscriptions, acknowledgements, publishing or administration. User definitions and password hashes are not fetched. RabbitMQ plugins beyond the management API are not covered.

## Local demo

Use `make dev-up`, then `make dev-run` and choose `local_rabbitmq`. The fixture has paged classic queues, quorum/stream queues, bindings, a policy and a Unicode virtual host. A fixture-only AMQP client keeps connection, channel and consumer metrics populated. See [local fixtures](../hack/README.md).
