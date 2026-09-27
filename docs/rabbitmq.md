# RabbitMQ

Inspect overview metrics, nodes, virtual hosts, queues, exchanges, bindings, connections, channels, consumers and policies. Use the query editor to publish, retrieve messages or administer resources through the management HTTP API.

## Configuration

Enable the RabbitMQ management plugin, then configure its HTTP API endpoint:

```toml
[connections.rabbit]
kind = "rabbitmq"
url = "https://rabbit.example.com:15671"
username = "user" # or username_env = "RABBITMQ_USERNAME"
password = "..." # or password_env = "RABBITMQ_PASSWORD"
```

Use `username_env` or `password_env` instead to read either value from an environment variable. Protect the config file if it contains a password.

Use an origin URL without `/api`. HTTPS verifies certificates; HTTP is allowed only on loopback. Use `onetui schema --datasource rabbitmq` for settings, including private CA support.

Give the account the `monitoring` tag and access to the required virtual hosts. Empty `configure`, `write` and `read` permission patterns (`^$`) allow metadata inspection without application reads or writes. See [RabbitMQ management permissions](https://www.rabbitmq.com/docs/management#permissions).

## Browse

Open a virtual host to narrow its resources. Enter on a row exposes the full returned details. Missing metrics remain NULL. The views reflect the configured management endpoint's observations.

Try `local_rabbitmq` in the [demo fixtures](../hack/README.md).

## Queries

Press `e` from a RabbitMQ view. Put a verb and its arguments on the first line, then a blank line and an optional body:

```
PUBLISH / amq.default demo

hello
```

Virtual hosts and names are literals. The default virtual host is `/`, and OneTUI encodes it for the wire, so you never type `%2F`.

`object` is `queue`, `exchange`, `binding`, `policy`, `vhost` or anything else the management API accepts. OneTUI pluralises the word and sends it without checking it against a list, so RabbitMQ reports an object kind or count it does not support.

| Verb | Form |
| --- | --- |
| `PUBLISH` | `PUBLISH vhost exchange routing_key`, body is the payload. Write an empty routing key as `""`. |
| `GET` | `GET vhost queue [count] [requeue\|ack]`. Destructive read, by default one message put back. |
| `DECLARE` | `DECLARE object vhost name`, or `DECLARE vhost name`. The JSON body is the definition; an empty body means defaults. |
| `DELETE` | `DELETE object vhost name`, or `DELETE vhost name`. |
| `PURGE` | `PURGE vhost queue` removes every ready message. |
| `RAW` | `RAW METHOD /path` with an optional JSON body. |

Add an `Onetui-Encoding: base64` or `hex` header line to `PUBLISH` for a payload that is not valid UTF-8, as in the [NATS editor](nats.md). `GET` returns payloads as bytes.

`RAW` reaches endpoints the verbs above do not model, such as users, permissions and node operations. Its paths are native and include `/api`, so a literal `/` inside a name is percent-encoded as `%2F` there:

```
RAW PUT /api/users/alice

{"tags":"monitoring"}
```

See the [RabbitMQ HTTP API](https://www.rabbitmq.com/docs/http-api-reference) for native request formats and required permissions. The metadata-only fixture account cannot write.

Reads return a page. Writes report applied or rejected with the native status and reason, so a permission error is a rejection rather than a failure. A publish the broker accepted but routed nowhere says so. Requests stay on the configured endpoint and are not retried automatically. If a write has an unknown outcome, inspect the target before resubmitting.

Use the [shared editor controls](ui.md#query-editor) for confirmation, cancellation and history.
