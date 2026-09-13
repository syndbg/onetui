# onetui-schema-source

File, directory, Confluent registry and Buf adapters shared by Kafka and NATS. Decoding stays in `onetui-avro` and `onetui-protobuf`; this crate loads sources and assembles JSON/native previews.

Directory catalogs require Unix: flat `.avsc`/`.pb` inventories, pinned directory handles, no symlink following. Registry caches hold eight exact IDs per binding. Connectors own file/catalog/Buf caches and pass cancellation/deadline checks to loaders.

Use `make test` for package-owned tests and `onetui schema --datasource nats` for source settings and limits.
