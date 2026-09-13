pub(crate) use onetui_schema_source::registry::*;

#[cfg(test)]
mod tests {
    #[test]
    fn registry_binding_configuration_is_offline_and_strict() {
        let prefix = "bootstrap_servers=['127.0.0.1:9092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic='events'\nfield='value'\nframing='confluent'\n";
        for invalid in [
            "format='protobuf'\nmessage_name='Event'\nregistry={url='https://registry.test'}",
            "format='avro'\nschema_file='/tmp/schema'\nregistry={url='https://registry.test'}",
            "format='avro'\nregistry={url='http://localhost:8081'}",
            "format='avro'\nregistry={url='https://registry.test', username_env='USER'}",
            "format='avro'\nregistry={url='https://registry.test', token_env='TOKEN', password_env='P', username_env='U'}",
            "format='avro'\nregistry={url='https://registry.test', authorization='secret'}",
        ] {
            let options = toml::from_str(&format!("{prefix}{invalid}")).unwrap();
            assert!(crate::config::Config::parse(&options).is_err());
        }
        let options = toml::from_str("bootstrap_servers=['127.0.0.1:9092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic='events'\nfield='value'\nframing='confluent'\nformat='protobuf'\nregistry={url='https://registry.test'}").unwrap();
        assert!(crate::config::Config::parse(&options).is_ok());
    }
}
