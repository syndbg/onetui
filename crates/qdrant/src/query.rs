use onetui_core::catalog::ResourceDescriptor;

pub const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.query",
    description: "HTTP request response from the configured REST endpoint",
    columns: &["status", "body"],
    paging: true,
    actions: &[],
};
