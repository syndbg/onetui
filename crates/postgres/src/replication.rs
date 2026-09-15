use anyhow::{Result, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::{Column, Page, Resource, Row};

pub(crate) const ROOT: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.resources",
    description: "Schemas and read-only replication state",
    columns: &["resource", "description"],
    paging: true,
    actions: &[],
};
pub(crate) const REPLICAS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.replication",
    description: "Connected WAL senders from pg_stat_replication; columns follow the server version",
    columns: &[],
    paging: true,
    actions: &[],
};
pub(crate) const RECEIVER: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.wal_receiver",
    description: "Upstream WAL receiver from pg_stat_wal_receiver; columns follow the server version",
    columns: &[],
    paging: true,
    actions: &[],
};

pub(crate) fn root(resource: &Resource, continuation: Option<&str>) -> Result<Page> {
    ensure!(
        resource.path.is_empty() && continuation.is_none(),
        "PostgreSQL resource menu has no path or continuation"
    );
    Ok(Page {
        columns: ["resource", "description"]
            .into_iter()
            .map(|name| Column {
                name: name.into(),
                datatype: "text".into(),
            })
            .collect(),
        rows: [
            ("Schemas", "Schemas, relations and rows", "postgres.schemas"),
            (
                "Replicas",
                "Connected downstream WAL senders",
                "postgres.replication",
            ),
            (
                "WAL receiver",
                "Connected upstream WAL receiver",
                "postgres.wal_receiver",
            ),
        ]
        .into_iter()
        .map(|(name, description, id)| Row {
            cells: vec![Some(name.into()), Some(description.into())],
            target: Some(Resource::new(id, vec![])),
        })
        .collect(),
        ..Page::default()
    })
}

pub(crate) async fn fetch(
    client: &tokio_postgres::Client,
    resource: &Resource,
    offset: i64,
) -> Result<Page> {
    ensure!(
        resource.path.is_empty(),
        "PostgreSQL replication views have no resource path"
    );
    let sql = match resource.id {
        "postgres.replication" => "SELECT * FROM pg_catalog.pg_stat_replication ORDER BY pid",
        "postgres.wal_receiver" => "SELECT * FROM pg_catalog.pg_stat_wal_receiver ORDER BY pid",
        _ => anyhow::bail!("Unknown PostgreSQL replication resource"),
    };
    let mut page = crate::query::fetch(client, sql, offset).await?;
    page.notice = "Connected replication processes only, not HA membership. PostgreSQL may hide fields without pg_read_all_stats. Independent reads; no replication slots, promotion or configuration changes.".into();
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn menu_targets_are_explicit_and_have_no_continuation() {
        let resource = Resource::new("postgres.resources", vec![]);
        let page = root(&resource, None).unwrap();
        assert_eq!(page.rows.len(), 3);
        assert_eq!(
            page.rows[0].target,
            Some(Resource::new("postgres.schemas", vec![]))
        );
        assert_eq!(
            page.rows[1].target,
            Some(Resource::new("postgres.replication", vec![]))
        );
        assert_eq!(
            page.rows[2].target,
            Some(Resource::new("postgres.wal_receiver", vec![]))
        );
        assert!(root(&resource, Some("unexpected")).is_err());
        assert!(
            root(
                &Resource::new("postgres.resources", vec!["bad".into()]),
                None
            )
            .is_err()
        );
    }
}
