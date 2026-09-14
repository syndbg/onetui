use crate::{attributes, query::Read, response::Capture};
use anyhow::{Result, anyhow, bail};
use aws_sdk_dynamodb::{
    Client,
    error::DisplayErrorContext,
    types::{Get, KeysAndAttributes, ReturnConsumedCapacity, TransactGetItem},
};
use serde_json::Value;

macro_rules! send {
    ($request:expr) => {{
        let capture = Capture::default();
        let result = $request
            .customize()
            .config_override(
                aws_sdk_dynamodb::config::Builder::new().retry_classifier(capture.clone()),
            )
            .interceptor(capture.clone())
            .send()
            .await
            .map(|_| ())
            .map_err(|error| anyhow!("{}", DisplayErrorContext(error)));
        capture.finish(result)
    }};
}

pub(crate) async fn metadata(
    client: &Client,
    id: &str,
    path: &[String],
    cursor: Option<&Value>,
) -> Result<Value> {
    let name = path.first().map(String::as_str).unwrap_or("");
    let token = cursor
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("Invalid DynamoDB metadata bookmark"))
        })
        .transpose()?;
    match id {
        "dynamodb.tables" => send!(
            client
                .list_tables()
                .limit(100)
                .set_exclusive_start_table_name(token)
        ),
        "dynamodb.table_info" | "dynamodb.indexes" | "dynamodb.replicas" => {
            send!(client.describe_table().table_name(name))
        }
        "dynamodb.ttl" => send!(client.describe_time_to_live().table_name(name)),
        "dynamodb.backups_status" => send!(client.describe_continuous_backups().table_name(name)),
        "dynamodb.insights" => send!(client.describe_contributor_insights().table_name(name)),
        "dynamodb.index_insights" => send!(
            client
                .describe_contributor_insights()
                .table_name(name)
                .index_name(&path[1])
        ),
        "dynamodb.stream_policy" => send!(client.get_resource_policy().resource_arn(name)),
        "dynamodb.kinesis" => send!(
            client
                .describe_kinesis_streaming_destination()
                .table_name(name)
        ),
        "dynamodb.replica_scaling" => send!(
            client
                .describe_table_replica_auto_scaling()
                .table_name(name)
        ),
        "dynamodb.tags" => {
            let table = send!(client.describe_table().table_name(name))?;
            let arn = table["Table"]["TableArn"]
                .as_str()
                .ok_or_else(|| anyhow!("DynamoDB returned no table ARN"))?;
            send!(
                client
                    .list_tags_of_resource()
                    .resource_arn(arn)
                    .set_next_token(token)
            )
        }
        "dynamodb.policy" => {
            let table = send!(client.describe_table().table_name(name))?;
            let arn = table["Table"]["TableArn"]
                .as_str()
                .ok_or_else(|| anyhow!("DynamoDB returned no table ARN"))?;
            send!(client.get_resource_policy().resource_arn(arn))
        }
        "dynamodb.backups" => send!(
            client
                .list_backups()
                .limit(100)
                .set_exclusive_start_backup_arn(token)
        ),
        "dynamodb.backup" => send!(client.describe_backup().backup_arn(name)),
        "dynamodb.exports" => send!(client.list_exports().max_results(100).set_next_token(token)),
        "dynamodb.export" => send!(client.describe_export().export_arn(name)),
        "dynamodb.imports" => send!(client.list_imports().page_size(100).set_next_token(token)),
        "dynamodb.import" => send!(client.describe_import().import_arn(name)),
        "dynamodb.global_tables" => send!(
            client
                .list_global_tables()
                .limit(100)
                .set_exclusive_start_global_table_name(token)
        ),
        "dynamodb.global_table" => send!(client.describe_global_table().global_table_name(name)),
        "dynamodb.global_settings" => send!(
            client
                .describe_global_table_settings()
                .global_table_name(name)
        ),
        "dynamodb.account_insights" => send!(
            client
                .list_contributor_insights()
                .max_results(100)
                .set_next_token(token)
        ),
        "dynamodb.limits" => send!(client.describe_limits()),
        "dynamodb.endpoints" => send!(client.describe_endpoints()),
        _ => bail!("Unknown DynamoDB metadata resource"),
    }
}

pub(crate) async fn query(
    client: &Client,
    table: &str,
    query: Read,
    cursor: Option<&Value>,
) -> Result<Value> {
    match query {
        Read::ExecuteStatement {
            statement,
            parameters,
            consistent_read,
            limit,
        } => {
            let token = cursor
                .map(|v| {
                    let token = v
                        .as_str()
                        .ok_or_else(|| anyhow!("Invalid PartiQL bookmark"))?;
                    anyhow::ensure!(
                        !token.is_empty() && token.len() <= 32768,
                        "Invalid PartiQL bookmark length"
                    );
                    Ok::<_, anyhow::Error>(token.to_owned())
                })
                .transpose()?;
            send!(
                client
                    .execute_statement()
                    .statement(statement)
                    .set_parameters(crate::partiql::parameters(parameters.as_deref())?)
                    .consistent_read(consistent_read)
                    .limit(limit)
                    .set_next_token(token)
                    .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            )
        }
        Read::BatchExecuteStatement { statements } => {
            anyhow::ensure!(
                cursor.is_none(),
                "BatchExecuteStatement has no continuation"
            );
            let statements = statements
                .into_iter()
                .map(|s| {
                    Ok(aws_sdk_dynamodb::types::BatchStatementRequest::builder()
                        .statement(s.statement)
                        .set_parameters(crate::partiql::parameters(s.parameters.as_deref())?)
                        .set_consistent_read(s.consistent_read)
                        .build()?)
                })
                .collect::<Result<Vec<_>>>()?;
            send!(
                client
                    .batch_execute_statement()
                    .set_statements(Some(statements))
                    .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            )
        }
        Read::ExecuteTransaction { statements } => {
            anyhow::ensure!(cursor.is_none(), "ExecuteTransaction has no continuation");
            let statements = statements
                .into_iter()
                .map(|s| {
                    Ok(aws_sdk_dynamodb::types::ParameterizedStatement::builder()
                        .statement(s.statement)
                        .set_parameters(crate::partiql::parameters(s.parameters.as_deref())?)
                        .build()?)
                })
                .collect::<Result<Vec<_>>>()?;
            send!(
                client
                    .execute_transaction()
                    .set_transact_statements(Some(statements))
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
            )
        }
        Read::SearchVectors {
            index,
            search_vector,
            top_k,
            search_condition_expression,
            projection_expression,
            expression_attribute_names,
            expression_attribute_values,
        } => {
            anyhow::ensure!(cursor.is_none(), "SearchVectors has no continuation");
            send!(
                client
                    .search_vectors()
                    .table_name(table)
                    .index_name(index)
                    .set_search_vector(Some(
                        search_vector
                            .into_iter()
                            .map(|n| aws_sdk_dynamodb::types::AttributeValue::N(n.to_string()))
                            .collect()
                    ))
                    .top_k(top_k)
                    .set_search_condition_expression(search_condition_expression)
                    .set_projection_expression(projection_expression)
                    .set_expression_attribute_names(expression_attribute_names)
                    .set_expression_attribute_values(
                        expression_attribute_values
                            .as_ref()
                            .map(attributes::item)
                            .transpose()?
                    )
                    .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            )
        }
        Read::BatchGetItem {
            keys,
            projection_expression,
            expression_attribute_names,
            consistent_read,
        } => {
            let keys = match cursor {
                Some(value) => serde_json::from_value(value.clone())?,
                None => keys,
            };
            crate::query::validate_keys(&keys)?;
            send!(
                client
                    .batch_get_item()
                    .request_items(
                        table,
                        KeysAndAttributes::builder()
                            .set_keys(Some(
                                keys.iter().map(attributes::item).collect::<Result<_>>()?
                            ))
                            .set_projection_expression(projection_expression)
                            .set_expression_attribute_names(expression_attribute_names)
                            .consistent_read(consistent_read)
                            .build()?
                    )
                    .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            )
        }
        Read::TransactGetItems { items } => {
            anyhow::ensure!(cursor.is_none(), "TransactGetItems has no continuation");
            let items = items
                .into_iter()
                .map(|item| {
                    Ok(TransactGetItem::builder()
                        .get(
                            Get::builder()
                                .table_name(table)
                                .set_key(Some(attributes::item(&item.key)?))
                                .set_projection_expression(item.projection_expression)
                                .set_expression_attribute_names(item.expression_attribute_names)
                                .build()?,
                        )
                        .build())
                })
                .collect::<Result<Vec<_>>>()?;
            send!(
                client
                    .transact_get_items()
                    .set_transact_items(Some(items))
                    .return_consumed_capacity(ReturnConsumedCapacity::Total)
            )
        }
        Read::GetRecords { .. } => bail!("GetRecords requires a Streams client"),
        Read::Scan {
            index,
            filter_expression,
            projection_expression,
            expression_attribute_names,
            expression_attribute_values,
            consistent_read,
            limit,
        } => send!(
            client
                .scan()
                .table_name(table)
                .set_index_name(index)
                .set_filter_expression(filter_expression)
                .set_projection_expression(projection_expression)
                .set_expression_attribute_names(expression_attribute_names)
                .set_expression_attribute_values(
                    expression_attribute_values
                        .as_ref()
                        .map(attributes::item)
                        .transpose()?
                )
                .consistent_read(consistent_read)
                .limit(limit)
                .set_exclusive_start_key(cursor.map(attributes::item).transpose()?)
                .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
        ),
        Read::Query {
            index,
            key_condition_expression,
            filter_expression,
            projection_expression,
            expression_attribute_names,
            expression_attribute_values,
            consistent_read,
            scan_index_forward,
            limit,
        } => send!(
            client
                .query()
                .table_name(table)
                .set_index_name(index)
                .key_condition_expression(key_condition_expression)
                .set_filter_expression(filter_expression)
                .set_projection_expression(projection_expression)
                .set_expression_attribute_names(expression_attribute_names)
                .set_expression_attribute_values(
                    expression_attribute_values
                        .as_ref()
                        .map(attributes::item)
                        .transpose()?
                )
                .consistent_read(consistent_read)
                .scan_index_forward(scan_index_forward)
                .limit(limit)
                .set_exclusive_start_key(cursor.map(attributes::item).transpose()?)
                .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
        ),
        Read::GetItem {
            key: requested,
            projection_expression,
            expression_attribute_names,
            consistent_read,
        } => {
            anyhow::ensure!(cursor.is_none(), "GetItem has no continuation");
            send!(
                client
                    .get_item()
                    .table_name(table)
                    .set_key(Some(attributes::item(&requested)?))
                    .set_projection_expression(projection_expression)
                    .set_expression_attribute_names(expression_attribute_names)
                    .consistent_read(consistent_read)
                    .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            )
        }
    }
}
