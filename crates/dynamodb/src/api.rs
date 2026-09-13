use crate::{attributes, query::Read, response::Capture};
use anyhow::{Result, anyhow, bail};
use aws_sdk_dynamodb::{Client, error::DisplayErrorContext, types::ReturnConsumedCapacity};
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
    name: &str,
    cursor: Option<&Value>,
) -> Result<Value> {
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
    let key = cursor.map(attributes::item).transpose()?;
    match query {
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
                .set_exclusive_start_key(key)
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
                .set_exclusive_start_key(key)
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
