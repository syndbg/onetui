use std::ffi::CString;
use std::ptr;
use std::time::Duration;

use anyhow::{Result, ensure};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use rdkafka::bindings as native;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};

use crate::groups::{slice, text};

// The safe AdminClient creates another client/thread. These read-only calls use
// the existing owner instead. Each handle owns one native allocation; none escape.
struct Owned<T>(*mut T, unsafe extern "C" fn(*mut T));

impl<T> Owned<T> {
    fn new(ptr: *mut T, destroy: unsafe extern "C" fn(*mut T)) -> Result<Self> {
        ensure!(!ptr.is_null(), "Kafka returned a null admin handle");
        Ok(Self(ptr, destroy))
    }
}

impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        // Constructor requires a non-null pointer from the matching allocator.
        unsafe { (self.1)(self.0) }
    }
}

pub(crate) fn page<C: ConsumerContext>(
    client: &BaseConsumer<C>,
    resource: &Resource,
    offset: i64,
    identity: u64,
    remaining: impl Fn() -> Result<Duration>,
    watermarks: impl Fn(&str, i32) -> Result<(i64, i64)>,
) -> Result<Page> {
    let timeout = remaining()?.as_millis().clamp(1, i32::MAX as u128) as i32;
    let name = CString::new(resource.path[0].as_str())?;
    // librdkafka copies request arguments when enqueued. The client outlives the
    // queue and event; result pointers are borrowed only while the event is owned.
    unsafe {
        let rk = client.client().native_ptr();
        let queue = Owned::new(
            native::rd_kafka_queue_new(rk),
            native::rd_kafka_queue_destroy,
        )?;
        let options = Owned::new(
            native::rd_kafka_AdminOptions_new(
                rk,
                native::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ANY,
            ),
            native::rd_kafka_AdminOptions_destroy,
        )?;
        let mut error = [0; 512];
        check(
            native::rd_kafka_AdminOptions_set_request_timeout(
                options.0,
                timeout,
                error.as_mut_ptr(),
                error.len(),
            ),
            error.as_ptr(),
        )?;
        if resource.id == "kafka.offsets" {
            // NULL asks for all stored offsets, not an application subscription.
            let mut request = Owned::new(
                native::rd_kafka_ListConsumerGroupOffsets_new(name.as_ptr(), ptr::null()),
                native::rd_kafka_ListConsumerGroupOffsets_destroy,
            )?;
            native::rd_kafka_ListConsumerGroupOffsets(rk, &mut request.0, 1, options.0, queue.0);
        } else {
            let kind = if resource.id == "kafka.topic_config" {
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC
            } else {
                native::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER
            };
            let mut request = Owned::new(
                native::rd_kafka_ConfigResource_new(kind, name.as_ptr()),
                native::rd_kafka_ConfigResource_destroy,
            )?;
            native::rd_kafka_DescribeConfigs(rk, &mut request.0, 1, options.0, queue.0);
        }
        let event = loop {
            let wait = remaining()?.as_millis().clamp(1, 100) as i32;
            let event = native::rd_kafka_queue_poll(queue.0, wait);
            if !event.is_null() {
                break Owned::new(event, native::rd_kafka_event_destroy)?;
            }
            // Service transport/auth callbacks without consuming any application group.
            if let Some(Err(error)) = client.poll(Duration::ZERO)
                && !matches!(error, rdkafka::error::KafkaError::PartitionEOF(_))
            {
                return Err(error.into());
            }
        };
        remaining()?;
        check(
            native::rd_kafka_event_error(event.0),
            native::rd_kafka_event_error_string(event.0),
        )?;
        if resource.id == "kafka.offsets" {
            offsets(event.0, resource, offset, identity, watermarks)
        } else {
            configs(event.0, resource, offset, identity)
        }
    }
}

unsafe fn check(code: native::rd_kafka_resp_err_t, message: *const std::ffi::c_char) -> Result<()> {
    if code != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        // Keep both the native code and broker detail; NULL falls back to its native description.
        let message = if message.is_null() {
            unsafe { native::rd_kafka_err2str(code) }
        } else {
            message
        };
        anyhow::bail!("{code:?}: {}", unsafe { text(message)? });
    }
    Ok(())
}

unsafe fn configs(
    event: *mut native::rd_kafka_event_t,
    resource: &Resource,
    offset: i64,
    identity: u64,
) -> Result<Page> {
    // All pointers belong to the caller's live event. Only owned Page values escape.
    unsafe {
        let result = native::rd_kafka_event_DescribeConfigs_result(event);
        ensure!(
            !result.is_null(),
            "Kafka returned an unexpected admin event"
        );
        let mut count = 0;
        let resources = native::rd_kafka_DescribeConfigs_result_resources(result, &mut count);
        let resources = slice(resources, i32::try_from(count)?)?;
        ensure!(
            resources.len() == 1 && !resources[0].is_null(),
            "Kafka returned no configuration resource"
        );
        let config = resources[0];
        check(
            native::rd_kafka_ConfigResource_error(config),
            native::rd_kafka_ConfigResource_error_string(config),
        )?;
        ensure!(
            text(native::rd_kafka_ConfigResource_name(config))? == resource.path[0],
            "Kafka returned another configuration resource"
        );
        let entries = native::rd_kafka_ConfigResource_configs(config, &mut count);
        let mut entries = slice(entries, i32::try_from(count)?)?
            .iter()
            .map(|&entry| {
                ensure!(
                    !entry.is_null(),
                    "Kafka returned a null configuration entry"
                );
                Ok((text(native::rd_kafka_ConfigEntry_name(entry))?, entry))
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|(name, _)| *name);
        let total = entries.len();
        let mut page = crate::browse::page(
            resource,
            "Configuration; sensitive values withheld. Re-read per page, no snapshot.",
        );
        for (name, entry) in entries
            .into_iter()
            .skip(usize::try_from(offset)?)
            .take(PAGE_SIZE as usize)
        {
            let sensitive = native::rd_kafka_ConfigEntry_is_sensitive(entry) != 0;
            let value = native::rd_kafka_ConfigEntry_value(entry);
            page.rows.push(Row {
                cells: vec![
                    Some(name.into()),
                    config_value(sensitive, value)?,
                    Some(
                        text(native::rd_kafka_ConfigSource_name(
                            native::rd_kafka_ConfigEntry_source(entry),
                        ))?
                        .into(),
                    ),
                    Some(
                        (native::rd_kafka_ConfigEntry_is_default(entry) != 0)
                            .to_string()
                            .into(),
                    ),
                    Some(
                        (native::rd_kafka_ConfigEntry_is_read_only(entry) != 0)
                            .to_string()
                            .into(),
                    ),
                    Some(sensitive.to_string().into()),
                    Some(config_synonyms(entry, sensitive)?),
                ],
                target: None,
            });
            ensure!(
                page.bytes() <= PAGE_BYTES,
                "Kafka configuration page exceeds 1 MiB"
            );
        }
        finish(page, resource, offset, total, identity)
    }
}

unsafe fn config_value(sensitive: bool, value: *const std::ffi::c_char) -> Result<Option<Value>> {
    if sensitive || value.is_null() {
        Ok(None)
    } else {
        Ok(Some(unsafe { text(value)? }.into()))
    }
}

unsafe fn config_synonyms(
    entry: *const native::rd_kafka_ConfigEntry_t,
    sensitive: bool,
) -> Result<Value> {
    // Synonyms borrow the same event as their parent. Preserve broker precedence,
    // and never expose a sensitive parent's value through a synonym.
    unsafe {
        let mut count = 0;
        let entries = native::rd_kafka_ConfigEntry_synonyms(entry, &mut count);
        let mut json = String::from("[");
        for &synonym in slice(entries, i32::try_from(count)?)? {
            ensure!(
                !synonym.is_null(),
                "Kafka returned a null configuration synonym"
            );
            let name = text(native::rd_kafka_ConfigEntry_name(synonym))?;
            let source = text(native::rd_kafka_ConfigSource_name(
                native::rd_kafka_ConfigEntry_source(synonym),
            ))?;
            let hidden = sensitive || native::rd_kafka_ConfigEntry_is_sensitive(synonym) != 0;
            let value = native::rd_kafka_ConfigEntry_value(synonym);
            let value = if hidden || value.is_null() {
                None
            } else {
                Some(text(value)?)
            };
            ensure!(
                name.len() + source.len() + value.map_or(0, str::len) <= PAGE_BYTES,
                "Kafka configuration synonym exceeds 1 MiB"
            );
            let item = serde_json::to_string(&serde_json::json!({
                "name": name, "value": value, "source": source
            }))?;
            ensure!(
                json.len() + item.len() + 2 <= PAGE_BYTES,
                "Kafka configuration synonyms exceed 1 MiB"
            );
            if json.len() > 1 {
                json.push(',');
            }
            json.push_str(&item);
        }
        json.push(']');
        Ok(Value::Json(json))
    }
}

unsafe fn offsets(
    event: *mut native::rd_kafka_event_t,
    resource: &Resource,
    offset: i64,
    identity: u64,
    watermarks: impl Fn(&str, i32) -> Result<(i64, i64)>,
) -> Result<Page> {
    // Group and partition results borrow the live event, including error strings.
    unsafe {
        let result = native::rd_kafka_event_ListConsumerGroupOffsets_result(event);
        ensure!(
            !result.is_null(),
            "Kafka returned an unexpected admin event"
        );
        let mut count = 0;
        let groups = native::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut count);
        let groups = slice(groups, i32::try_from(count)?)?;
        ensure!(
            groups.len() == 1 && !groups[0].is_null(),
            "Kafka returned no offset group"
        );
        let group = groups[0];
        let error = native::rd_kafka_group_result_error(group);
        if !error.is_null() {
            check(
                native::rd_kafka_error_code(error),
                native::rd_kafka_error_string(error),
            )?;
        }
        ensure!(
            text(native::rd_kafka_group_result_name(group))? == resource.path[0],
            "Kafka returned another offset group"
        );
        let partitions = native::rd_kafka_group_result_partitions(group);
        ensure!(
            !partitions.is_null(),
            "Kafka returned null offset partitions"
        );
        let mut partitions = slice((*partitions).elems, (*partitions).cnt)?
            .iter()
            .map(|p| {
                let topic = text(p.topic)?;
                ensure!(
                    crate::browse::valid_topic(topic) && p.partition >= 0,
                    "Kafka returned an invalid offset partition"
                );
                Ok((topic, p))
            })
            .collect::<Result<Vec<_>>>()?;
        partitions.sort_by_key(|(topic, p)| (*topic, p.partition));
        let total = partitions.len();
        let mut page = crate::browse::page(
            resource,
            "Stored offsets only; lag = stable_end - committed, not message count. Independent reads, no snapshot or commits.",
        );
        for (topic, p) in partitions
            .into_iter()
            .skip(usize::try_from(offset)?)
            .take(PAGE_SIZE as usize)
        {
            check(p.err, ptr::null())?;
            let (low, end) = watermarks(topic, p.partition)?;
            let (lag, status) = lag(p.offset, low, end)?;
            page.rows.push(Row {
                cells: vec![
                    Some(topic.into()),
                    Some(p.partition.to_string().into()),
                    (p.offset >= 0).then(|| p.offset.to_string().into()),
                    Some(low.to_string().into()),
                    Some(end.to_string().into()),
                    lag.map(|n| n.to_string().into()),
                    Some(status.into()),
                ],
                target: None,
            });
            ensure!(
                page.bytes() <= PAGE_BYTES,
                "Kafka offset page exceeds 1 MiB"
            );
        }
        finish(page, resource, offset, total, identity)
    }
}

fn lag(committed: i64, low: i64, end: i64) -> Result<(Option<i64>, &'static str)> {
    ensure!(
        low >= 0 && end >= low,
        "Kafka returned an invalid watermark window"
    );
    Ok(if committed == native::RD_KAFKA_OFFSET_INVALID as i64 {
        (None, "no commit")
    } else if committed < 0 {
        anyhow::bail!("Kafka returned invalid committed offset {committed}")
    } else if committed < low {
        (None, "before retained start")
    } else if committed > end {
        (None, "beyond stable end")
    } else {
        (Some(end - committed), "within window")
    })
}

fn finish(
    page: Page,
    resource: &Resource,
    offset: i64,
    total: usize,
    identity: u64,
) -> Result<Page> {
    let offset = usize::try_from(offset)?;
    ensure!(
        offset <= total,
        "Kafka metadata changed; refresh its parent"
    );
    let next = offset + page.rows.len();
    crate::browse::finish(
        page,
        resource,
        identity,
        (next < total).then_some((next as i64, None)),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_does_not_fabricate_counts_for_unavailable_offsets() {
        assert_eq!(lag(7, 0, 20).unwrap(), (Some(13), "within window"));
        assert_eq!(lag(20, 0, 20).unwrap().0, Some(0));
        assert_eq!(lag(7, 8, 20).unwrap(), (None, "before retained start"));
        assert_eq!(lag(21, 0, 20).unwrap(), (None, "beyond stable end"));
        assert_eq!(lag(-1001, 0, 20).unwrap(), (None, "no commit"));
        assert!(lag(-1, 0, 20).is_err());
        assert!(lag(0, 2, 1).is_err());
        assert_eq!(lag(0, 0, i64::MAX).unwrap().0, Some(i64::MAX));
    }

    #[test]
    fn sensitive_config_and_native_errors_survive_conversion() {
        // Buffers below remain alive for conversion; sensitive bytes must never be read.
        unsafe {
            assert_eq!(config_value(true, c"secret".as_ptr()).unwrap(), None);
            assert_eq!(config_value(false, ptr::null()).unwrap(), None);
            assert_eq!(config_value(false, c"".as_ptr()).unwrap(), Some("".into()));
            let error = check(
                native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED,
                c"broker detail".as_ptr(),
            )
            .unwrap_err();
            assert!(error.to_string().contains("TOPIC_AUTHORIZATION_FAILED"));
            assert!(error.to_string().contains("broker detail"));
        }
        let resource = Resource::new("kafka.topic_config", vec!["demo".into()]);
        let page = crate::browse::page(&resource, "");
        assert!(finish(page.clone(), &resource, 2, 1, 7).is_err());
        let first = finish(page, &resource, 0, 101, 7).unwrap();
        assert!(first.next);
    }
}
