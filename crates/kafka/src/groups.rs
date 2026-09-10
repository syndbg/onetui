use std::ffi::{CStr, CString, c_char, c_void};
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use rdkafka::bindings as native;
use rdkafka::client::{Client, ClientContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

// The SDK's GroupInfo omits the broker's per-group error. Own the native list so
// those errors survive, and free partial responses as well as successful ones.
pub(crate) struct Groups(*const native::rd_kafka_group_list);

impl Drop for Groups {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // librdkafka transfers list ownership to this wrapper, exactly once.
            unsafe { native::rd_kafka_group_list_destroy(self.0) };
        }
    }
}

impl Groups {
    pub(crate) fn fetch<C: ClientContext>(
        client: &Client<C>,
        group: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, KafkaError> {
        let group = group.map(CString::new).transpose()?;
        let mut list = Self(std::ptr::null());
        // Client and optional group string outlive the blocking native call.
        let error = unsafe {
            native::rd_kafka_list_groups(
                client.native_ptr(),
                group.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
                &mut list.0,
                timeout.as_millis().clamp(1, i32::MAX as u128) as i32,
            )
        };
        if error != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(KafkaError::GroupListFetch(error.into()));
        }
        if list.0.is_null() {
            return Err(KafkaError::GroupListFetch(RDKafkaErrorCode::BadMessage));
        }
        Ok(list)
    }

    pub(crate) fn page(&self, resource: &Resource, offset: i64, identity: u64) -> Result<Page> {
        // Every pointer below belongs to this native list. It stays alive for the
        // entire conversion; only owned values escape. Helpers check null/lengths
        // and UTF-8 before inspecting data. Native fields are never cast to SDK types.
        unsafe {
            let list = &*self.0;
            let groups = slice(list.groups, list.group_cnt)?;
            let mut named = groups
                .iter()
                .map(|group| Ok((text(group.group)?, group)))
                .collect::<Result<Vec<_>>>()?;
            named.sort_by_key(|(name, _)| *name);
            let mut page = crate::browse::page(
                resource,
                "Read-only group metadata; no membership changes or offset commits. Assignments remain protocol bytes.",
            );
            let offset = usize::try_from(offset)?;
            let total;
            if resource.id == "kafka.groups" {
                total = named.len();
                for (name, group) in named.into_iter().skip(offset).take(PAGE_SIZE as usize) {
                    check_error(group.err)?;
                    ensure!(
                        crate::browse::valid_group(name),
                        "Kafka group name exceeds supported bounds"
                    );
                    let members = slice(group.members, group.member_cnt)?;
                    page.rows.push(Row {
                        cells: vec![
                            Some(name.into()),
                            Some(text(group.state)?.into()),
                            Some(text(group.protocol_type)?.into()),
                            Some(text(group.protocol)?.into()),
                            Some(members.len().to_string().into()),
                        ],
                        target: Some(Resource::new("kafka.members", vec![name.into()])),
                    });
                }
            } else {
                let (_, group) = named
                    .into_iter()
                    .find(|(name, _)| *name == resource.path[0])
                    .ok_or_else(|| {
                        anyhow!("Kafka group missing from metadata; refresh its parent")
                    })?;
                check_error(group.err)?;
                let members = slice(group.members, group.member_cnt)?;
                let mut members = members
                    .iter()
                    .map(|member| Ok((text(member.member_id)?, member)))
                    .collect::<Result<Vec<_>>>()?;
                members.sort_by_key(|(id, _)| *id);
                total = members.len();
                for (id, member) in members.into_iter().skip(offset).take(PAGE_SIZE as usize) {
                    page.rows.push(Row {
                        cells: vec![
                            Some(id.into()),
                            Some(text(member.client_id)?.into()),
                            Some(text(member.client_host)?.into()),
                            bytes(member.member_metadata, member.member_metadata_size)?,
                            bytes(member.member_assignment, member.member_assignment_size)?,
                        ],
                        target: None,
                    });
                    ensure!(page.bytes() <= PAGE_BYTES, "Kafka group page exceeds 1 MiB");
                }
            }
            ensure!(
                offset <= total,
                "Kafka group metadata changed; refresh its parent"
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
    }
}

fn check_error(error: native::rd_kafka_resp_err_t) -> Result<()> {
    if error != native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaError::GroupListFetch(error.into()).into());
    }
    Ok(())
}

// Callers provide pointers from a still-owned librdkafka response (or a test buffer).
unsafe fn text<'a>(ptr: *const c_char) -> Result<&'a str> {
    ensure!(!ptr.is_null(), "Kafka returned a null metadata string");
    Ok(unsafe { CStr::from_ptr(ptr) }.to_str()?)
}

unsafe fn slice<'a, T>(ptr: *const T, count: i32) -> Result<&'a [T]> {
    let count = usize::try_from(count)?;
    ensure!(
        count
            .checked_mul(std::mem::size_of::<T>())
            .is_some_and(|n| n <= 4 * PAGE_BYTES),
        "Kafka group metadata exceeds 4 MiB"
    );
    if count == 0 {
        return Ok(&[]);
    }
    ensure!(!ptr.is_null(), "Kafka returned null group metadata");
    Ok(unsafe { std::slice::from_raw_parts(ptr, count) })
}

unsafe fn bytes(ptr: *const c_void, count: i32) -> Result<Option<Value>> {
    let count = usize::try_from(count)?;
    ensure!(count <= PAGE_BYTES, "Kafka member metadata exceeds 1 MiB");
    if ptr.is_null() {
        ensure!(
            count == 0,
            "Kafka returned null member metadata with a nonzero length"
        );
        return Ok(None);
    }
    Ok(Some(Value::Bytes(
        unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), count) }.to_vec(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_errors_and_raw_metadata_survive_without_lossy_utf8() {
        let error =
            check_error(native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED)
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            KafkaError::GroupListFetch(RDKafkaErrorCode::GroupAuthorizationFailed).to_string()
        );
        let mut group = native::rd_kafka_group_info {
            broker: native::rd_kafka_metadata_broker {
                id: 1,
                host: std::ptr::null_mut(),
                port: 9092,
            },
            group: c"denied".as_ptr().cast_mut(),
            err: native::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
            state: std::ptr::null_mut(),
            protocol_type: std::ptr::null_mut(),
            protocol: std::ptr::null_mut(),
            members: std::ptr::null_mut(),
            member_cnt: 0,
        };
        let list = native::rd_kafka_group_list {
            groups: &mut group,
            group_cnt: 1,
        };
        // Stack-owned test response must not go through librdkafka's deallocator.
        let groups = std::mem::ManuallyDrop::new(Groups(&list));
        for resource in [
            Resource::new("kafka.groups", vec![]),
            Resource::new("kafka.members", vec!["denied".into()]),
        ] {
            assert_eq!(
                groups.page(&resource, 0, 1).unwrap_err().to_string(),
                error.to_string()
            );
        }
        let raw = [0xff_u8, 0];
        // Test buffers remain allocated until every borrowed conversion finishes.
        unsafe {
            assert!(text(raw.as_ptr().cast()).is_err());
            assert!(text(std::ptr::null()).is_err());
            assert_eq!(
                bytes(raw.as_ptr().cast(), 2).unwrap(),
                Some(Value::Bytes(raw.to_vec()))
            );
            assert_eq!(
                bytes(raw.as_ptr().cast(), 0).unwrap(),
                Some(Value::Bytes(vec![]))
            );
            assert_eq!(bytes(std::ptr::null(), 0).unwrap(), None);
            assert!(bytes(std::ptr::null(), 1).is_err());
            assert!(bytes(raw.as_ptr().cast(), -1).is_err());
            assert!(bytes(raw.as_ptr().cast(), PAGE_BYTES as i32 + 1).is_err());
            assert!(slice::<u8>(std::ptr::null(), 1).is_err());
            assert!(slice::<u8>(std::ptr::null(), -1).is_err());
        }
    }
}
