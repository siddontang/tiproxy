// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Keyspace selection for multi-keyspace (shared) backends.
//!
//! A client selects a tenant with a `<keyspace>.<user>` name, the same prefix
//! a multi-keyspace `TiDB` node routes by. Namespace resolution is unchanged;
//! the keyspace only narrows the backend candidates of the resolved namespace.

use std::sync::atomic::{AtomicBool, Ordering};

use control_topology::MergedBackend;

use crate::RouteError;

/// Per-namespace, process-lifetime memory that the namespace has had a shared
/// (multi-keyspace) backend.
///
/// The memory is sticky: once set it is never cleared, and the route plane
/// hands the same instance to every router incarnation of a namespace name.
/// Without it, the lease expiry of every shared backend in a mixed namespace
/// would switch the keyspace filter off and let a `<keyspace>.<user>` client
/// land on another tenant's single-keyspace backend.
#[derive(Debug, Default)]
pub(crate) struct KeyspaceMemory {
    seen_shared: AtomicBool,
}

impl KeyspaceMemory {
    /// Records any shared backend in `backends`; returns whether a shared
    /// backend is present now or has been observed before.
    pub(crate) fn observe(&self, backends: &[MergedBackend]) -> bool {
        if backends
            .iter()
            .any(|backend| backend.backend.serves.is_shared())
        {
            // A monotonic flag guards no other data, so relaxed is enough.
            self.seen_shared.store(true, Ordering::Relaxed);
            return true;
        }
        self.seen_shared.load(Ordering::Relaxed)
    }
}

/// The keyspace a client selected with its user name: the nonempty prefix
/// before the first `.`, or `None` when the name has no such prefix.
#[must_use]
pub fn user_keyspace(user: &str) -> Option<&str> {
    user.split_once('.')
        .map(|(prefix, _)| prefix)
        .filter(|prefix| !prefix.is_empty())
}

/// Resolves the keyspace filter for one admission against a namespace's
/// discovered backends (health-independent).
///
/// The filter is active only when the client named a keyspace **and** the
/// namespace has a multi-keyspace backend now **or has had one since this
/// process started** (sticky [`KeyspaceMemory`]; it observes every routing
/// snapshot the router's worker, admission check and reserve see). A
/// namespace that never had a shared backend — classic or single-keyspace,
/// whose user-name prefix need not be the keyspace name — keeps today's
/// routing exactly, including for user names that merely contain a dot. A
/// shared backend that appears and expires between two observed snapshots
/// is not remembered. When active, at least one
/// discovered backend must serve the keyspace (`Single` equal, `Set`
/// containing it, or `Any`); otherwise the connection is refused with
/// [`RouteError::KeyspaceNotServed`] rather than falling back.
///
/// # Errors
///
/// [`RouteError::KeyspaceNotServed`] when the filter is active and no
/// discovered backend serves the keyspace.
pub(crate) fn admission_filter<'a>(
    backends: &[MergedBackend],
    keyspace: Option<&'a str>,
    memory: &KeyspaceMemory,
) -> Result<Option<&'a str>, RouteError> {
    let had_shared = memory.observe(backends);
    let Some(keyspace) = keyspace else {
        return Ok(None);
    };
    if !had_shared {
        return Ok(None);
    }
    if backends
        .iter()
        .any(|backend| backend.backend.serves.serves(keyspace))
    {
        Ok(Some(keyspace))
    } else {
        Err(RouteError::KeyspaceNotServed)
    }
}

/// Whether `backend` is a candidate under an (already resolved) filter.
pub(crate) fn admits(backend: &MergedBackend, filter: Option<&str>) -> bool {
    filter.is_none_or(|keyspace| backend.backend.serves.serves(keyspace))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use control_topology::{BackendInfo, KeyspaceScope, MergedBackend};

    use super::{KeyspaceMemory, RouteError, admission_filter, admits, user_keyspace};

    fn backend(addr: &str, serves: KeyspaceScope) -> MergedBackend {
        MergedBackend {
            backend_id: Arc::from(format!("c/{addr}")),
            cluster_name: Arc::from("c"),
            backend: BackendInfo {
                addr: addr.to_owned(),
                serves,
                ip: String::new(),
                status_port: 0,
                version: String::new(),
                git_hash: String::new(),
                deploy_path: String::new(),
                start_timestamp: 0,
                labels: std::collections::BTreeMap::new(),
            },
        }
    }

    fn set(keyspaces: &[&str]) -> KeyspaceScope {
        KeyspaceScope::Set(
            keyspaces
                .iter()
                .map(|ks| (*ks).to_owned())
                .collect::<BTreeSet<_>>(),
        )
    }

    #[test]
    fn user_prefix_is_split_at_the_first_dot() {
        assert_eq!(user_keyspace("keyspace1.app"), Some("keyspace1"));
        assert_eq!(user_keyspace("ks.app.with.dots"), Some("ks"));
        assert_eq!(user_keyspace("ks."), Some("ks"));
        assert_eq!(user_keyspace("app"), None);
        assert_eq!(user_keyspace(""), None);
        assert_eq!(user_keyspace(".app"), None);
    }

    #[test]
    fn shared_backend_memory_is_sticky() {
        let memory = KeyspaceMemory::default();
        let single = [
            backend("a:1", KeyspaceScope::default()),
            backend("b:1", KeyspaceScope::Single("ks2".into())),
        ];
        // Never shared: today's routing, even for an unserved prefix.
        assert_eq!(admission_filter(&single, Some("ks1"), &memory), Ok(None));
        // A shared backend is observed (by a no-prefix admission too).
        let mixed = [
            backend("a:1", KeyspaceScope::default()),
            backend("b:1", KeyspaceScope::Single("ks2".into())),
            backend("c:1", set(&["ks1"])),
        ];
        assert_eq!(admission_filter(&mixed, None, &memory), Ok(None));
        // It expires; the filter stays active on the remaining backends.
        assert_eq!(
            admission_filter(&single, Some("ks1"), &memory),
            Err(RouteError::KeyspaceNotServed)
        );
        assert_eq!(
            admission_filter(&single, Some("ks2"), &memory),
            Ok(Some("ks2"))
        );
        assert!(!admits(&single[0], Some("ks2")));
        assert_eq!(admission_filter(&single, None, &memory), Ok(None));
    }

    #[test]
    fn no_prefix_or_no_shared_backend_keeps_todays_routing() {
        let classic = [
            backend("a:1", KeyspaceScope::default()),
            backend("b:1", KeyspaceScope::Single("ks2".into())),
        ];
        assert_eq!(
            admission_filter(&classic, None, &KeyspaceMemory::default()),
            Ok(None)
        );
        // A dotted user on single-keyspace backends is not filtered.
        assert_eq!(
            admission_filter(&classic, Some("ks1"), &KeyspaceMemory::default()),
            Ok(None)
        );
        let shared = [backend("a:1", set(&["ks1"]))];
        assert_eq!(
            admission_filter(&shared, None, &KeyspaceMemory::default()),
            Ok(None)
        );
        assert!(admits(&shared[0], None));
    }

    #[test]
    fn served_keyspace_filters_candidates() {
        let backends = [
            backend("a:1", set(&["ks1", "ks2"])),
            backend("b:1", set(&["ks3"])),
            backend("c:1", KeyspaceScope::Single("ks1".into())),
            backend("d:1", KeyspaceScope::default()),
        ];
        let filter = admission_filter(&backends, Some("ks1"), &KeyspaceMemory::default());
        assert_eq!(filter, Ok(Some("ks1")));
        let admitted: Vec<_> = backends
            .iter()
            .filter(|backend| admits(backend, Some("ks1")))
            .map(|backend| backend.backend.addr.as_str())
            .collect();
        assert_eq!(admitted, ["a:1", "c:1"]);
    }

    #[test]
    fn any_serves_every_keyspace() {
        let backends = [
            backend("a:1", KeyspaceScope::Any),
            backend("b:1", set(&["ks1"])),
        ];
        assert_eq!(
            admission_filter(&backends, Some("cold"), &KeyspaceMemory::default()),
            Ok(Some("cold"))
        );
        assert!(admits(&backends[0], Some("cold")));
        assert!(!admits(&backends[1], Some("cold")));
    }

    #[test]
    fn unserved_keyspace_is_refused() {
        let backends = [
            backend("a:1", set(&["ks1"])),
            backend("b:1", KeyspaceScope::default()),
        ];
        assert_eq!(
            admission_filter(&backends, Some("ks9"), &KeyspaceMemory::default()),
            Err(RouteError::KeyspaceNotServed)
        );
        let empty = [backend("a:1", KeyspaceScope::Set(BTreeSet::new()))];
        assert_eq!(
            admission_filter(&empty, Some("ks1"), &KeyspaceMemory::default()),
            Err(RouteError::KeyspaceNotServed)
        );
    }
}
