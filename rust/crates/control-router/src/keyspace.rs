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
//! the namespace's `keyspace-routing` setting decides whether the keyspace
//! narrows the backend candidates of the resolved namespace.

use control_topology::MergedBackend;

use crate::RouteError;

/// The keyspace a client selected with its user name: the nonempty prefix
/// before the first `.`, or `None` when the name has no such prefix.
#[must_use]
pub fn user_keyspace(user: &str) -> Option<&str> {
    user.split_once('.')
        .map(|(prefix, _)| prefix)
        .filter(|prefix| !prefix.is_empty())
}

/// The backend candidate rule for one admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyspaceFilter<'a> {
    /// `keyspace-routing = false`: today's routing over every backend except
    /// shared ones, which can neither be selected nor attribute a legacy
    /// login to a keyspace.
    Legacy,
    /// `keyspace-routing = true` and no user-name prefix: every backend.
    Unprefixed,
    /// `keyspace-routing = true` and a `<keyspace>.<user>` login: only the
    /// backends serving that keyspace.
    Keyspace(&'a str),
}

/// Resolves the candidate rule for one admission from the namespace's
/// explicit `keyspace-routing` setting and the client's user-name prefix.
///
/// The decision never depends on what discovery currently shows, so it is
/// stable across restarts and lease expiry: with `keyspace-routing = true` a
/// prefixed login is always filtered, and it is refused with
/// [`RouteError::KeyspaceNotServed`] when no discovered backend (regardless
/// of health) serves its keyspace (`Single` equal, `Set` containing it, or
/// `Any`). There is no fallback to a non-serving backend.
///
/// # Errors
///
/// [`RouteError::KeyspaceNotServed`] as described above.
pub(crate) fn admission_filter<'a>(
    backends: &[MergedBackend],
    keyspace_routing: bool,
    keyspace: Option<&'a str>,
) -> Result<KeyspaceFilter<'a>, RouteError> {
    if !keyspace_routing {
        return Ok(KeyspaceFilter::Legacy);
    }
    let Some(keyspace) = keyspace else {
        return Ok(KeyspaceFilter::Unprefixed);
    };
    if backends
        .iter()
        .any(|backend| backend.backend.serves.serves(keyspace))
    {
        Ok(KeyspaceFilter::Keyspace(keyspace))
    } else {
        Err(RouteError::KeyspaceNotServed)
    }
}

/// Whether `backend` is a candidate under an (already resolved) filter.
pub(crate) fn admits(backend: &MergedBackend, filter: KeyspaceFilter<'_>) -> bool {
    match filter {
        KeyspaceFilter::Legacy => !backend.backend.serves.is_shared(),
        KeyspaceFilter::Unprefixed => true,
        KeyspaceFilter::Keyspace(keyspace) => backend.backend.serves.serves(keyspace),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use control_topology::{BackendInfo, KeyspaceScope, MergedBackend};

    use super::{KeyspaceFilter, RouteError, admission_filter, admits, user_keyspace};

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

    fn admitted<'a>(backends: &'a [MergedBackend], filter: KeyspaceFilter<'_>) -> Vec<&'a str> {
        backends
            .iter()
            .filter(|backend| admits(backend, filter))
            .map(|backend| backend.backend.addr.as_str())
            .collect()
    }

    fn mixed() -> [MergedBackend; 4] {
        [
            backend("a:1", set(&["ks1", "ks2"])),
            backend("b:1", KeyspaceScope::Any),
            backend("c:1", KeyspaceScope::Single("ks1".into())),
            backend("d:1", KeyspaceScope::default()),
        ]
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
    fn legacy_namespace_ignores_the_prefix_and_never_selects_shared() {
        let backends = mixed();
        for keyspace in [None, Some("ks1"), Some("ks9")] {
            let filter = admission_filter(&backends, false, keyspace);
            assert_eq!(filter, Ok(KeyspaceFilter::Legacy));
            assert_eq!(admitted(&backends, KeyspaceFilter::Legacy), ["c:1", "d:1"]);
        }
    }

    #[test]
    fn keyspace_routing_filters_a_prefixed_login() {
        let backends = mixed();
        let filter = admission_filter(&backends, true, Some("ks1"));
        assert_eq!(filter, Ok(KeyspaceFilter::Keyspace("ks1")));
        assert_eq!(
            admitted(&backends, KeyspaceFilter::Keyspace("ks1")),
            ["a:1", "b:1", "c:1"]
        );
        // Any serves a cold keyspace nobody lists.
        assert_eq!(
            admitted(&backends, KeyspaceFilter::Keyspace("cold")),
            ["b:1"]
        );
    }

    #[test]
    fn keyspace_routing_without_prefix_keeps_every_backend() {
        let backends = mixed();
        assert_eq!(
            admission_filter(&backends, true, None),
            Ok(KeyspaceFilter::Unprefixed)
        );
        assert_eq!(admitted(&backends, KeyspaceFilter::Unprefixed).len(), 4);
    }

    #[test]
    fn keyspace_routing_refuses_an_unserved_keyspace_whatever_discovery_shows() {
        let without_shared = [
            backend("c:1", KeyspaceScope::Single("ks1".into())),
            backend("d:1", KeyspaceScope::default()),
        ];
        assert_eq!(
            admission_filter(&without_shared, true, Some("ks9")),
            Err(RouteError::KeyspaceNotServed)
        );
        assert_eq!(
            admission_filter(&[], true, Some("ks1")),
            Err(RouteError::KeyspaceNotServed)
        );
        let empty = [backend("a:1", KeyspaceScope::Set(BTreeSet::new()))];
        assert_eq!(
            admission_filter(&empty, true, Some("ks1")),
            Err(RouteError::KeyspaceNotServed)
        );
    }
}
