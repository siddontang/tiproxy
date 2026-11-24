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

use control_topology::{KeyspaceScope, MergedBackend};

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

impl<'a> KeyspaceFilter<'a> {
    /// The keyspace an admission under this rule records as the session
    /// keyspace: only a `<keyspace>.<user>` login in a `keyspace-routing`
    /// namespace has one. A legacy namespace never treats a user-name prefix
    /// as a keyspace (it need not be one there).
    pub(crate) const fn session_keyspace(self) -> Option<&'a str> {
        match self {
            Self::Keyspace(keyspace) => Some(keyspace),
            Self::Legacy | Self::Unprefixed => None,
        }
    }
}

/// The record `backend_id` has in the current routing snapshot, if any.
///
/// Tenant-aware redirect checks read scopes from here, never from the
/// router's retained backend state, which keeps a withdrawn address's last
/// record while sessions still own it.
pub(crate) fn current_record<'a>(
    backends: &'a [MergedBackend],
    backend_id: &str,
) -> Option<&'a MergedBackend> {
    backends
        .iter()
        .find(|backend| backend.backend_id.as_ref() == backend_id)
}

/// The session side of the tenant rule for a redirect in a `keyspace-routing`
/// namespace with `balance.tenant-migration` on: whether a session admitted
/// on (or last moved onto) `admitted`, now owned by the current record
/// `owner`, may be moved or reconnected at all.
///
/// - A session with a keyspace (design 5.7 K3): `admitted` and `owner` must
///   serve it. A shared backend serving it qualifies; this is the only pin
///   the flag relaxes. An `Unverified` record serves no keyspace.
/// - A session without one: `admitted` must be a verified single keyspace and
///   `owner` must still be exactly that one, as in PR 1.
pub(crate) fn session_on_tenant(
    keyspace: Option<&str>,
    admitted: &KeyspaceScope,
    owner: &MergedBackend,
) -> bool {
    match keyspace {
        Some(keyspace) => admitted.serves(keyspace) && owner.backend.serves.serves(keyspace),
        None => matches!(admitted, KeyspaceScope::Single(tenant) if verified_tenant(owner, tenant)),
    }
}

/// The target side of the same rule, on the current records of the source
/// and the target.
///
/// - With a session keyspace: both must serve it (`Single` equal, `Set`
///   containing it, or `Any`), whether either is shared, so multi-keyspace
///   <-> single-keyspace moves within the tenant are allowed and
///   cross-tenant moves refused.
/// - Without one (including balance pair planning): both must be the same
///   verified single keyspace.
pub(crate) fn tenant_target(
    keyspace: Option<&str>,
    source: &MergedBackend,
    target: &MergedBackend,
) -> bool {
    match keyspace {
        Some(keyspace) => {
            source.backend.serves.serves(keyspace) && target.backend.serves.serves(keyspace)
        }
        None => matches!(&source.backend.serves, KeyspaceScope::Single(tenant)
            if verified_tenant(target, tenant)),
    }
}

/// Whether `backend`'s authoritative discovery scope is the verified single
/// keyspace `tenant`. Tenant-aware code compares this scope (`serves`), never
/// the assignment keyspace, which may come from an operator `keyspace` label
/// on a classic record. Shared and unverified records never qualify.
pub(crate) fn verified_tenant(backend: &MergedBackend, tenant: &str) -> bool {
    matches!(&backend.backend.serves, KeyspaceScope::Single(own) if own == tenant)
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
    fn only_a_keyspace_routed_prefix_is_a_session_keyspace() {
        assert_eq!(
            KeyspaceFilter::Keyspace("ks1").session_keyspace(),
            Some("ks1")
        );
        assert_eq!(KeyspaceFilter::Unprefixed.session_keyspace(), None);
        assert_eq!(KeyspaceFilter::Legacy.session_keyspace(), None);
    }

    #[test]
    fn tenant_rule_compares_discovery_scopes() {
        use super::{session_on_tenant, tenant_target};
        let shared_ab = backend("a:1", set(&["ksa", "ksb"]));
        let single_a = backend("b:1", KeyspaceScope::Single("ksa".into()));
        let single_b = backend("c:1", KeyspaceScope::Single("ksb".into()));
        let any = backend("d:1", KeyspaceScope::Any);
        let unverified = backend("e:1", KeyspaceScope::Unverified("ksa".into()));
        // With a session keyspace: serving is enough, shared or not.
        for target in [&single_a, &shared_ab, &any] {
            assert!(tenant_target(Some("ksa"), &shared_ab, target));
        }
        for target in [&single_b, &unverified] {
            assert!(!tenant_target(Some("ksa"), &shared_ab, target));
        }
        assert!(session_on_tenant(
            Some("ksa"),
            &shared_ab.backend.serves,
            &shared_ab
        ));
        assert!(!session_on_tenant(
            Some("ksa"),
            &unverified.backend.serves,
            &single_a
        ));
        assert!(!session_on_tenant(
            Some("ksa"),
            &single_a.backend.serves,
            &single_b
        ));
        // Without one: the verified single keyspace only.
        assert!(tenant_target(None, &single_a, &single_a));
        for target in [&shared_ab, &any, &unverified, &single_b] {
            assert!(!tenant_target(None, &single_a, target));
        }
        assert!(!tenant_target(None, &shared_ab, &shared_ab));
        assert!(session_on_tenant(None, &single_a.backend.serves, &single_a));
        assert!(!session_on_tenant(
            None,
            &shared_ab.backend.serves,
            &shared_ab
        ));
        assert!(!session_on_tenant(
            None,
            &single_a.backend.serves,
            &unverified
        ));
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
    fn verified_tenant_uses_the_discovery_scope_never_a_label() {
        use super::verified_tenant;
        let mut labelled = backend("a:1", KeyspaceScope::default());
        labelled
            .backend
            .labels
            .insert("keyspace".to_owned(), "ksb".to_owned());
        assert!(
            !verified_tenant(&labelled, "ksb"),
            "a label is not a tenant"
        );
        assert!(verified_tenant(&labelled, ""));
        let single = backend("b:1", KeyspaceScope::Single("ksb".into()));
        assert!(verified_tenant(&single, "ksb") && !verified_tenant(&single, "ksa"));
        for scope in [
            KeyspaceScope::Unverified("ksb".into()),
            set(&["ksb"]),
            KeyspaceScope::Any,
        ] {
            assert!(!verified_tenant(&backend("c:1", scope), "ksb"));
        }
    }

    #[test]
    fn unverified_record_serves_legacy_and_unprefixed_logins_only() {
        let backends = [backend("a:1", KeyspaceScope::Unverified("ksb".into()))];
        assert_eq!(admitted(&backends, KeyspaceFilter::Legacy), ["a:1"]);
        assert_eq!(admitted(&backends, KeyspaceFilter::Unprefixed), ["a:1"]);
        assert!(admitted(&backends, KeyspaceFilter::Keyspace("ksb")).is_empty());
        assert_eq!(
            admission_filter(&backends, true, Some("ksb")),
            Err(RouteError::KeyspaceNotServed)
        );
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
