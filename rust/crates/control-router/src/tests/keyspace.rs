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

//! Multi-keyspace (shared) backends: username-prefix routing and the
//! migration/balance exclusion that stays in force until shared-backend
//! migration is qualified end to end.

use super::*;
use crate::{MigrationSimulation, Session};
use std::time::Instant;

const PROCESS: &str = "/topology/tidb/";
const KS2_PATH: &str = "/keyspaces/tidb/ks2/topology/tidb/";

const SET_KS1: &str = "default/127.0.0.1:4000";
const ANY: &str = "default/127.0.0.1:4001";
const CLASSIC: &str = "default/127.0.0.1:4002";
const SINGLE_KS2: &str = "default/127.0.0.1:4003";

fn shared(keyspaces: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({"ip":"127.0.0.1","multi_keyspace":true,"keyspaces":keyspaces})
}

fn classic() -> serde_json::Value {
    serde_json::json!({"ip":"127.0.0.1"})
}

/// A `Set{ks1}`, an `Any`, a classic and a `Single(ks2)` backend.
fn mixed(with_any: bool) -> Vec<(&'static str, &'static str, serde_json::Value)> {
    let mut records = vec![
        (
            PROCESS,
            "127.0.0.1:4000",
            shared(&serde_json::json!(["ks1"])),
        ),
        (PROCESS, "127.0.0.1:4002", classic()),
        (KS2_PATH, "127.0.0.1:4003", classic()),
    ];
    if with_any {
        records.push((PROCESS, "127.0.0.1:4001", shared(&serde_json::json!("*"))));
    }
    records
}

async fn harness_with(
    records: &[(&str, &str, serde_json::Value)],
    expected: usize,
) -> TestResult<(Harness, Candidate)> {
    let h = Harness::with_health(
        "",
        "connection",
        &[("127.0.0.1:4000", &[])],
        HealthCheckConfig {
            enabled: false,
            interval_nanos: 3_600_000_000_000,
            ..HealthCheckConfig::default()
        },
        "",
    )
    .await?;
    let first = h.ready().await;
    h.fixture.info_records(records);
    let candidate = published(&h, &first, expected).await;
    Ok((h, candidate))
}

/// Waits for the routing snapshot replacing `old` with `expected` backends.
async fn published(h: &Harness, old: &Candidate, expected: usize) -> Candidate {
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = h.router.capture()
                    && !Arc::ptr_eq(&candidate.routing, &old.routing)
                    && candidate.routing.backends.backends.len() == expected
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}

/// One reservation attempt, returning the selected backend id.
fn pick(
    router: &Router,
    candidate: &Candidate,
    keyspace: Option<&str>,
    excluded: &[&str],
) -> Result<String, RouteError> {
    let session = router.open()?;
    let result = router
        .reserve_in_keyspace(
            &session,
            candidate,
            ClientInfo::default(),
            "",
            excluded,
            keyspace,
        )
        .map(|reservation| {
            router.finish(&reservation, false);
            reservation.assignment().backend_id.clone()
        });
    router.close(&session);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefix_routes_only_to_serving_backends() -> TestResult {
    let (h, c) = harness_with(&mixed(true), 4).await?;
    let r = &h.router;

    // `ks1` is served by Set{ks1} and Any only, never by classic/Single(ks2).
    for _ in 0..8 {
        let id = must(pick(r, &c, Some("ks1"), &[]));
        assert!(id == SET_KS1 || id == ANY, "ks1 routed to {id}");
    }
    assert_eq!(must(pick(r, &c, Some("ks1"), &[SET_KS1])), ANY);
    assert_eq!(must(pick(r, &c, Some("ks1"), &[ANY])), SET_KS1);
    assert_eq!(
        pick(r, &c, Some("ks1"), &[SET_KS1, ANY]),
        Err(RouteError::NoBackend),
        "an excluded serving set must not fall back to a non-serving backend"
    );

    // `ks2` is served by the per-keyspace Single(ks2) record and by Any.
    assert_eq!(must(pick(r, &c, Some("ks2"), &[ANY])), SINGLE_KS2);
    assert_eq!(must(pick(r, &c, Some("ks2"), &[SINGLE_KS2])), ANY);

    // Any serves a cold keyspace nobody else lists.
    for _ in 0..4 {
        assert_eq!(must(pick(r, &c, Some("cold"), &[])), ANY);
    }

    // No prefix keeps today's rule: every backend is a candidate.
    assert_eq!(
        must(pick(r, &c, None, &[SET_KS1, ANY, SINGLE_KS2])),
        CLASSIC
    );
    assert_eq!(
        must(pick(r, &c, None, &[ANY, CLASSIC, SINGLE_KS2])),
        SET_KS1
    );
    assert_eq!(r.check_keyspace(None), Ok(()));
    assert_eq!(r.check_keyspace(Some("cold")), Ok(()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unserved_prefix_is_refused_without_fallback() -> TestResult {
    let (h, c) = harness_with(&mixed(false), 3).await?;
    let r = &h.router;
    assert_eq!(
        pick(r, &c, Some("ks9"), &[]),
        Err(RouteError::KeyspaceNotServed)
    );
    assert_eq!(
        r.check_keyspace(Some("ks9")),
        Err(RouteError::KeyspaceNotServed)
    );
    assert_eq!(r.check_keyspace(Some("ks1")), Ok(()));
    assert_eq!(must(pick(r, &c, Some("ks1"), &[])), SET_KS1);

    // The connection-scoped selector carries the keyspace for every attempt,
    // including the retry that clears exclusions.
    let mut selector = must(Arc::clone(r).selector());
    selector.set_keyspace(Some("ks9"));
    assert!(matches!(
        selector.next_candidate(&c, ClientInfo::default(), ""),
        Err(RouteError::KeyspaceNotServed)
    ));
    let mut selector = must(Arc::clone(r).selector());
    selector.set_keyspace(Some("ks1"));
    for _ in 0..3 {
        let reservation = must(selector.next_candidate(&c, ClientInfo::default(), ""));
        assert_eq!(reservation.assignment().backend_id, SET_KS1);
        assert_eq!(selector.finish(&reservation, false), Settlement::Applied);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classic_fleet_with_dotted_user_routes_as_today() -> TestResult {
    const CLASSIC_B: &str = "default/127.0.0.1:4004";
    let (h, c) = harness_with(
        &[
            (PROCESS, "127.0.0.1:4002", classic()),
            (PROCESS, "127.0.0.1:4004", classic()),
        ],
        2,
    )
    .await?;
    // With only classic backends the prefix is not a keyspace selector.
    assert_eq!(h.router.check_keyspace(Some("ks9")), Ok(()));
    assert_eq!(
        must(pick(&h.router, &c, Some("ks9"), &[CLASSIC_B])),
        CLASSIC
    );
    assert_eq!(
        must(pick(&h.router, &c, Some("ks9"), &[CLASSIC])),
        CLASSIC_B
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_shared_single_keyspace_fleet_routes_as_today() -> TestResult {
    const KS1_PATH: &str = "/keyspaces/tidb/ks1/topology/tidb/";
    const KS1_A: &str = "default/127.0.0.1:4005";
    const KS1_B: &str = "default/127.0.0.1:4006";
    let (h, c) = harness_with(
        &[
            (KS1_PATH, "127.0.0.1:4005", classic()),
            (KS1_PATH, "127.0.0.1:4006", classic()),
            (KS2_PATH, "127.0.0.1:4003", classic()),
        ],
        3,
    )
    .await?;
    let r = &h.router;
    // Existing single-keyspace deployments may use a user-name prefix that
    // is not the keyspace name: without a shared backend ever seen, the
    // prefix selects nothing.
    assert_eq!(r.check_keyspace(Some("ks9")), Ok(()));
    let id = must(pick(r, &c, Some("ks9"), &[]));
    assert!([KS1_A, KS1_B, SINGLE_KS2].contains(&id.as_str()), "{id}");
    assert_eq!(must(pick(r, &c, Some("ks1"), &[KS1_A, KS1_B])), SINGLE_KS2);
    assert_eq!(must(pick(r, &c, Some("ks9"), &[KS1_A, SINGLE_KS2])), KS1_B);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_shared_backends_keep_the_filter_active() -> TestResult {
    let (h, c) = harness_with(&mixed(true), 4).await?;
    assert_eq!(must(pick(&h.router, &c, Some("ks1"), &[ANY])), SET_KS1);
    // Every shared backend's lease expires; classic and Single(ks2) remain.
    h.fixture.info_records(&[
        (PROCESS, "127.0.0.1:4002", classic()),
        (KS2_PATH, "127.0.0.1:4003", classic()),
    ]);
    let c = published(&h, &c, 2).await;
    let r = &h.router;
    assert_eq!(
        pick(r, &c, Some("ks1"), &[]),
        Err(RouteError::KeyspaceNotServed),
        "ks1 must not fall back to the classic or ks2 backend"
    );
    assert_eq!(
        r.check_keyspace(Some("ks1")),
        Err(RouteError::KeyspaceNotServed)
    );
    for _ in 0..4 {
        assert_eq!(must(pick(r, &c, Some("ks2"), &[])), SINGLE_KS2);
    }
    assert_eq!(must(pick(r, &c, None, &[SINGLE_KS2])), CLASSIC);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_plane_admission_refuses_an_unserved_prefix() -> TestResult {
    let (h, _) = harness_with(&mixed(false), 3).await?;
    let (plane, mut handle) = RoutePlane::new(Arc::new(h.source.clone()), h.topology.clone(), None);
    let context = h.runtime.handle().module_context();
    let plane_task = tokio::spawn(async move { Box::new(plane).run(context).await });
    tokio::time::timeout(Duration::from_secs(5), handle.wait_ready()).await??;
    // Wait until the plane's router observes the shared topology.
    let mut admitted = None;
    for _ in 0..400 {
        match handle.admit("ks9.app") {
            Err(RouteError::KeyspaceNotServed) => {
                admitted = Some(());
                break;
            }
            _ => tokio::task::yield_now().await,
        }
    }
    assert!(admitted.is_some(), "unserved keyspace must be refused");
    assert!(matches!(
        handle
            .admit_within("ks9.app", Duration::from_millis(100))
            .await,
        Err(RouteError::KeyspaceNotServed)
    ));
    let mut served = must(handle.admit("ks1.app"));
    assert_eq!(served.namespace(), "default");
    assert_eq!(served.selector().keyspace(), Some("ks1"));
    let reservation = must(served.selector_mut().next(ClientInfo::default(), ""));
    assert_eq!(reservation.assignment().backend_id, SET_KS1);
    let plain = must(handle.admit("app"));
    assert_eq!(plain.selector().keyspace(), None);
    drop((served, plain));

    // The only shared backend expires. The plane's router remembers that the
    // namespace had one, so `ks1.app` is refused rather than falling back to
    // the classic or Single(ks2) backend, and `ks2.app` stays on ks2.
    h.fixture.info_records(&[
        (PROCESS, "127.0.0.1:4002", classic()),
        (KS2_PATH, "127.0.0.1:4003", classic()),
    ]);
    let refused = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if matches!(handle.admit("ks1.app"), Err(RouteError::KeyspaceNotServed)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        refused.is_ok(),
        "expired shared backend must stay remembered"
    );
    for _ in 0..4 {
        let mut ks2 = must(handle.admit("ks2.app"));
        let reservation = must(ks2.selector_mut().next(ClientInfo::default(), ""));
        assert_eq!(reservation.assignment().backend_id, SINGLE_KS2);
    }
    plane_task.abort();
    Ok(())
}

fn simulation(h: &Harness) -> MigrationSimulation {
    must(MigrationSimulation::new(
        Arc::new(h.source.clone()),
        &h.topology,
        &h.runtime.handle().module_context(),
        "default",
        16,
        8,
        None,
    ))
}

/// Waits for the simulation router to observe `expected` backends.
async fn sim_ready(sim: &MigrationSimulation, expected: usize) -> Candidate {
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && candidate.routing.backends.backends.len() == expected
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}

/// An active physical session on `id`, selected by excluding every other id.
fn active_on(sim: &MigrationSimulation, c: &Candidate, id: &str, others: &[&str]) -> Session {
    let session = must(sim.router().open());
    let reservation = must(
        sim.router()
            .reserve(&session, c, ClientInfo::default(), "", others),
    );
    assert_eq!(reservation.assignment().backend_id, id);
    assert_eq!(sim.router().finish(&reservation, true), Settlement::Applied);
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_backend_is_never_a_migration_source_or_target() -> TestResult {
    // One shared Set{ks1} backend and one classic backend in one group.
    let (h, _) = harness_with(
        &[
            (
                PROCESS,
                "127.0.0.1:4000",
                shared(&serde_json::json!(["ks1"])),
            ),
            (PROCESS, "127.0.0.1:4002", classic()),
        ],
        2,
    )
    .await?;
    let sim = simulation(&h);
    let c = sim_ready(&sim, 2).await;
    let on_shared = active_on(&sim, &c, SET_KS1, &[CLASSIC]);
    let on_classic = active_on(&sim, &c, CLASSIC, &[SET_KS1]);

    // Direct redirects in both directions are refused as cross-keyspace.
    assert!(matches!(
        sim.prepare(&on_classic, &c, SET_KS1),
        Err(RouteError::CrossKeyspace)
    ));
    assert!(matches!(
        sim.prepare(&on_shared, &c, CLASSIC),
        Err(RouteError::CrossKeyspace)
    ));

    // Self-redirect (management reconnect) skips the shared session only.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!(summary.offered, 2);
    assert_eq!(summary.accepted, 1);
    let redirect = queue.take_redirect().ok_or("classic self-redirect")?;
    assert_eq!(redirect.to().backend_id, CLASSIC);
    assert!(queue.take_redirect().is_none());
    assert_eq!(sim.finish(&redirect, true), Settlement::Applied);

    // Draining the classic backend proposes a pair onto the shared backend;
    // balance refuses it and moves nothing.
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4002\"]", 3);
    h.source.deliver();
    h.applied().await;
    let drain_classic = must(
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && !Arc::ptr_eq(&candidate.config, &c.config)
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    );
    assert!(matches!(
        sim.prepare_balance(&drain_classic, ClientInfo::default(), ""),
        Err(RouteError::CrossKeyspace)
    ));

    // Draining the shared backend proposes a pair off it; also refused.
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4000\"]", 4);
    h.source.deliver();
    h.applied().await;
    let drain_shared = must(
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && !Arc::ptr_eq(&candidate.config, &drain_classic.config)
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    );
    assert!(matches!(
        sim.prepare_balance(&drain_shared, ClientInfo::default(), ""),
        Err(RouteError::CrossKeyspace)
    ));

    // A full scheduler round admits no redirect for either drain.
    let (_stop_tx, stop) = watch::channel(false);
    let now = Instant::now();
    must(sim.round_at(&drain_shared, true, &stop, now, now, 1));
    assert!(sim.take_redirect().is_none(), "no shared-backend migration");
    sim.router().close(&on_shared);
    sim.router().close(&on_classic);
    Ok(())
}

#[test]
fn shared_backend_assignment_has_no_single_keyspace() {
    // A shared backend never borrows an operator `keyspace` label as its
    // assignment keyspace; a single-keyspace backend keeps the label fallback.
    let merged = |serves| control_topology::MergedBackend {
        backend_id: Arc::from("c/a:1"),
        cluster_name: Arc::from("c"),
        backend: control_topology::BackendInfo {
            addr: "a:1".into(),
            serves,
            ip: String::new(),
            status_port: 0,
            version: String::new(),
            git_hash: String::new(),
            deploy_path: String::new(),
            start_timestamp: 0,
            labels: BTreeMap::from([("keyspace".to_owned(), "label".to_owned())]),
        },
    };
    let shared = crate::selector::assignment_keyspace_for_test(&merged(
        control_topology::KeyspaceScope::Any,
    ));
    assert_eq!(shared, "");
    let single = crate::selector::assignment_keyspace_for_test(&merged(
        control_topology::KeyspaceScope::default(),
    ));
    assert_eq!(single, "label");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_follows_tenant_between_existing_addresses() -> TestResult {
    let records = |left, right| {
        [
            (
                PROCESS,
                "127.0.0.1:4000",
                shared(&serde_json::json!([left])),
            ),
            (
                PROCESS,
                "127.0.0.1:4001",
                shared(&serde_json::json!([right])),
            ),
        ]
    };
    let (h, first) = harness_with(&records("a", "b"), 2).await?;
    let mut selector = must(h.router.selector());
    selector.set_keyspace(Some("a"));
    let before = must(selector.next_candidate(&first, ClientInfo::default(), ""));
    assert_eq!(before.assignment().backend_address, "127.0.0.1:4000");
    assert_eq!(selector.finish(&before, false), Settlement::Applied);

    // Neither address changes. Only the two process allow-lists change.
    h.fixture.info_records(&records("b", "b"));
    let withdrawn = published(&h, &first, 2).await;
    assert!(matches!(
        selector.next_candidate(&withdrawn, ClientInfo::default(), ""),
        Err(RouteError::KeyspaceNotServed)
    ));

    h.fixture.info_records(&records("b", "a"));
    let restored = published(&h, &withdrawn, 2).await;
    let after = must(selector.next_candidate(&restored, ClientInfo::default(), ""));
    assert_eq!(after.assignment().backend_address, "127.0.0.1:4001");
    assert_eq!(selector.finish(&after, false), Settlement::Applied);
    Ok(())
}
