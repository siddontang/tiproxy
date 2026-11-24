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

//! Multi-keyspace (shared) backends: the namespace `keyspace-routing` mode,
//! username-prefix routing, and the migration/balance exclusion that stays in
//! force until shared-backend migration is qualified end to end.

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

/// A harness whose `default` namespace has the given `keyspace-routing`
/// setting, publishing `records` and waiting for `expected` backends.
async fn harness_with(
    records: &[(&str, &str, serde_json::Value)],
    expected: usize,
    keyspace_routing: bool,
) -> TestResult<(Harness, Candidate)> {
    let h = Harness::with_fixture_routing(
        "",
        "connection",
        &[("127.0.0.1:4000", &[])],
        HealthCheckConfig {
            enabled: false,
            interval_nanos: 3_600_000_000_000,
            ..HealthCheckConfig::default()
        },
        "",
        KvFixture::new().await?,
        keyspace_routing,
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
    let (h, c) = harness_with(&mixed(true), 4, true).await?;
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

    // No prefix: every backend is a candidate, shared ones included.
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
    let (h, c) = harness_with(&mixed(false), 3, true).await?;
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
async fn legacy_classic_fleet_with_dotted_user_routes_as_today() -> TestResult {
    const CLASSIC_B: &str = "default/127.0.0.1:4004";
    let (h, c) = harness_with(
        &[
            (PROCESS, "127.0.0.1:4002", classic()),
            (PROCESS, "127.0.0.1:4004", classic()),
        ],
        2,
        false,
    )
    .await?;
    // In a legacy namespace the prefix is not a keyspace selector.
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
async fn legacy_single_keyspace_fleet_routes_as_today() -> TestResult {
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
        false,
    )
    .await?;
    let r = &h.router;
    // Existing single-keyspace deployments may use a user-name prefix that
    // is not the keyspace name; legacy mode never filters by it.
    assert_eq!(r.check_keyspace(Some("ks9")), Ok(()));
    let id = must(pick(r, &c, Some("ks9"), &[]));
    assert!([KS1_A, KS1_B, SINGLE_KS2].contains(&id.as_str()), "{id}");
    assert_eq!(must(pick(r, &c, Some("ks1"), &[KS1_A, KS1_B])), SINGLE_KS2);
    assert_eq!(must(pick(r, &c, Some("ks9"), &[KS1_A, SINGLE_KS2])), KS1_B);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_namespace_never_selects_a_shared_backend() -> TestResult {
    let (h, c) = harness_with(&mixed(true), 4, false).await?;
    let r = &h.router;
    for keyspace in [None, Some("ks1"), Some("cold")] {
        for _ in 0..6 {
            let id = must(pick(r, &c, keyspace, &[]));
            assert!(id == CLASSIC || id == SINGLE_KS2, "{keyspace:?} -> {id}");
        }
        assert_eq!(
            pick(r, &c, keyspace, &[CLASSIC, SINGLE_KS2]),
            Err(RouteError::NoBackend),
            "shared backends are never a fallback in a legacy namespace"
        );
    }
    assert_eq!(r.check_keyspace(Some("ks9")), Ok(()));
    // Each shared backend is reported once, not on every observation.
    let mut warned = r.warn_legacy_shared(&c);
    warned.sort();
    assert_eq!(warned, [Arc::<str>::from(SET_KS1), Arc::<str>::from(ANY)]);
    assert!(r.warn_legacy_shared(&c).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyspace_routing_filters_without_any_shared_backend() -> TestResult {
    const KS1_PATH: &str = "/keyspaces/tidb/ks1/topology/tidb/";
    const KS1_A: &str = "default/127.0.0.1:4005";
    let (h, c) = harness_with(
        &[
            (KS1_PATH, "127.0.0.1:4005", classic()),
            (KS2_PATH, "127.0.0.1:4003", classic()),
            (PROCESS, "127.0.0.1:4002", classic()),
        ],
        3,
        true,
    )
    .await?;
    let r = &h.router;
    // The explicit mode does not depend on what discovery shows.
    for _ in 0..4 {
        assert_eq!(must(pick(r, &c, Some("ks1"), &[])), KS1_A);
    }
    assert_eq!(
        pick(r, &c, Some("ks1"), &[KS1_A]),
        Err(RouteError::NoBackend)
    );
    assert_eq!(
        pick(r, &c, Some("ks9"), &[]),
        Err(RouteError::KeyspaceNotServed)
    );
    assert_eq!(
        r.check_keyspace(Some("ks9")),
        Err(RouteError::KeyspaceNotServed)
    );
    assert!(r.warn_legacy_shared(&c).is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyspace_routing_survives_expiry_of_every_shared_backend() -> TestResult {
    let (h, c) = harness_with(&mixed(true), 4, true).await?;
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
    let (h, _) = harness_with(&mixed(false), 3, true).await?;
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

    // The only shared backend expires. The namespace still routes by
    // keyspace, so `ks1.app` is refused rather than falling back to the
    // classic or Single(ks2) backend, and `ks2.app` stays on ks2.
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
        "keyspace routing must not depend on discovery"
    );
    for _ in 0..4 {
        let mut ks2 = must(handle.admit("ks2.app"));
        let reservation = must(ks2.selector_mut().next(ClientInfo::default(), ""));
        assert_eq!(reservation.assignment().backend_id, SINGLE_KS2);
    }
    plane_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partially_registered_address_is_not_admitted_for_a_stale_tenant() -> TestResult {
    const ADDR: &str = "127.0.0.1:4000";
    const BACKEND: &str = "default/127.0.0.1:4000";
    let (h, _) = harness_with(&[(PROCESS, "127.0.0.1:4002", classic())], 1, true).await?;
    let (plane, mut handle) = RoutePlane::new(Arc::new(h.source.clone()), h.topology.clone(), None);
    let context = h.runtime.handle().module_context();
    let plane_task = tokio::spawn(async move { Box::new(plane).run(context).await });
    tokio::time::timeout(Duration::from_secs(5), handle.wait_ready()).await??;

    let info = |ks: &str| {
        (
            format!("/keyspaces/tidb/{ks}/topology/tidb/{ADDR}/info"),
            must(serde_json::to_vec(&classic())),
        )
    };
    let ttl = |ks: &str| {
        (
            format!("/keyspaces/tidb/{ks}/topology/tidb/{ADDR}/ttl"),
            b"1".to_vec(),
        )
    };
    // The address moved from ksb to ksa: ksb's info is stale (its own ttl
    // expired), ksa has registered its ttl but not yet its info.
    h.fixture.raw_records(&[info("ksb"), ttl("ksa")]);
    let partial = published(&h, &h.ready().await, 1).await;
    assert_eq!(
        partial.routing.backends.backends[0].backend.serves,
        control_topology::KeyspaceScope::Unverified("ksb".to_owned()),
        "fixture: the legacy fallback keeps the stale record, tenant unknown"
    );
    let refused = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if matches!(handle.admit("ksb.user"), Err(RouteError::KeyspaceNotServed)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(refused.is_ok(), "a stale tenant gets no assignment");
    assert!(matches!(
        handle.admit("ksa.user"),
        Err(RouteError::KeyspaceNotServed)
    ));

    // ksa's info arrives: the address is now verified for ksa only.
    h.fixture
        .raw_records(&[info("ksb"), info("ksa"), ttl("ksa")]);
    let mut admitted = None;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(admission) = handle.admit("ksa.user") {
                admitted = Some(admission);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut admission = admitted.ok_or("ksa admitted")?;
    let reservation = must(admission.selector_mut().next(ClientInfo::default(), ""));
    assert_eq!(reservation.assignment().backend_id, BACKEND);
    assert!(matches!(
        handle.admit("ksb.user"),
        Err(RouteError::KeyspaceNotServed)
    ));
    drop(admission);
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
        true,
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

    // Self-redirect (management reconnect): none in a tenant-aware namespace.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (2, 0));
    assert!(queue.take_redirect().is_none());

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

/// Waits for the simulation router to observe a routing snapshot other than
/// `old`'s.
async fn sim_changed(sim: &MigrationSimulation, old: &Candidate) -> Candidate {
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && !Arc::ptr_eq(&candidate.routing, &old.routing)
                    && !Arc::ptr_eq(&candidate.config, &old.config)
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_admitted_on_shared_backend_stays_ineligible_after_record_flip() -> TestResult {
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
        true,
    )
    .await?;
    let sim = simulation(&h);
    let c = sim_ready(&sim, 2).await;
    let on_shared = active_on(&sim, &c, SET_KS1, &[CLASSIC]);

    // The address's record flips to single-keyspace (same address, same
    // account) and the classic backend is drained, so every current-scope
    // guard would now allow moving the session.
    h.fixture.info_records(&[
        (PROCESS, "127.0.0.1:4000", classic()),
        (PROCESS, "127.0.0.1:4002", classic()),
    ]);
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4000\"]", 3);
    h.source.deliver();
    h.applied().await;
    let flipped = sim_changed(&sim, &c).await;
    assert!(
        flipped
            .routing
            .backends
            .backends
            .iter()
            .all(|backend| !backend.backend.serves.is_shared()),
        "fixture: no shared record remains"
    );

    // Direct redirect.
    assert!(matches!(
        sim.prepare(&on_shared, &flipped, CLASSIC),
        Err(RouteError::CrossKeyspace)
    ));
    // Balance / failover-driven: the pair is planned, the offer is refused.
    assert!(matches!(
        sim.prepare_balance(&flipped, ClientInfo::default(), ""),
        Err(RouteError::CrossKeyspace)
    ));
    assert!(sim.take_redirect().is_none());
    // A full scheduler round admits nothing either.
    let (_stop_tx, stop) = watch::channel(false);
    let now = Instant::now();
    must(sim.round_at(&flipped, true, &stop, now, now, 1));
    assert!(
        sim.take_redirect().is_none(),
        "no migration of a shared session"
    );
    // Management self-redirect.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (1, 0));
    assert!(queue.take_redirect().is_none());
    sim.router().close(&on_shared);
    Ok(())
}

/// Waits for the simulation router to capture a candidate matching `ready`.
async fn sim_until(sim: &MigrationSimulation, ready: impl Fn(&Candidate) -> bool) -> Candidate {
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && ready(&candidate)
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}

fn scope_of(candidate: &Candidate, id: &str) -> Option<control_topology::KeyspaceScope> {
    candidate
        .routing
        .backends
        .backends
        .iter()
        .find(|backend| backend.backend_id.as_ref() == id)
        .map(|backend| backend.backend.serves.clone())
}

fn keyspace_record(ks: &str, addr: &str, suffix: &str) -> (String, Vec<u8>) {
    let value = if suffix == "info" {
        must(serde_json::to_vec(&classic()))
    } else {
        b"1".to_vec()
    };
    (
        format!("/keyspaces/tidb/{ks}/topology/tidb/{addr}/{suffix}"),
        value,
    )
}

fn active_count(sim: &MigrationSimulation, id: &str) -> u64 {
    sim.router().accounting(id).unwrap_or_default().active()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unverified_record_is_never_a_redirect_source_or_target() -> TestResult {
    const VERIFIED_B: &str = "default/127.0.0.1:4003";
    const UNVERIFIED_B: &str = "default/127.0.0.1:4000";
    let (h, _) = harness_with(&[(PROCESS, "127.0.0.1:4002", classic())], 1, true).await?;
    // 4003 is a verified ksb backend; 4000 carries ksb's stale info and only
    // ksa's ttl, so the legacy fallback keeps it as Unverified(ksb).
    h.fixture.raw_records(&[
        keyspace_record("ksb", "127.0.0.1:4003", "info"),
        keyspace_record("ksb", "127.0.0.1:4003", "ttl"),
        keyspace_record("ksb", "127.0.0.1:4000", "info"),
        keyspace_record("ksa", "127.0.0.1:4000", "ttl"),
    ]);
    let sim = simulation(&h);
    let c = sim_until(&sim, |c| c.routing.backends.backends.len() == 2).await;
    assert_eq!(
        scope_of(&c, UNVERIFIED_B),
        Some(control_topology::KeyspaceScope::Unverified(
            "ksb".to_owned()
        ))
    );
    let on_verified = active_on(&sim, &c, VERIFIED_B, &[UNVERIFIED_B]);
    let on_unverified = active_on(&sim, &c, UNVERIFIED_B, &[VERIFIED_B]);

    // Direct, in both directions.
    assert!(matches!(
        sim.prepare(&on_verified, &c, UNVERIFIED_B),
        Err(RouteError::CrossKeyspace)
    ));
    assert!(matches!(
        sim.prepare(&on_unverified, &c, VERIFIED_B),
        Err(RouteError::CrossKeyspace)
    ));
    // Balance / failover-driven, draining either side.
    let (_stop_tx, stop) = watch::channel(false);
    for (revision, drained) in [(3, "127.0.0.1:4003"), (4, "127.0.0.1:4000")] {
        let before = must(sim.router().capture());
        h.patch(
            &format!("[proxy]\nfail-backend-list=[\"{drained}\"]"),
            revision,
        );
        h.source.deliver();
        h.applied().await;
        let drained = sim_until(&sim, |c| !Arc::ptr_eq(&c.config, &before.config)).await;
        assert!(matches!(
            sim.prepare_balance(&drained, ClientInfo::default(), ""),
            Err(RouteError::CrossKeyspace)
        ));
        let now = Instant::now();
        must(sim.round_at(&drained, true, &stop, now, now, 1));
        assert!(sim.take_redirect().is_none(), "no balance redirect");
    }
    // Reconnect-to-self: no offer for either session.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (2, 0));
    assert!(queue.take_redirect().is_none());
    // Both sessions keep serving where they were.
    assert_eq!(active_count(&sim, VERIFIED_B), 1);
    assert_eq!(active_count(&sim, UNVERIFIED_B), 1);
    sim.router().close(&on_verified);
    sim.router().close(&on_unverified);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_is_not_redirected_after_its_address_changes_tenant() -> TestResult {
    const FLIPPED: &str = "default/127.0.0.1:4003";
    const OTHER_A: &str = "default/127.0.0.1:4002";
    let (h, _) = harness_with(&[(PROCESS, "127.0.0.1:4002", classic())], 1, true).await?;
    h.fixture.raw_records(&[
        keyspace_record("ksb", "127.0.0.1:4003", "info"),
        keyspace_record("ksb", "127.0.0.1:4003", "ttl"),
        keyspace_record("ksa", "127.0.0.1:4002", "info"),
        keyspace_record("ksa", "127.0.0.1:4002", "ttl"),
    ]);
    let sim = simulation(&h);
    let c = sim_until(&sim, |c| {
        scope_of(c, FLIPPED) == Some(control_topology::KeyspaceScope::Single("ksb".to_owned()))
    })
    .await;
    let session = active_on(&sim, &c, FLIPPED, &[OTHER_A]);

    // The address is re-registered as a verified ksa backend while the ksb
    // session stays open, and is drained so balance proposes moving it to
    // the other ksa backend.
    h.fixture.raw_records(&[
        keyspace_record("ksa", "127.0.0.1:4003", "info"),
        keyspace_record("ksa", "127.0.0.1:4003", "ttl"),
        keyspace_record("ksa", "127.0.0.1:4002", "info"),
        keyspace_record("ksa", "127.0.0.1:4002", "ttl"),
    ]);
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4003\"]", 3);
    h.source.deliver();
    h.applied().await;
    let flipped = sim_until(&sim, |candidate| {
        !Arc::ptr_eq(&candidate.config, &c.config)
            && scope_of(candidate, FLIPPED)
                == Some(control_topology::KeyspaceScope::Single("ksa".to_owned()))
    })
    .await;

    // Direct.
    assert!(matches!(
        sim.prepare(&session, &flipped, OTHER_A),
        Err(RouteError::CrossKeyspace)
    ));
    // Balance / failover-driven: the ksa pair is planned, the offer refused.
    assert!(matches!(
        sim.prepare_balance(&flipped, ClientInfo::default(), ""),
        Err(RouteError::CrossKeyspace)
    ));
    assert!(sim.take_redirect().is_none());
    let (_stop_tx, stop) = watch::channel(false);
    let now = Instant::now();
    must(sim.round_at(&flipped, true, &stop, now, now, 1));
    assert!(sim.take_redirect().is_none(), "no balance redirect");
    // Reconnect-to-self onto the reused (now ksa) address.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (1, 0));
    assert!(queue.take_redirect().is_none());
    // The session keeps serving on its original account.
    assert_eq!(active_count(&sim, FLIPPED), 1);
    assert_eq!(active_count(&sim, OTHER_A), 0);
    sim.router().close(&session);
    Ok(())
}

/// Two verified ksa backends (4003, 4002) under `/keyspaces/tidb/ksa`.
fn two_ksa_backends() -> Vec<(String, Vec<u8>)> {
    vec![
        keyspace_record("ksa", "127.0.0.1:4003", "info"),
        keyspace_record("ksa", "127.0.0.1:4003", "ttl"),
        keyspace_record("ksa", "127.0.0.1:4002", "info"),
        keyspace_record("ksa", "127.0.0.1:4002", "ttl"),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_aware_namespace_offers_no_redirect_on_any_path() -> TestResult {
    const HELD: &str = "default/127.0.0.1:4003";
    const OTHER: &str = "default/127.0.0.1:4002";
    let (h, first) = harness_with(&[(PROCESS, "127.0.0.1:4002", classic())], 1, true).await?;
    let sim = simulation(&h);
    // A session on a verified ksa backend, with another verified ksa backend:
    // every legacy rule would allow moving it.
    h.fixture.raw_records(&two_ksa_backends());
    let c = sim_until(&sim, |c| c.routing.backends.backends.len() == 2).await;
    let session = active_on(&sim, &c, HELD, &[OTHER]);
    let queue = crate::scheduler::CommandQueue::new(8);

    // Self-redirect as the first operation after a new publication: the sim
    // router has not refreshed since (the harness router observes it).
    let mut records = two_ksa_backends();
    records.push(keyspace_record("ksa", "127.0.0.1:4005", "info"));
    records.push(keyspace_record("ksa", "127.0.0.1:4005", "ttl"));
    h.fixture.raw_records(&records);
    let published_three = published(&h, &first, 3).await;
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (1, 0));
    assert!(queue.take_redirect().is_none());

    // Direct, balance/failover-driven and a full scheduler round.
    let c = sim_until(&sim, |c| c.routing.backends.backends.len() == 3).await;
    assert!(matches!(
        sim.prepare(&session, &c, OTHER),
        Err(RouteError::CrossKeyspace)
    ));
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4003\"]", 3);
    h.source.deliver();
    h.applied().await;
    let drained = sim_until(&sim, |candidate| !Arc::ptr_eq(&candidate.config, &c.config)).await;
    assert!(matches!(
        sim.prepare_balance(&drained, ClientInfo::default(), ""),
        Err(RouteError::CrossKeyspace)
    ));
    let (_stop_tx, stop) = watch::channel(false);
    let now = Instant::now();
    must(sim.round_at(&drained, true, &stop, now, now, 1));
    assert!(sim.take_redirect().is_none(), "no balance redirect");

    // The held address is withdrawn; the router retains it for the open
    // session. Self-redirect before and after the router refreshes.
    h.fixture.raw_records(&records[2..]);
    published(&h, &published_three, 2).await;
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (1, 0));
    let withdrawn = sim_until(&sim, |c| c.routing.backends.backends.len() == 2).await;
    let now = Instant::now();
    must(sim.round_at(&withdrawn, true, &stop, now, now, 1));
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (1, 0));
    assert!(queue.take_redirect().is_none());
    assert!(sim.take_redirect().is_none());

    // The session keeps serving on its original account.
    assert_eq!(active_count(&sim, HELD), 1);
    sim.router().close(&session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_namespace_still_redirects() -> TestResult {
    const HELD: &str = "default/127.0.0.1:4003";
    const OTHER: &str = "default/127.0.0.1:4002";
    let (h, _) = harness_with(&[(PROCESS, "127.0.0.1:4002", classic())], 1, false).await?;
    let sim = simulation(&h);
    h.fixture.raw_records(&two_ksa_backends());
    let c = sim_until(&sim, |c| c.routing.backends.backends.len() == 2).await;
    let first = active_on(&sim, &c, HELD, &[OTHER]);
    let second = active_on(&sim, &c, HELD, &[OTHER]);

    // Direct redirect is offered and settles.
    let prepared = must(sim.prepare(&first, &c, OTHER));
    assert!(must(sim.offer(&prepared)));
    let redirect = sim.take_redirect().ok_or("direct redirect")?;
    assert_eq!(redirect.to().backend_id, OTHER);
    assert_eq!(sim.finish(&redirect, true), Settlement::Applied);

    // Management reconnect-to-self is offered for every session.
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    assert_eq!((summary.offered, summary.accepted), (2, 2));
    while let Some(redirect) = queue.take_redirect() {
        assert_eq!(sim.finish(&redirect, true), Settlement::Applied);
    }

    // Draining the held backend plans a balance redirect.
    h.patch("[proxy]\nfail-backend-list=[\"127.0.0.1:4003\"]", 3);
    h.source.deliver();
    h.applied().await;
    let drained = sim_until(&sim, |candidate| !Arc::ptr_eq(&candidate.config, &c.config)).await;
    let plan = must(sim.prepare_balance(&drained, ClientInfo::default(), ""))
        .ok_or("legacy drain pair")?;
    assert_eq!(plan.redirects().len(), 1);
    sim.router().close(&first);
    sim.router().close(&second);
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
    let (h, first) = harness_with(&records("a", "b"), 2, true).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_on_one_backend_share_its_scope_allocation() -> TestResult {
    // A long allow-list must be held once per backend record, not per session.
    let tenants: Vec<String> = (0..512).map(|index| format!("ks{index}")).collect();
    let (h, _) = harness_with(
        &[(
            PROCESS,
            "127.0.0.1:4000",
            shared(&serde_json::json!(tenants)),
        )],
        1,
        true,
    )
    .await?;
    let sim = simulation(&h);
    let c = sim_ready(&sim, 1).await;
    let first = active_on(&sim, &c, SET_KS1, &[]);
    let second = active_on(&sim, &c, SET_KS1, &[]);
    let (Some(a), Some(b)) = (
        sim.router().admitted_scope_for_test(&first),
        sim.router().admitted_scope_for_test(&second),
    ) else {
        return Err("both sessions are active".into());
    };
    assert!(Arc::ptr_eq(&a, &b), "one allocation per backend record");
    assert!(a.serves("ks511"));

    // A changed scope gets a new allocation, recorded for new sessions only.
    h.fixture.info_records(&[(
        PROCESS,
        "127.0.0.1:4000",
        shared(&serde_json::json!(["ks1"])),
    )]);
    let changed = sim_changed_routing(&sim, &c).await;
    let third = active_on(&sim, &changed, SET_KS1, &[]);
    let Some(c3) = sim.router().admitted_scope_for_test(&third) else {
        return Err("third session is active".into());
    };
    assert!(!Arc::ptr_eq(&a, &c3));
    assert!(!c3.serves("ks511") && c3.serves("ks1"));
    let Some(still) = sim.router().admitted_scope_for_test(&first) else {
        return Err("first session is still active".into());
    };
    assert!(
        Arc::ptr_eq(&a, &still),
        "admission scope is never rewritten"
    );
    for session in [first, second, third] {
        sim.router().close(&session);
    }
    Ok(())
}

/// Waits for the simulation router to observe a routing snapshot other than
/// `old`'s, whatever the configuration.
async fn sim_changed_routing(sim: &MigrationSimulation, old: &Candidate) -> Candidate {
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && !Arc::ptr_eq(&candidate.routing, &old.routing)
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}
// K3: with `balance.tenant-migration = true`, a session with a
// keyspace may migrate only to a backend serving that keyspace.

const KSA_PATH: &str = "/keyspaces/tidb/ksa/topology/tidb/";
const KSB_PATH: &str = "/keyspaces/tidb/ksb/topology/tidb/";

const SHARED_AB: &str = "default/127.0.0.1:4000";
const SHARED_ANY: &str = "default/127.0.0.1:4001";
const PLAIN: &str = "default/127.0.0.1:4002";
const SINGLE_A: &str = "default/127.0.0.1:4003";
const SINGLE_B: &str = "default/127.0.0.1:4004";
const SHARED_AC: &str = "default/127.0.0.1:4005";
const SINGLE_A2: &str = "default/127.0.0.1:4006";

/// `Set{ksa,ksb}`, `Any`, classic, `Single(ksa)` x2, `Single(ksb)` and
/// `Set{ksa,ksc}`, all in one routing group.
fn k3_records() -> Vec<(&'static str, &'static str, serde_json::Value)> {
    vec![
        (
            PROCESS,
            "127.0.0.1:4000",
            shared(&serde_json::json!(["ksa", "ksb"])),
        ),
        (PROCESS, "127.0.0.1:4001", shared(&serde_json::json!("*"))),
        (PROCESS, "127.0.0.1:4002", classic()),
        (KSA_PATH, "127.0.0.1:4003", classic()),
        (KSB_PATH, "127.0.0.1:4004", classic()),
        (
            PROCESS,
            "127.0.0.1:4005",
            shared(&serde_json::json!(["ksa", "ksc"])),
        ),
        (KSA_PATH, "127.0.0.1:4006", classic()),
    ]
}

const K3_BACKENDS: usize = 7;

/// An active session on `id` admitted with the `keyspace` user-name prefix,
/// selected by excluding every other backend.
fn active_in(
    sim: &MigrationSimulation,
    c: &Candidate,
    id: &str,
    keyspace: Option<&str>,
) -> Session {
    let others: Vec<&str> = c
        .routing
        .backends
        .backends
        .iter()
        .map(|backend| backend.backend_id.as_ref())
        .filter(|other| *other != id)
        .collect();
    let session = must(sim.router().open());
    let reservation = must(sim.router().reserve_in_keyspace(
        &session,
        c,
        ClientInfo::default(),
        "",
        &others,
        keyspace,
    ));
    assert_eq!(reservation.assignment().backend_id, id);
    assert_eq!(sim.router().finish(&reservation, true), Settlement::Applied);
    session
}

/// Sets `balance.tenant-migration` and waits for the simulation to
/// capture it.
async fn tenant_migration(
    h: &Harness,
    sim: &MigrationSimulation,
    enabled: bool,
    revision: u64,
) -> Candidate {
    h.patch(
        &format!("[balance]\ntenant-migration = {enabled}\n"),
        revision,
    );
    h.source.deliver();
    h.applied().await;
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && candidate.policy.tenant_migration == enabled
                    && candidate.routing.backends.backends.len() == K3_BACKENDS
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    )
}

fn active(sim: &MigrationSimulation, id: &str) -> u64 {
    sim.router()
        .accounting(id)
        .map_or(0, crate::Accounting::active)
}

/// Offers `prepared`, settles it successfully and checks it moved.
fn migrate(sim: &MigrationSimulation, prepared: &crate::PreparedRedirect, to: &str) {
    assert!(must(sim.offer(prepared)), "offer to {to} accepted");
    let redirect = must(sim.take_redirect().ok_or("admitted redirect"));
    assert_eq!(redirect.to().backend_id, to);
    assert_eq!(sim.finish(&redirect, true), Settlement::Applied);
}

fn self_redirects(sim: &MigrationSimulation) -> (u64, u64) {
    let queue = crate::scheduler::CommandQueue::new(8);
    let summary = sim.router().redirect_all_to_self(&queue, Instant::now());
    while let Some(redirect) = queue.take_redirect() {
        assert_eq!(sim.finish(&redirect, true), Settlement::Applied);
    }
    (summary.offered, summary.accepted)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_target_must_serve_the_session_keyspace() -> TestResult {
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    let off = sim_ready(&sim, K3_BACKENDS).await;
    assert!(!off.policy.tenant_migration, "the gate is off by default");
    let session = active_in(&sim, &off, SHARED_AB, Some("ksa"));

    // Gate off: exactly PR 1, every move of a shared session is refused.
    for target in [SINGLE_A, SINGLE_B, SHARED_AC, SHARED_ANY, PLAIN] {
        assert!(
            matches!(
                sim.prepare(&session, &off, target),
                Err(RouteError::CrossKeyspace)
            ),
            "gate off: {target}"
        );
    }
    assert_eq!(self_redirects(&sim), (1, 0));

    // Gate on: the target must serve `ksa`.
    let on = tenant_migration(&h, &sim, true, 3).await;
    for target in [SINGLE_B, PLAIN] {
        assert!(
            matches!(
                sim.prepare(&session, &on, target),
                Err(RouteError::CrossKeyspace)
            ),
            "cross-tenant: {target}"
        );
    }
    // The refused session is untouched.
    assert!(sim.take_redirect().is_none());
    assert_eq!(active(&sim, SHARED_AB), 1);
    assert_eq!((active(&sim, SINGLE_B), active(&sim, PLAIN)), (0, 0));
    for target in [SHARED_AC, SHARED_ANY] {
        must(sim.prepare(&session, &on, target));
    }
    // Shared {a,b} -> Single(a) -> Set{a,c}: multi <-> single in one tenant.
    migrate(&sim, &must(sim.prepare(&session, &on, SINGLE_A)), SINGLE_A);
    assert_eq!((active(&sim, SHARED_AB), active(&sim, SINGLE_A)), (0, 1));
    migrate(
        &sim,
        &must(sim.prepare(&session, &on, SHARED_AC)),
        SHARED_AC,
    );
    assert_eq!((active(&sim, SINGLE_A), active(&sim, SHARED_AC)), (0, 1));
    // The self-redirect is allowed: its owner serves `ksa`.
    assert_eq!(self_redirects(&sim), (1, 1));
    assert_eq!(active(&sim, SHARED_AC), 1);
    migrate(&sim, &must(sim.prepare(&session, &on, SINGLE_A)), SINGLE_A);

    // Gate off again: having been on a shared backend, the session is
    // ineligible again, even from its current single-keyspace backend.
    let off = tenant_migration(&h, &sim, false, 4).await;
    assert!(matches!(
        sim.prepare(&session, &off, SINGLE_A2),
        Err(RouteError::CrossKeyspace)
    ));
    assert_eq!(
        self_redirects(&sim),
        (1, 0),
        "PR 1 rule: a once-shared session is skipped by the sweep too"
    );
    sim.router().close(&session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_sessions_without_a_keyspace_keep_the_verified_tenant_rule() -> TestResult {
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS).await;
    let on = tenant_migration(&h, &sim, true, 3).await;
    // Unprefixed users: no session keyspace even in a keyspace-routing
    // namespace.
    let single = active_in(&sim, &on, SINGLE_A, None);
    let on_shared = active_in(&sim, &on, SHARED_AB, None);

    assert!(matches!(
        sim.prepare(&single, &on, SINGLE_B),
        Err(RouteError::CrossKeyspace)
    ));
    for target in [SHARED_AB, SHARED_AC, SHARED_ANY] {
        assert!(
            matches!(
                sim.prepare(&single, &on, target),
                Err(RouteError::CrossKeyspace)
            ),
            "shared target: {target}"
        );
    }
    for target in [SINGLE_A, SINGLE_B, SHARED_AC, SHARED_ANY, PLAIN] {
        assert!(
            matches!(
                sim.prepare(&on_shared, &on, target),
                Err(RouteError::CrossKeyspace)
            ),
            "shared source: {target}"
        );
    }
    // Same backend keyspace is still allowed, exactly as today.
    migrate(&sim, &must(sim.prepare(&single, &on, SINGLE_A2)), SINGLE_A2);
    // Only the single-keyspace session is reconnected by the sweep.
    assert_eq!(self_redirects(&sim), (2, 1));
    sim.router().close(&single);
    sim.router().close(&on_shared);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_change_user_to_another_prefix_clears_the_session_keyspace() -> TestResult {
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS).await;
    let on = tenant_migration(&h, &sim, true, 3).await;
    let on_shared = active_in(&sim, &on, SHARED_AB, Some("ksa"));
    let single = active_in(&sim, &on, SINGLE_A, Some("ksa"));

    // Same prefix: the keyspace is kept.
    assert!(!sim.router().change_user(&on_shared, "ksa.reporting"));
    let prepared = must(sim.prepare(&on_shared, &on, SINGLE_A2));

    // Another tenant's prefix, accepted by the shared backend: the session
    // keyspace is gone, and an offer prepared before the change is refused.
    assert!(sim.router().change_user(&on_shared, "ksb.app"));
    assert!(
        !must(sim.offer(&prepared)),
        "offer refused after change-user"
    );
    assert!(sim.take_redirect().is_none());
    assert_eq!(active(&sim, SHARED_AB), 1);
    for target in [SINGLE_A, SINGLE_B, SHARED_AC, SHARED_ANY] {
        assert!(
            matches!(
                sim.prepare(&on_shared, &on, target),
                Err(RouteError::CrossKeyspace)
            ),
            "cleared keyspace: {target}"
        );
    }
    // Changing back does not restore it (sticky, fail closed).
    assert!(!sim.router().change_user(&on_shared, "ksa.app"));
    assert!(matches!(
        sim.prepare(&on_shared, &on, SINGLE_A),
        Err(RouteError::CrossKeyspace)
    ));

    // A single-keyspace session falls back to today's rule, which is
    // still bound by its backend's keyspace.
    assert!(sim.router().change_user(&single, "app"));
    assert!(matches!(
        sim.prepare(&single, &on, SHARED_AC),
        Err(RouteError::CrossKeyspace)
    ));
    migrate(&sim, &must(sim.prepare(&single, &on, SINGLE_A2)), SINGLE_A2);

    // The sweep skips the shared session and reconnects the single one.
    assert_eq!(self_redirects(&sim), (2, 1));

    // A successful move records the target's scope: a session moved from
    // the shared backend onto Single(ksa) is then judged as admitted there,
    // so after losing its keyspace it can still move within ksa.
    let moved = active_in(&sim, &on, SHARED_AB, Some("ksa"));
    migrate(&sim, &must(sim.prepare(&moved, &on, SINGLE_A)), SINGLE_A);
    assert!(sim.router().change_user(&moved, "app"));
    must(sim.prepare(&moved, &on, SINGLE_A2));
    sim.router().close(&moved);
    sim.router().close(&on_shared);
    sim.router().close(&single);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_unverified_target_does_not_serve_the_session_keyspace() -> TestResult {
    const UNVERIFIED_A: &str = "default/127.0.0.1:4007";
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS).await;
    let on = tenant_migration(&h, &sim, true, 3).await;
    let keyed = active_in(&sim, &on, SINGLE_A, Some("ksa"));
    let unkeyed = active_in(&sim, &on, SINGLE_A, None);

    // 4007's `ksa` info is live only through another path's ttl.
    let mut raw: Vec<(String, Vec<u8>)> = Vec::new();
    for (prefix, addr, info) in k3_records() {
        raw.push((
            format!("{prefix}{addr}/info"),
            must(serde_json::to_vec(&info)),
        ));
        raw.push((format!("{prefix}{addr}/ttl"), b"1".to_vec()));
    }
    raw.push((
        format!("{KSA_PATH}127.0.0.1:4007/info"),
        must(serde_json::to_vec(&classic())),
    ));
    raw.push((format!("{KSB_PATH}127.0.0.1:4007/ttl"), b"1".to_vec()));
    h.fixture.raw_records(&raw);
    let partial = must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && candidate.routing.backends.backends.len() == K3_BACKENDS + 1
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    );
    assert!(
        partial.routing.backends.backends.iter().any(|backend| {
            backend.backend_id.as_ref() == UNVERIFIED_A
                && backend.backend.serves
                    == control_topology::KeyspaceScope::Unverified("ksa".to_owned())
        }),
        "fixture: 4007 is unverified"
    );

    // An unverified record serves no tenant, with or without a session
    // keyspace, although it projects the `ksa` path keyspace.
    for session in [&keyed, &unkeyed] {
        assert!(matches!(
            sim.prepare(session, &partial, UNVERIFIED_A),
            Err(RouteError::CrossKeyspace)
        ));
        must(sim.prepare(session, &partial, SINGLE_A2));
    }
    // A session admitted on the unverified record stays pinned.
    let on_unverified = active_in(&sim, &partial, UNVERIFIED_A, None);
    assert!(matches!(
        sim.prepare(&on_unverified, &partial, SINGLE_A2),
        Err(RouteError::CrossKeyspace)
    ));
    for session in [keyed, unkeyed, on_unverified] {
        sim.router().close(&session);
    }
    Ok(())
}

/// `k3_records()` without the record of `addr`.
fn k3_records_without(addr: &str) -> Vec<(&'static str, &'static str, serde_json::Value)> {
    k3_records()
        .into_iter()
        .filter(|(_, record_addr, _)| *record_addr != addr)
        .collect()
}

/// Publishes `records` and waits for the simulation to capture `expected`
/// backends, without any other router operation in between.
async fn republish(
    h: &Harness,
    sim: &MigrationSimulation,
    old: &Candidate,
    records: &[(&str, &str, serde_json::Value)],
    expected: usize,
) -> Candidate {
    h.fixture.info_records(records);
    must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_withdrawn_address_is_checked_against_the_current_snapshot() -> TestResult {
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS).await;
    let on = tenant_migration(&h, &sim, true, 3).await;
    let stays = active_in(&sim, &on, SINGLE_A, Some("ksa"));
    let withdrawn = active_in(&sim, &on, SINGLE_A2, Some("ksa"));
    assert_eq!(self_redirects(&sim), (2, 2), "fixture: both reconnect");

    // 4006 leaves discovery; the router keeps its record for the session it
    // still owns. The reconnect sweep is the first operation afterwards.
    let current = republish(
        &h,
        &sim,
        &on,
        &k3_records_without("127.0.0.1:4006"),
        K3_BACKENDS - 1,
    )
    .await;
    assert_eq!(
        self_redirects(&sim),
        (2, 1),
        "no reconnect to a withdrawn address"
    );
    assert!(sim.prepare(&stays, &current, SINGLE_A2).is_err());
    assert!(matches!(
        sim.prepare(&withdrawn, &current, SINGLE_A),
        Err(RouteError::CrossKeyspace)
    ));
    sim.router().close(&stays);
    sim.router().close(&withdrawn);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_address_reused_by_another_tenant_refuses_its_old_sessions() -> TestResult {
    let (h, _) = harness_with(&k3_records(), K3_BACKENDS, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS).await;
    let on = tenant_migration(&h, &sim, true, 3).await;
    let keyed = active_in(&sim, &on, SINGLE_A, Some("ksa"));
    let unkeyed = active_in(&sim, &on, SINGLE_A, None);

    // 4003 now registers for ksb; the sweep is the first operation after
    // that publication.
    let mut records = k3_records_without("127.0.0.1:4003");
    records.push((KSB_PATH, "127.0.0.1:4003", classic()));
    let flipped = republish(&h, &sim, &on, &records, K3_BACKENDS).await;
    assert!(
        flipped.routing.backends.backends.iter().any(|backend| {
            backend.backend_id.as_ref() == SINGLE_A
                && backend.backend.serves == control_topology::KeyspaceScope::Single("ksb".into())
        }),
        "fixture: 4003 is ksb's now"
    );
    assert_eq!(self_redirects(&sim), (2, 0));
    for session in [&keyed, &unkeyed] {
        assert!(matches!(
            sim.prepare(session, &flipped, SINGLE_A2),
            Err(RouteError::CrossKeyspace)
        ));
    }
    sim.router().close(&keyed);
    sim.router().close(&unkeyed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k3_keyspace_label_never_qualifies_a_target() -> TestResult {
    const LABELLED: &str = "default/127.0.0.1:4008";
    let mut records = k3_records();
    records.push((
        PROCESS,
        "127.0.0.1:4008",
        serde_json::json!({"ip":"127.0.0.1","labels":{"keyspace":"ksa"}}),
    ));
    let (h, _) = harness_with(&records, K3_BACKENDS + 1, true).await?;
    let sim = simulation(&h);
    sim_ready(&sim, K3_BACKENDS + 1).await;
    h.patch("[balance]\ntenant-migration = true\n", 3);
    h.source.deliver();
    h.applied().await;
    let on = must(
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(candidate) = sim.router().capture()
                    && candidate.policy.tenant_migration
                    && candidate.routing.backends.backends.len() == K3_BACKENDS + 1
                {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        })
        .await,
    );
    let labelled = on
        .routing
        .backends
        .backends
        .iter()
        .find(|backend| backend.backend_id.as_ref() == LABELLED)
        .ok_or("labelled record")?;
    assert_eq!(
        crate::selector::assignment_keyspace_for_test(labelled),
        "ksa",
        "fixture: the label projects the assignment keyspace"
    );
    let keyed = active_in(&sim, &on, SINGLE_A, Some("ksa"));
    let unkeyed = active_in(&sim, &on, SINGLE_A, None);
    for session in [&keyed, &unkeyed] {
        assert!(matches!(
            sim.prepare(session, &on, LABELLED),
            Err(RouteError::CrossKeyspace)
        ));
        must(sim.prepare(session, &on, SINGLE_A2));
    }
    sim.router().close(&keyed);
    sim.router().close(&unkeyed);
    Ok(())
}
