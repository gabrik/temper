//! nerdsane/temper#519 V6: an `Idempotency-Key` must be bound to the action and
//! canonical request body it was first used with.
//!
//! Every scenario runs on the warm path (same process, HTTP + actor cache) and
//! on the cold path (fresh server over the same journal: durable replay path).
//! Expected contract (ADR-0182):
//! - same key + same logical request -> 200 with the ORIGINAL logical response,
//!   no new journal event;
//! - same key + different action/body -> 422 `IdempotencyKeyMismatch`, no event;
//! - key known but its binding cannot be verified -> 409
//!   `IdempotencyKeyUnverifiable`, no event.

use axum::http::StatusCode;
use serde_json::Value;
use temper_runtime::scheduler::install_deterministic_context;

use super::harness::{
    Harness, INTRUDER, Mode, Reply, TENANT_A, TENANT_B, action_request, items, logical, status,
};
use super::legacy::{legacy_copy, save_legacy_snapshot};

const MARK: &str = "FACTORY_REGRESSION_ASSERTION";

const ORDER: &str = "order-v6";
const K1: &str = "idem-k1";
const K2: &str = "idem-k2";

/// First request bound to K1.
const ORIGINAL: &str =
    r#"{"ProductId":"p-1","Quantity":1,"Meta":{"gift":{"wrap":true,"note":"n"},"tags":["a","b"]}}"#;
/// Same logical request: nested keys reordered and a top-level server-derived
/// `Id` that dispatch strips. Array order is unchanged.
const EQUIVALENT: &str = r#"{"Meta":{"tags":["a","b"],"gift":{"note":"n","wrap":true}},"Quantity":1,"ProductId":"p-1","Id":"order-v6"}"#;
/// Intervening successful action bound to K2.
const SECOND: &str = r#"{"ProductId":"p-2","Quantity":1}"#;

/// Transport-only metadata that must not influence the binding.
const TRANSPORT_HEADERS: &[(&str, &str)] = &[
    (
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    ),
    ("x-session-id", "retry-session"),
    ("x-intent", "retry after timeout"),
    ("x-temper-observe-meta-producer.attempt", "2"),
];

/// Boot, create the order, run K1 (original) and K2 (intervening). Returns the
/// harness, the original K1 response and the journal length after K2.
async fn setup(seed: u64) -> (Harness, Reply, usize) {
    let h = Harness::new(seed);
    h.create_order(TENANT_A, ORDER).await;
    let original = h
        .action(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            ORIGINAL,
        )
        .await;
    assert_eq!(
        original.status,
        StatusCode::OK,
        "harness setup: original K1 AddItem failed: {}",
        original.body
    );
    let second = h
        .action(TENANT_A, ORDER, "Temper.Example.AddItem", Some(K2), SECOND)
        .await;
    assert_eq!(
        second.status,
        StatusCode::OK,
        "harness setup: intervening K2 AddItem failed: {}",
        second.body
    );
    let len = h.journal_len(TENANT_A, ORDER);
    assert!(
        len >= 3,
        "harness setup: expected Created + 2 AddItem events, journal has {len}"
    );
    (h, original, len)
}

/// Assert a mismatch/unverifiable rejection that appended nothing.
fn assert_rejected(
    reply: &Reply,
    expected_status: StatusCode,
    expected_code: &str,
    journal_before: usize,
    journal_after: usize,
    scenario: &str,
) {
    assert!(
        reply.status == expected_status && reply.error_code() == Some(expected_code),
        "{MARK}: {scenario}: expected {expected_status} {expected_code}, got {} with body {}",
        reply.status,
        reply.body
    );
    assert_eq!(
        journal_after,
        journal_before,
        "{MARK}: {scenario}: rejected request appended {} journal event(s)",
        journal_after.saturating_sub(journal_before)
    );
}

// ---------------------------------------------------------------------------
// Same key, same logical request -> original response, no new event.
// ---------------------------------------------------------------------------

async fn same_logical_request_returns_original(mode: Mode, seed: u64) {
    let (_g, _c, _i) = install_deterministic_context(seed);
    let (mut h, original, before) = setup(seed).await;
    h.maybe_restart(mode);

    // Different qualifier, reordered nested keys, top-level Id, transport headers.
    let retry = h
        .send(action_request(
            TENANT_A,
            ORDER,
            "Temper.AddItem",
            Some(K1),
            EQUIVALENT,
            TRANSPORT_HEADERS,
            Some(super::harness::TESTER),
        ))
        .await;
    assert!(
        retry.status == StatusCode::OK && logical(&retry.body) == logical(&original.body),
        "{MARK}: {mode:?}: same-key same-logical retry after an intervening action must return \
         the ORIGINAL response.\n status: {}\n original: {}\n got: {}",
        retry.status,
        logical(&original.body),
        logical(&retry.body)
    );
    assert_eq!(
        h.journal_len(TENANT_A, ORDER),
        before,
        "{MARK}: {mode:?}: idempotent retry appended a journal event"
    );
}

#[tokio::test]
async fn same_logical_request_returns_original_response_after_intervening_action_warm() {
    same_logical_request_returns_original(Mode::WarmHttpCache, 5191).await;
}

#[tokio::test]
async fn same_logical_request_returns_original_response_after_intervening_action_cold() {
    same_logical_request_returns_original(Mode::ColdRestart, 5192).await;
}

// ---------------------------------------------------------------------------
// Same key, different action or body -> 422, no event.
// ---------------------------------------------------------------------------

async fn mismatch_is_rejected(
    mode: Mode,
    seed: u64,
    qualified_action: &str,
    body: &str,
    scenario: &str,
) {
    let (_g, _c, _i) = install_deterministic_context(seed);
    let (mut h, _original, before) = setup(seed).await;
    let status_before = h.get_order(TENANT_A, ORDER).await;
    h.maybe_restart(mode);

    let reply = h
        .action(TENANT_A, ORDER, qualified_action, Some(K1), body)
        .await;
    let after = h.journal_len(TENANT_A, ORDER);
    assert_rejected(
        &reply,
        StatusCode::UNPROCESSABLE_ENTITY,
        "IdempotencyKeyMismatch",
        before,
        after,
        &format!("{mode:?}: {scenario}"),
    );
    let get = h.get_order(TENANT_A, ORDER).await;
    assert_eq!(
        status(&get.body),
        status(&status_before.body),
        "{MARK}: {mode:?}: {scenario}: mismatched request changed entity status"
    );
}

const CANCEL: &str = r#"{"Reason":"changed my mind"}"#;
const OTHER_QUANTITY: &str =
    r#"{"ProductId":"p-1","Quantity":2,"Meta":{"gift":{"wrap":true,"note":"n"},"tags":["a","b"]}}"#;
const REORDERED_ARRAY: &str =
    r#"{"ProductId":"p-1","Quantity":1,"Meta":{"gift":{"wrap":true,"note":"n"},"tags":["b","a"]}}"#;
const NESTED_ODATA_PROPERTY: &str = r##"{"ProductId":"p-1","Quantity":1,"Meta":{"gift":{"wrap":true,"note":"n","@odata.type":"#Gift"},"tags":["a","b"]}}"##;

#[tokio::test]
async fn different_action_same_key_is_rejected_warm() {
    mismatch_is_rejected(
        Mode::WarmHttpCache,
        5201,
        "Temper.Example.CancelOrder",
        CANCEL,
        "different action",
    )
    .await;
}

#[tokio::test]
async fn different_action_same_key_is_rejected_cold() {
    mismatch_is_rejected(
        Mode::ColdRestart,
        5202,
        "Temper.Example.CancelOrder",
        CANCEL,
        "different action",
    )
    .await;
}

#[tokio::test]
async fn different_body_same_key_is_rejected_warm() {
    mismatch_is_rejected(
        Mode::WarmHttpCache,
        5211,
        "Temper.Example.AddItem",
        OTHER_QUANTITY,
        "different body",
    )
    .await;
}

#[tokio::test]
async fn different_body_same_key_is_rejected_cold() {
    mismatch_is_rejected(
        Mode::ColdRestart,
        5212,
        "Temper.Example.AddItem",
        OTHER_QUANTITY,
        "different body",
    )
    .await;
}

#[tokio::test]
async fn nested_array_order_is_significant_warm() {
    mismatch_is_rejected(
        Mode::WarmHttpCache,
        5221,
        "Temper.Example.AddItem",
        REORDERED_ARRAY,
        "nested array reordered",
    )
    .await;
}

#[tokio::test]
async fn nested_array_order_is_significant_cold() {
    mismatch_is_rejected(
        Mode::ColdRestart,
        5222,
        "Temper.Example.AddItem",
        REORDERED_ARRAY,
        "nested array reordered",
    )
    .await;
}

#[tokio::test]
async fn nested_odata_named_business_property_is_bound_warm() {
    mismatch_is_rejected(
        Mode::WarmHttpCache,
        5231,
        "Temper.Example.AddItem",
        NESTED_ODATA_PROPERTY,
        "nested @odata.* business property added",
    )
    .await;
}

#[tokio::test]
async fn nested_odata_named_business_property_is_bound_cold() {
    mismatch_is_rejected(
        Mode::ColdRestart,
        5232,
        "Temper.Example.AddItem",
        NESTED_ODATA_PROPERTY,
        "nested @odata.* business property added",
    )
    .await;
}

// ---------------------------------------------------------------------------
// Failed requests never bind a key (guard).
// ---------------------------------------------------------------------------

async fn failed_request_never_binds_key(mode: Mode, seed: u64) {
    let (_g, _c, _i) = install_deterministic_context(seed);
    let mut h = Harness::new(seed);
    let id = "order-failed-bind";
    let key = "idem-failed";
    h.create_order(TENANT_A, id).await;
    let empty = h.journal_len(TENANT_A, id);

    // Guard `items > 0` fails on an empty order.
    let failed = h
        .action(
            TENANT_A,
            id,
            "Temper.Example.RemoveItem",
            Some(key),
            r#"{"ItemId":"i-1"}"#,
        )
        .await;
    assert_eq!(
        failed.status,
        StatusCode::CONFLICT,
        "failed RemoveItem should be 409: {}",
        failed.body
    );
    assert_eq!(
        h.journal_len(TENANT_A, id),
        empty,
        "failed request appended an event"
    );
    h.maybe_restart(mode);

    let applied = h
        .action(TENANT_A, id, "Temper.Example.AddItem", Some(key), SECOND)
        .await;
    assert_eq!(
        applied.status,
        StatusCode::OK,
        "{mode:?}: a key whose only use failed must stay free: {}",
        applied.body
    );
    assert_eq!(
        h.journal_len(TENANT_A, id),
        empty + 1,
        "{mode:?}: AddItem must append one event"
    );
    h.maybe_restart(mode);

    let retry = h
        .action(TENANT_A, id, "Temper.Example.AddItem", Some(key), SECOND)
        .await;
    assert_eq!(
        retry.status,
        StatusCode::OK,
        "{mode:?}: retry should succeed: {}",
        retry.body
    );
    assert_eq!(
        logical(&retry.body),
        logical(&applied.body),
        "{mode:?}: retry should return the original response"
    );
    assert_eq!(
        h.journal_len(TENANT_A, id),
        empty + 1,
        "{mode:?}: retry appended an event"
    );
}

#[tokio::test]
async fn failed_request_never_binds_key_warm() {
    failed_request_never_binds_key(Mode::WarmHttpCache, 5241).await;
}

#[tokio::test]
async fn failed_request_never_binds_key_cold() {
    failed_request_never_binds_key(Mode::ColdRestart, 5242).await;
}

// ---------------------------------------------------------------------------
// Concurrency (warm).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_identical_requests_append_one_event() {
    let (_g, _c, _i) = install_deterministic_context(5251);
    let h = Harness::new(5251);
    let id = "order-concurrent-same";
    h.create_order(TENANT_A, id).await;
    let before = h.journal_len(TENANT_A, id);

    let (a, b) = tokio::join!(
        h.action(TENANT_A, id, "Temper.Example.AddItem", Some(K1), ORIGINAL),
        h.send(action_request(
            TENANT_A,
            id,
            "Temper.AddItem",
            Some(K1),
            r#"{"Meta":{"tags":["a","b"],"gift":{"note":"n","wrap":true}},"Quantity":1,"ProductId":"p-1"}"#,
            TRANSPORT_HEADERS,
            Some(super::harness::TESTER),
        )),
    );
    assert_eq!(
        a.status,
        StatusCode::OK,
        "first concurrent caller: {}",
        a.body
    );
    assert_eq!(
        b.status,
        StatusCode::OK,
        "second concurrent caller: {}",
        b.body
    );
    assert_eq!(
        logical(&a.body),
        logical(&b.body),
        "concurrent identical callers saw different responses"
    );
    assert_eq!(
        h.journal_len(TENANT_A, id),
        before + 1,
        "concurrent identical callers must append exactly one event"
    );
}

#[tokio::test]
async fn concurrent_different_bodies_one_wins() {
    let (_g, _c, _i) = install_deterministic_context(5261);
    let h = Harness::new(5261);
    let id = "order-concurrent-diff";
    h.create_order(TENANT_A, id).await;
    let before = h.journal_len(TENANT_A, id);

    let (a, b) = tokio::join!(
        h.action(TENANT_A, id, "Temper.Example.AddItem", Some(K1), ORIGINAL),
        h.action(
            TENANT_A,
            id,
            "Temper.Example.AddItem",
            Some(K1),
            OTHER_QUANTITY
        ),
    );
    let ok = [&a, &b]
        .iter()
        .filter(|r| r.status == StatusCode::OK)
        .count();
    let mismatched = [&a, &b]
        .iter()
        .filter(|r| {
            r.status == StatusCode::UNPROCESSABLE_ENTITY
                && r.error_code() == Some("IdempotencyKeyMismatch")
        })
        .count();
    assert!(
        ok == 1 && mismatched == 1,
        "{MARK}: concurrent callers with one key and different bodies: expected one 200 and one \
         422 IdempotencyKeyMismatch, got {} {} and {} {}",
        a.status,
        a.body,
        b.status,
        b.body
    );
    assert_eq!(
        h.journal_len(TENANT_A, id),
        before + 1,
        "{MARK}: concurrent different-body callers must append exactly one event"
    );
}

// ---------------------------------------------------------------------------
// Legacy (pre-binding) journal and snapshot.
// ---------------------------------------------------------------------------

/// Run K1 + K2 in a current-format store, then rebuild a legacy store (no
/// binding field, old-format snapshot at the head) and boot over it cold.
async fn legacy_setup(seed: u64, extra_processed_keys: &[&str]) -> (Harness, Reply, usize) {
    let h = Harness::new(seed);
    h.create_order(TENANT_A, ORDER).await;
    let original = h
        .action(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            ORIGINAL,
        )
        .await;
    assert_eq!(
        original.status,
        StatusCode::OK,
        "harness setup: original K1 failed: {}",
        original.body
    );
    let second = h
        .action(TENANT_A, ORDER, "Temper.Example.AddItem", Some(K2), SECOND)
        .await;
    assert_eq!(
        second.status,
        StatusCode::OK,
        "harness setup: K2 failed: {}",
        second.body
    );

    let legacy = legacy_copy(&h.store, TENANT_A, ORDER, seed + 1).await;
    save_legacy_snapshot(&legacy, TENANT_A, ORDER, &second.body, extra_processed_keys).await;
    let cold = Harness::over(legacy);
    let len = cold.journal_len(TENANT_A, ORDER);
    (cold, original, len)
}

#[tokio::test]
async fn legacy_journal_and_snapshot_same_request_returns_original() {
    let (_g, _c, _i) = install_deterministic_context(5271);
    let (h, original, before) = legacy_setup(5271, &[]).await;
    let retry = h
        .action(TENANT_A, ORDER, "Temper.AddItem", Some(K1), EQUIVALENT)
        .await;
    assert!(
        retry.status == StatusCode::OK && logical(&retry.body) == logical(&original.body),
        "{MARK}: legacy journal+snapshot: same-logical retry must return the ORIGINAL response.\n \
         status: {}\n original: {}\n got: {}",
        retry.status,
        logical(&original.body),
        logical(&retry.body)
    );
    assert_eq!(
        h.journal_len(TENANT_A, ORDER),
        before,
        "{MARK}: legacy journal+snapshot: idempotent retry appended an event"
    );
}

#[tokio::test]
async fn legacy_journal_different_body_is_rejected() {
    let (_g, _c, _i) = install_deterministic_context(5281);
    let (h, _original, before) = legacy_setup(5281, &[]).await;
    let reply = h
        .action(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            OTHER_QUANTITY,
        )
        .await;
    let after = h.journal_len(TENANT_A, ORDER);
    assert_rejected(
        &reply,
        StatusCode::UNPROCESSABLE_ENTITY,
        "IdempotencyKeyMismatch",
        before,
        after,
        "legacy journal: different body",
    );
}

#[tokio::test]
async fn legacy_snapshot_key_without_event_fails_closed() {
    let (_g, _c, _i) = install_deterministic_context(5291);
    let ghost = "idem-ghost";
    let (h, _original, before) = legacy_setup(5291, &[ghost]).await;
    let reply = h
        .action(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(ghost),
            SECOND,
        )
        .await;
    let after = h.journal_len(TENANT_A, ORDER);
    assert_rejected(
        &reply,
        StatusCode::CONFLICT,
        "IdempotencyKeyUnverifiable",
        before,
        after,
        "legacy snapshot key with no journal event",
    );
}

// ---------------------------------------------------------------------------
// Tenant isolation and fail-closed auth (guard).
// ---------------------------------------------------------------------------

async fn binding_respects_tenant_and_auth(mode: Mode, seed: u64) {
    let (_g, _c, _i) = install_deterministic_context(seed);
    let (mut h, _original, before) = setup(seed).await;
    h.create_order(TENANT_B, ORDER).await;
    let b_before = h.journal_len(TENANT_B, ORDER);
    h.maybe_restart(mode);

    // Same id + key in another tenant executes independently.
    let b = h
        .action(
            TENANT_B,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            OTHER_QUANTITY,
        )
        .await;
    assert_eq!(
        b.status,
        StatusCode::OK,
        "{mode:?}: tenant B must execute independently: {}",
        b.body
    );
    assert_eq!(
        h.journal_len(TENANT_B, ORDER),
        b_before + 1,
        "{mode:?}: tenant B event missing"
    );
    assert_eq!(
        h.journal_len(TENANT_A, ORDER),
        before,
        "{mode:?}: tenant B wrote tenant A's journal"
    );

    // Denied principal never sees a cached hit.
    let denied = h
        .send(action_request(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            ORIGINAL,
            &[],
            Some(INTRUDER),
        ))
        .await;
    assert_eq!(
        denied.status,
        StatusCode::FORBIDDEN,
        "{mode:?}: denied principal: {}",
        denied.body
    );
    assert!(
        status(&denied.body).is_none(),
        "{mode:?}: denied response leaked entity state: {}",
        denied.body
    );

    // Unauthenticated request is rejected before any cache lookup.
    let anonymous = h
        .send(action_request(
            TENANT_A,
            ORDER,
            "Temper.Example.AddItem",
            Some(K1),
            ORIGINAL,
            &[],
            None,
        ))
        .await;
    assert_eq!(
        anonymous.status,
        StatusCode::UNAUTHORIZED,
        "{mode:?}: unauthenticated: {}",
        anonymous.body
    );
    assert!(
        status(&anonymous.body).is_none(),
        "{mode:?}: unauthenticated response leaked entity state"
    );
    assert_eq!(
        h.journal_len(TENANT_A, ORDER),
        before,
        "{mode:?}: rejected requests appended events"
    );
}

#[tokio::test]
async fn binding_respects_tenant_and_auth_warm() {
    binding_respects_tenant_and_auth(Mode::WarmHttpCache, 5301).await;
}

#[tokio::test]
async fn binding_respects_tenant_and_auth_cold() {
    binding_respects_tenant_and_auth(Mode::ColdRestart, 5302).await;
}

// ---------------------------------------------------------------------------
// Offline replay fold vs response history, GET and catalog (guard).
// ---------------------------------------------------------------------------

/// Fold `(status, items)` from journal payloads, optionally stopping after the
/// first event carrying `stop_key`.
fn fold(
    journal: &[temper_runtime::persistence::PersistenceEnvelope],
    stop_key: Option<&str>,
) -> (String, u64) {
    let mut status = String::new();
    let mut items: u64 = 0;
    for env in journal {
        let p = &env.payload;
        if let Some(to) = p.get("to_status").and_then(Value::as_str) {
            status = to.to_string();
        }
        match p.get("action").and_then(Value::as_str) {
            Some("AddItem") => items += 1,
            Some("RemoveItem") => items = items.saturating_sub(1),
            _ => {}
        }
        if stop_key.is_some() && p.get("idempotency_key").and_then(Value::as_str) == stop_key {
            break;
        }
    }
    (status, items)
}

#[tokio::test]
async fn offline_fold_matches_history_catalog_and_get() {
    let (_g, _c, _i) = install_deterministic_context(5311);
    let (h, original, _before) = setup(5311).await;
    let journal = h.journal(TENANT_A, ORDER);

    let (full_status, full_items) = fold(&journal, None);
    let get = h.get_order(TENANT_A, ORDER).await;
    assert_eq!(get.status, StatusCode::OK, "GET order: {}", get.body);
    assert_eq!(
        status(&get.body),
        Some(full_status.as_str()),
        "fold status != GET: {}",
        get.body
    );
    assert_eq!(
        items(&get.body),
        Some(full_items),
        "fold items != GET: {}",
        get.body
    );

    let catalog = h.list_orders(TENANT_A).await;
    assert_eq!(catalog.status, StatusCode::OK, "catalog: {}", catalog.body);
    let row = catalog
        .body
        .get("value")
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter().find(|r| {
                r.get("entity_id").and_then(Value::as_str) == Some(ORDER)
                    || r.get("Id").and_then(Value::as_str) == Some(ORDER)
            })
        })
        .unwrap_or_else(|| panic!("order missing from catalog: {}", catalog.body));
    assert_eq!(
        status(row),
        Some(full_status.as_str()),
        "fold status != catalog row: {row}"
    );

    let (k1_status, k1_items) = fold(&journal, Some(K1));
    assert_eq!(
        status(&original.body),
        Some(k1_status.as_str()),
        "prefix fold status != original K1 response"
    );
    assert_eq!(
        items(&original.body),
        Some(k1_items),
        "prefix fold items != original K1 response"
    );
}
