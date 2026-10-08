use super::*;
use crate::entity_actor::{EntityResponse, EntityState};
use std::collections::BTreeMap;

fn make_response(status: &str) -> EntityResponse {
    EntityResponse {
        success: true,
        state: EntityState {
            entity_type: String::new(),
            entity_id: String::new(),
            status: status.to_string(),
            item_count: 0,
            counters: BTreeMap::new(),
            booleans: BTreeMap::new(),
            lists: BTreeMap::new(),
            fields: serde_json::json!({}),
            events: std::collections::VecDeque::new(),
            total_event_count: 0,
            events_since_snapshot: 0,
            last_snapshot_sequence_nr: 0,
            sequence_nr: 0,
            processed_idempotency_keys: BTreeMap::new(),
        },
        error: None,
        custom_effects: vec![],
        scheduled_actions: vec![],
        spawn_requests: vec![],
        spec_governed: true,
    }
}

fn hit_status(lookup: IdempotencyLookup) -> Option<String> {
    match lookup {
        IdempotencyLookup::Hit(response) => Some(response.state.status),
        _ => None,
    }
}

const B: &str = "binding-a";

#[test]
fn put_then_lookup_returns_cached() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Active"));
    assert_eq!(
        hit_status(cache.lookup("Order:o1", "key-1", B)).as_deref(),
        Some("Active")
    );
}

#[test]
fn different_binding_is_mismatch_not_hit() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Active"));
    assert!(matches!(
        cache.lookup("Order:o1", "key-1", "binding-b"),
        IdempotencyLookup::Mismatch
    ));
    // Mismatch is reported even before effects are marked applied.
    assert!(matches!(
        cache.lookup_after_effects_applied("Order:o1", "key-1", "binding-b"),
        IdempotencyLookup::Mismatch
    ));
}

#[test]
fn pending_effects_do_not_satisfy_protocol_cache_hit() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Active"));

    assert!(hit_status(cache.lookup("Order:o1", "key-1", B)).is_some());
    assert!(matches!(
        cache.lookup_after_effects_applied("Order:o1", "key-1", B),
        IdempotencyLookup::Miss
    ));

    owner(&cache).complete(&make_response("Active"));
    assert!(hit_status(cache.lookup_after_effects_applied("Order:o1", "key-1", B)).is_some());
}

#[test]
fn lookup_missing_is_miss() {
    let cache = IdempotencyCache::new();
    assert!(matches!(
        cache.lookup("Order:o1", "no-such-key", B),
        IdempotencyLookup::Miss
    ));
}

#[test]
fn different_actors_isolated() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("A"));
    cache.put("Order:o2", "key-1", "binding-b", make_response("B"));
    assert_eq!(
        hit_status(cache.lookup("Order:o1", "key-1", B)).as_deref(),
        Some("A")
    );
    assert_eq!(
        hit_status(cache.lookup("Order:o2", "key-1", "binding-b")).as_deref(),
        Some("B")
    );
}

#[test]
fn budget_evicts_oldest() {
    let cache = IdempotencyCache::new();
    for i in 0..IDEMPOTENCY_BUDGET_PER_ACTOR {
        cache.put("actor", &format!("k-{i}"), B, make_response("S"));
    }
    cache.put("actor", "k-overflow", B, make_response("New"));
    let entries = cache.entries.read().unwrap();
    let actor_entries = entries.get("actor").unwrap();
    assert_eq!(actor_entries.len(), IDEMPOTENCY_BUDGET_PER_ACTOR);
    assert!(actor_entries.contains_key("k-overflow"));
}

#[test]
fn binding_ignores_qualifier_nested_key_order_and_top_level_server_fields() {
    let a = serde_json::json!({"Q": 1, "Meta": {"x": 1, "y": [1, 2]}});
    let b = serde_json::json!({"Id": "o1", "Meta": {"y": [1, 2], "x": 1}, "Q": 1});
    assert_eq!(
        request_binding("Temper.Example.AddItem", &a),
        request_binding("AddItem", &b)
    );
}

#[test]
fn binding_is_sensitive_to_action_arrays_values_and_nested_odata_keys() {
    let base = serde_json::json!({"Q": 1, "Meta": {"x": 1, "y": [1, 2]}});
    let reference = request_binding("AddItem", &base);
    assert_ne!(reference, request_binding("CancelOrder", &base));
    for other in [
        serde_json::json!({"Q": 2, "Meta": {"x": 1, "y": [1, 2]}}),
        serde_json::json!({"Q": 1, "Meta": {"x": 1, "y": [2, 1]}}),
        serde_json::json!({"Q": 1, "Meta": {"x": 1, "y": [1, 2], "@odata.type": "#T"}}),
        serde_json::json!({"Q": 1, "Meta": {"x": 1, "y": [1, 2], "Id": "nested"}}),
        serde_json::json!({"Q": "1", "Meta": {"x": 1, "y": [1, 2]}}),
    ] {
        assert_ne!(reference, request_binding("AddItem", &other), "{other}");
    }
}

fn owner(cache: &IdempotencyCache) -> EffectsOwner<'_> {
    match cache.claim_post_dispatch_effects("Order:o1", "key-1") {
        EffectsClaim::Owner(owner) => owner,
        _ => panic!("expected exclusive effects ownership"),
    }
}

fn waiter(cache: &IdempotencyCache) -> EffectsWaiter {
    match cache.claim_post_dispatch_effects("Order:o1", "key-1") {
        EffectsClaim::Wait(waiter) => waiter,
        _ => panic!("expected a subscription, not a successful replay"),
    }
}

#[tokio::test]
async fn exactly_one_dispatcher_owns_fresh_commit_effects_and_publishes_final_response() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Running"));
    let first = owner(&cache);
    let waiting = waiter(&cache);
    assert!(matches!(
        cache.lookup_after_effects_applied("Order:o1", "key-1", B),
        IdempotencyLookup::Miss
    ));
    first.complete(&make_response("Done"));
    assert_eq!(waiting.wait().await.unwrap().state.status, "Done");
    assert_eq!(
        hit_status(cache.lookup_after_effects_applied("Order:o1", "key-1", B)).as_deref(),
        Some("Done")
    );
    let EffectsClaim::Replay(response) = cache.claim_post_dispatch_effects("Order:o1", "key-1")
    else {
        panic!("completed effects must replay")
    };
    assert_eq!(
        response.state.status, "Done",
        "a dispatcher with an older actor reply still receives the final result"
    );
}

#[tokio::test]
async fn failed_attempt_releases_pending_but_existing_waiters_keep_its_failure() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Running"));
    let first = owner(&cache);
    let waiting = waiter(&cache);
    let mut failed = make_response("Running");
    failed.success = false;
    failed.error = Some("injected effects failure".into());
    first.complete(&failed);
    assert_eq!(
        hit_status(cache.lookup("Order:o1", "key-1", B)).as_deref(),
        Some("Running")
    );
    owner(&cache).complete(&make_response("Done"));
    let response = waiting.wait().await.unwrap();
    assert!(!response.success);
    assert_eq!(response.error, failed.error);
}

#[tokio::test]
async fn cancellation_releases_owner_without_a_lost_wakeup() {
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Running"));
    let first = owner(&cache);
    let waiting = waiter(&cache);
    drop(first);
    // Cancellation precedes the first poll of the subscription.
    assert!(waiting.wait().await.is_none());
    owner(&cache).complete(&make_response("Done"));
}

#[tokio::test]
async fn active_effects_survive_ttl_eviction_and_replacement() {
    let (_guard, clock, _) = temper_runtime::scheduler::install_deterministic_context(51930);
    let cache = IdempotencyCache::new();
    cache.put("Order:o1", "key-1", B, make_response("Running"));
    let first = owner(&cache);
    let waiting = waiter(&cache);
    clock.advance_by((IDEMPOTENCY_TTL_SECS as u64 + 1) * 10);
    for i in 0..=IDEMPOTENCY_BUDGET_PER_ACTOR {
        cache.put("Order:o1", &format!("other-{i}"), B, make_response("Other"));
    }
    cache.put_historical("Order:o1", "key-1", "different", make_response("Old"));
    assert!(matches!(
        cache.lookup("Order:o1", "key-1", "different"),
        IdempotencyLookup::Mismatch
    ));
    assert!(matches!(
        cache.lookup("Order:o1", "key-1", B),
        IdempotencyLookup::Hit(_)
    ));
    assert!(matches!(
        cache.claim_post_dispatch_effects("Order:o1", "key-1"),
        EffectsClaim::Wait(_)
    ));
    first.complete(&make_response("Done"));
    assert_eq!(waiting.wait().await.unwrap().state.status, "Done");
    assert_eq!(
        hit_status(cache.lookup_after_effects_applied("Order:o1", "key-1", B)).as_deref(),
        Some("Done")
    );
}

#[test]
fn historical_replays_never_own_transition_effects() {
    let cache = IdempotencyCache::new();
    cache.put_historical("Order:o1", "key-1", B, make_response("Active"));
    assert!(matches!(
        cache.claim_post_dispatch_effects("Order:o1", "key-1"),
        EffectsClaim::HistoricalReplay
    ));
    assert!(matches!(
        cache.lookup_after_effects_applied("Order:o1", "key-1", B),
        IdempotencyLookup::Miss
    ));
    assert!(matches!(
        cache.claim_post_dispatch_effects("Order:o2", "absent"),
        EffectsClaim::Uncached
    ));
}
