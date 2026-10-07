//! Idempotency cache for deduplicating agent retries.
//!
//! Per-entity-actor LRU cache of recent `Idempotency-Key` → `EntityResponse`.
//! Entries expire after `IDEMPOTENCY_TTL_SECS` and are evicted when the
//! per-actor budget is exceeded.
//!
//! Every entry is bound to the canonical request that produced it
//! ([`request_binding`], ADR-0182). Reusing a key for a different action or
//! body is a [`IdempotencyLookup::Mismatch`], never a cached success.

use std::collections::BTreeMap;
use std::sync::RwLock;

use sha2::{Digest, Sha256};
use temper_runtime::scheduler::sim_now;

use crate::entity_actor::EntityResponse;

/// Actor reply error for a key reused with a different action or body.
/// Dispatch maps it to `DispatchError::IdempotencyKeyMismatch` (HTTP 422).
pub const IDEMPOTENCY_KEY_MISMATCH: &str = "IdempotencyKeyMismatch: idempotency key was already used for a different action or request body";

/// Actor reply error for a processed key whose request binding cannot be
/// verified from the journal. Dispatch maps it to
/// `DispatchError::IdempotencyKeyUnverifiable` (HTTP 409).
pub const IDEMPOTENCY_KEY_UNVERIFIABLE: &str = "IdempotencyKeyUnverifiable: idempotency key was already used but its original request cannot be verified";

const REQUEST_BINDING_TAG: &[u8] = b"temper.idempotency.v1";

/// Canonical binding of an action request (ADR-0182).
///
/// SHA-256 over the unqualified action name and the canonical JSON of the
/// params. Only top-level server-derived keys (the ones dispatch strips before
/// journaling, see `sanitize_action_params`) are removed; nested objects are
/// hashed with sorted keys, arrays keep their order, and nested keys of any name
/// are part of the binding. Transport metadata is never an input.
pub fn request_binding(action: &str, params: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    update_bytes(&mut hasher, REQUEST_BINDING_TAG);
    update_bytes(&mut hasher, unqualified_action(action).as_bytes());
    update_json(&mut hasher, params, true);
    hex_digest(hasher)
}

const RESULT_DIGEST_TAG: &[u8] = b"temper.idempotency.result.v1";

/// Immutable execution provenance of a keyed commit (ADR-0182, review
/// correction 2): SHA-256 over the post-commit logical state — `status`,
/// `item_count`, `counters`, `booleans`, `lists` and `fields`. Bookkeeping
/// (event history, sequence numbers, snapshot counters, processed keys) is
/// excluded, so a live commit and a faithful replay of it hash equal, while a
/// replay under changed transition rules does not.
pub fn result_digest(state: &crate::entity_actor::EntityState) -> String {
    let logical = serde_json::json!({
        "status": state.status,
        "item_count": state.item_count,
        "counters": state.counters,
        "booleans": state.booleans,
        "lists": state.lists,
        "fields": state.fields,
    });
    let mut hasher = Sha256::new();
    update_bytes(&mut hasher, RESULT_DIGEST_TAG);
    update_json(&mut hasher, &logical, false);
    hex_digest(hasher)
}

/// Stamp an event committed under idempotency `key` with its request binding
/// and immutable result provenance (ADR-0182). `state` is the post-commit state.
pub fn stamp_keyed_commit(
    event: &mut crate::entity_actor::EntityEvent,
    key: &str,
    action: &str,
    params: &serde_json::Value,
    state: &crate::entity_actor::EntityState,
) {
    event.idempotency_key = Some(key.to_string());
    event.idempotency_binding = Some(request_binding(action, params));
    event.idempotency_result = Some(result_digest(state));
}

fn hex_digest(hasher: Sha256) -> String {
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn update_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    hasher.update(len.to_be_bytes());
    hasher.update(bytes);
}

/// Canonical JSON: sorted object keys, ordered arrays, type-tagged and
/// length-prefixed scalars. `top_level` drops server-derived keys of the
/// outermost object only.
fn update_json(hasher: &mut Sha256, value: &serde_json::Value, top_level: bool) {
    match value {
        serde_json::Value::Null => hasher.update([0]),
        serde_json::Value::Bool(value) => hasher.update([1, u8::from(*value)]),
        serde_json::Value::Number(value) => {
            hasher.update([2]);
            update_bytes(hasher, value.to_string().as_bytes());
        }
        serde_json::Value::String(value) => {
            hasher.update([3]);
            update_bytes(hasher, value.as_bytes());
        }
        serde_json::Value::Array(values) => {
            hasher.update([4]);
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                update_json(hasher, value, false);
            }
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map
                .keys()
                .filter(|key| {
                    !(top_level && temper_spec::automaton::is_server_derived_field_name(key))
                })
                .collect();
            keys.sort();
            hasher.update([5]);
            hasher.update((keys.len() as u64).to_be_bytes());
            for key in keys {
                update_bytes(hasher, key.as_bytes());
                update_json(hasher, &map[key.as_str()], false);
            }
        }
    }
}

/// Strip an OData namespace qualifier (`Temper.Example.AddItem` → `AddItem`).
pub fn unqualified_action(action: &str) -> &str {
    action.rsplit('.').next().unwrap_or(action)
}

/// Result of a bound idempotency lookup.
#[derive(Debug)]
pub enum IdempotencyLookup {
    /// No live entry for this key.
    Miss,
    /// The key was used for this exact request; the original response.
    Hit(Box<EntityResponse>),
    /// The key was used for a different action or body.
    Mismatch,
}

/// Maximum number of idempotency entries per actor (TigerStyle budget).
pub const IDEMPOTENCY_BUDGET_PER_ACTOR: usize = 1_000;

/// Time-to-live for idempotency entries in seconds.
pub const IDEMPOTENCY_TTL_SECS: i64 = 3600;

/// A cached idempotent response.
struct IdempotencyEntry {
    /// The cached response to return on duplicate requests.
    response: EntityResponse,
    /// Canonical request binding ([`request_binding`]) of the first request.
    binding: String,
    /// When this entry was created (for TTL eviction).
    created_at: chrono::DateTime<chrono::Utc>,
    /// Who owns the post-dispatch effects of the commit behind this entry.
    effects: EffectsState,
}

/// Ownership of a cached commit's post-dispatch effects (ADR-0182, review
/// correction 1). Only a fresh commit's effects may ever run, exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectsState {
    /// Fresh actor commit; no dispatcher has claimed its effects yet.
    Pending,
    /// A dispatcher is running the effects.
    Claimed,
    /// The effects completed.
    Applied,
    /// A response rebuilt from the journal or the latest in-memory event. It
    /// is a replay, not a newly committed transition.
    Historical,
}

/// Outcome of [`IdempotencyCache::claim_post_dispatch_effects`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectsClaim {
    /// This dispatcher owns the commit's post-dispatch effects and must run
    /// them, then call `mark_effects_applied` or `release_effects_claim`.
    Owner,
    /// Another dispatcher owns (or already ran) the effects: replay only.
    Replay,
    /// A response rebuilt from history: replay only; its re-emitted composite
    /// triggers may be re-run because they are idempotent by design.
    HistoricalReplay,
}

/// Per-entity-actor idempotency cache.
///
/// Thread-safe via `RwLock`. Uses `BTreeMap` for deterministic iteration
/// order (DST compliance).
pub struct IdempotencyCache {
    /// actor_key → (idempotency_key → entry).
    entries: RwLock<BTreeMap<String, BTreeMap<String, IdempotencyEntry>>>,
}

impl IdempotencyCache {
    /// Create a new empty idempotency cache.
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    /// Look up a cached response bound to `binding`.
    pub fn lookup(&self, actor_key: &str, idem_key: &str, binding: &str) -> IdempotencyLookup {
        self.lookup_inner(actor_key, idem_key, binding, false)
    }

    /// Bound lookup that only reports a hit after post-dispatch effects ran.
    ///
    /// HTTP/OData callers use this stricter lookup so a retry after a dropped
    /// actor reply re-enters dispatch and fires effects instead of short-
    /// circuiting at the protocol boundary. A mismatch is reported regardless
    /// of effect state: the key is bound as soon as the first request succeeds.
    pub fn lookup_after_effects_applied(
        &self,
        actor_key: &str,
        idem_key: &str,
        binding: &str,
    ) -> IdempotencyLookup {
        self.lookup_inner(actor_key, idem_key, binding, true)
    }

    fn lookup_inner(
        &self,
        actor_key: &str,
        idem_key: &str,
        binding: &str,
        require_effects_applied: bool,
    ) -> IdempotencyLookup {
        let now = sim_now();
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(entry) = entries
            .get(actor_key)
            .and_then(|actor_entries| actor_entries.get(idem_key))
        else {
            return IdempotencyLookup::Miss;
        };

        let age = now.signed_duration_since(entry.created_at);
        if age.num_seconds() > IDEMPOTENCY_TTL_SECS {
            return IdempotencyLookup::Miss;
        }
        if entry.binding != binding {
            return IdempotencyLookup::Mismatch;
        }
        if require_effects_applied && entry.effects != EffectsState::Applied {
            return IdempotencyLookup::Miss;
        }
        IdempotencyLookup::Hit(Box::new(entry.response.clone()))
    }

    /// Cache a response for a given actor and idempotency key.
    ///
    /// If the per-actor budget is exceeded, the oldest entry is evicted.
    pub fn put(&self, actor_key: &str, idem_key: &str, binding: &str, response: EntityResponse) {
        self.insert(
            actor_key,
            idem_key,
            binding,
            response,
            EffectsState::Pending,
        );
    }

    /// Cache a response rebuilt from history (journal replay or the latest
    /// in-memory event). It is not a new commit, so no dispatcher may run its
    /// transition effects.
    pub fn put_historical(
        &self,
        actor_key: &str,
        idem_key: &str,
        binding: &str,
        response: EntityResponse,
    ) {
        self.insert(
            actor_key,
            idem_key,
            binding,
            response,
            EffectsState::Historical,
        );
    }

    /// Cache a response whose post-dispatch effects are known to be complete.
    pub fn put_effects_applied(
        &self,
        actor_key: &str,
        idem_key: &str,
        binding: &str,
        response: EntityResponse,
    ) {
        self.insert(
            actor_key,
            idem_key,
            binding,
            response,
            EffectsState::Applied,
        );
    }

    /// Claim the post-dispatch effects of the commit cached under `idem_key`.
    ///
    /// Exactly one dispatcher becomes [`EffectsClaim::Owner`] of a fresh
    /// commit. Every other reply for the key is a replay. Without an entry
    /// (an actor without a cache, or an evicted entry) the caller owns the
    /// effects, as before this cache existed.
    pub fn claim_post_dispatch_effects(&self, actor_key: &str, idem_key: &str) -> EffectsClaim {
        let mut entries = self.entries.write().unwrap(); // ci-ok: infallible lock
        let Some(entry) = entries
            .get_mut(actor_key)
            .and_then(|actor_entries| actor_entries.get_mut(idem_key))
        else {
            return EffectsClaim::Owner;
        };
        match entry.effects {
            EffectsState::Pending => {
                entry.effects = EffectsState::Claimed;
                EffectsClaim::Owner
            }
            EffectsState::Claimed | EffectsState::Applied => EffectsClaim::Replay,
            EffectsState::Historical => EffectsClaim::HistoricalReplay,
        }
    }

    /// Return a claim after the owner's effects failed, so a retry can run
    /// them (recovery of genuinely pending effects).
    pub fn release_effects_claim(&self, actor_key: &str, idem_key: &str) {
        let mut entries = self.entries.write().unwrap(); // ci-ok: infallible lock
        if let Some(entry) = entries
            .get_mut(actor_key)
            .and_then(|actor_entries| actor_entries.get_mut(idem_key))
            && entry.effects == EffectsState::Claimed
        {
            entry.effects = EffectsState::Pending;
        }
    }

    /// Mark a cached response as having completed post-dispatch effects.
    pub fn mark_effects_applied(&self, actor_key: &str, idem_key: &str) -> bool {
        let now = sim_now();
        let mut entries = match self.entries.write() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        };
        let Some(actor_entries) = entries.get_mut(actor_key) else {
            return false;
        };
        let Some(entry) = actor_entries.get_mut(idem_key) else {
            return false;
        };

        let age = now.signed_duration_since(entry.created_at);
        if age.num_seconds() > IDEMPOTENCY_TTL_SECS {
            return false;
        }

        if entry.effects != EffectsState::Historical {
            entry.effects = EffectsState::Applied;
        }
        true
    }

    fn insert(
        &self,
        actor_key: &str,
        idem_key: &str,
        binding: &str,
        response: EntityResponse,
        effects: EffectsState,
    ) {
        let now = sim_now();
        let mut entries = self.entries.write().unwrap(); // ci-ok: infallible lock
        let actor_entries = entries.entry(actor_key.to_string()).or_default();

        // Evict expired entries first.
        actor_entries.retain(|_, entry| {
            now.signed_duration_since(entry.created_at).num_seconds() <= IDEMPOTENCY_TTL_SECS
        });

        // Budget enforcement: evict oldest if at capacity.
        while actor_entries.len() >= IDEMPOTENCY_BUDGET_PER_ACTOR {
            // Find the oldest entry by created_at.
            if let Some(oldest_key) = actor_entries
                .iter()
                .min_by_key(|(_, e)| e.created_at)
                .map(|(k, _)| k.clone())
            {
                actor_entries.remove(&oldest_key);
            } else {
                break;
            }
        }

        actor_entries.insert(
            idem_key.to_string(),
            IdempotencyEntry {
                response,
                binding: binding.to_string(),
                created_at: now,
                effects,
            },
        );
    }
}

impl Default for IdempotencyCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
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

        assert!(cache.mark_effects_applied("Order:o1", "key-1"));
        assert!(hit_status(cache.lookup_after_effects_applied("Order:o1", "key-1", B)).is_some());
    }

    #[test]
    fn put_effects_applied_satisfies_protocol_cache_hit() {
        let cache = IdempotencyCache::new();
        cache.put_effects_applied("Order:o1", "key-1", B, make_response("Active"));
        assert_eq!(
            hit_status(cache.lookup_after_effects_applied("Order:o1", "key-1", B)).as_deref(),
            Some("Active")
        );
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

    #[test]
    fn exactly_one_dispatcher_owns_fresh_commit_effects() {
        let cache = IdempotencyCache::new();
        cache.put("Order:o1", "key-1", B, make_response("Active"));
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o1", "key-1"),
            EffectsClaim::Owner
        );
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o1", "key-1"),
            EffectsClaim::Replay
        );
        cache.release_effects_claim("Order:o1", "key-1");
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o1", "key-1"),
            EffectsClaim::Owner,
            "failed effects are released so a retry recovers them"
        );
        assert!(cache.mark_effects_applied("Order:o1", "key-1"));
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o1", "key-1"),
            EffectsClaim::Replay
        );
    }

    #[test]
    fn historical_replays_never_own_transition_effects() {
        let cache = IdempotencyCache::new();
        cache.put_historical("Order:o1", "key-1", B, make_response("Active"));
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o1", "key-1"),
            EffectsClaim::HistoricalReplay
        );
        assert!(matches!(
            cache.lookup_after_effects_applied("Order:o1", "key-1", B),
            IdempotencyLookup::Miss
        ));
        assert_eq!(
            cache.claim_post_dispatch_effects("Order:o2", "absent"),
            EffectsClaim::Owner
        );
    }
}
