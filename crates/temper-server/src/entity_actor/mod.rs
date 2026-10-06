//! Generic entity actor powered by JIT transition tables.
//!
//! This is the bridge between the actor runtime and the state machine specs.
//! Each entity actor holds its current state and a TransitionTable, and
//! processes action messages by evaluating transitions through the table.

pub(crate) mod action_input;
mod actor;
pub(crate) mod bootstrap;
pub mod effects;
mod field_updates;
mod replay_validation;
pub mod sim_handler;
mod snapshot_queue;
pub mod types;

pub use actor::EntityActor;
pub(crate) use actor::{
    recover_authoritative_entity_state_from_store, recover_entity_state_from_store,
};
pub use effects::{
    ProcessResult, ScheduledAction, apply_effects, apply_new_state_fallback, build_eval_context,
    process_action, process_action_with_xref, sync_fields,
};
pub use sim_handler::EntityActorHandler;
pub(crate) use snapshot_queue::SnapshotWriteQueue;
pub use types::{EntityEvent, EntityMsg, EntityResponse, EntityState};

mod idempotency_replay {
    //! Durable verification of reused idempotency keys (ADR-0182).
    //!
    //! When the shared idempotency cache is cold (restart, eviction) a key found in
    //! `processed_idempotency_keys` is verified against the journal event that
    //! carries it, and the original response is rebuilt by replaying the journal up
    //! to that event. Keys that cannot be verified fail closed.

    use serde_json::Value;
    use temper_jit::table::TransitionTable;

    use super::actor::{EntityActor, ReplayPolicy, ReplayTarget};
    use super::types::{EntityEvent, EntityResponse, EntityState};
    use crate::idempotency::unqualified_action;

    /// Outcome of resolving a key that already produced a durable event.
    pub(super) enum ProcessedKeyResolution {
        /// Same logical request: the entity state right after the original event.
        Original(Box<EntityState>),
        /// The key was used for a different action or body.
        Mismatch,
        /// The key is recorded but its original request cannot be verified.
        Unverifiable,
    }

    /// Does `event` (which carries the request's key) record this same request?
    ///
    /// Bound events compare the stored binding. Legacy events (written before
    /// ADR-0182) are verified by repeating the transform the actor applied when it
    /// journaled them: `sanitize(normalize_ref(action, params))` must equal the
    /// stored `params`, and the unqualified action name must match.
    fn event_matches_request(
        event: &EntityEvent,
        state: &EntityState,
        action: &str,
        params: &Value,
        binding: &str,
    ) -> bool {
        if let Some(stored) = event.idempotency_binding.as_deref() {
            return stored == binding;
        }
        let action = unqualified_action(action);
        let normalized = super::effects::normalize_ref_action_params(state, action, params);
        let journaled = super::effects::sanitize_action_params(normalized.as_ref());
        event.action == action && event.params == *journaled
    }

    /// Failed reply for a mismatched or unverifiable key. Appends nothing.
    pub(super) fn idempotency_rejection(state: &EntityState, error: &str) -> EntityResponse {
        EntityResponse {
            success: false,
            state: state.clone(),
            error: Some(error.to_string()),
            custom_effects: vec![],
            scheduled_actions: vec![],
            spawn_requests: vec![],
            spec_governed: true,
        }
    }

    impl EntityActor {
        /// Resolve a request whose key is already in `processed_idempotency_keys`.
        pub(super) async fn resolve_processed_idempotency_key(
            &self,
            table: &TransitionTable,
            state: &EntityState,
            key: &str,
            action: &str,
            params: &Value,
            binding: &str,
        ) -> ProcessedKeyResolution {
            // The latest event carries the key: the current state is the original
            // response, no replay needed.
            if let Some(last) = state.events.back()
                && last.idempotency_key.as_deref() == Some(key)
            {
                return if event_matches_request(last, state, action, params, binding) {
                    ProcessedKeyResolution::Original(Box::new(state.clone()))
                } else {
                    ProcessedKeyResolution::Mismatch
                };
            }

            let (Some(store), Some(backend)) = (self.event_journal.as_ref(), self.event_backend)
            else {
                return ProcessedKeyResolution::Unverifiable;
            };
            let mut replayed = EntityActor::build_initial_state(
                &state.entity_type,
                &state.entity_id,
                table,
                &self.initial_fields,
            );
            let target = ReplayTarget {
                policy: ReplayPolicy::StrictFullJournal,
                stop_after_idempotency_key: Some(key),
            };
            if let Err(error) = Self::replay_events_until(
                table,
                store,
                backend,
                &mut replayed,
                &self.tenant,
                self.blob_store.as_ref(),
                target,
            )
            .await
            {
                tracing::warn!(
                    tenant = %self.tenant,
                    entity_type = %state.entity_type,
                    entity_id = %state.entity_id,
                    %error,
                    "idempotency key cannot be verified: journal replay failed"
                );
                return ProcessedKeyResolution::Unverifiable;
            }

            match replayed.events.back() {
                Some(event) if event.idempotency_key.as_deref() == Some(key) => {
                    if event_matches_request(event, state, action, params, binding) {
                        ProcessedKeyResolution::Original(Box::new(replayed))
                    } else {
                        ProcessedKeyResolution::Mismatch
                    }
                }
                _ => ProcessedKeyResolution::Unverifiable,
            }
        }
    }
}
