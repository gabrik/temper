//! Stateright `Model` implementation for `TemperModel`.
//!
//! Implements `init_states`, `actions`, `next_state`, and `properties` to
//! enable exhaustive model checking via Stateright. Supports multi-variable
//! state (counters + booleans), safety invariants, and liveness properties.

use stateright::{Model, Property};

use super::semantics::{apply_effects, evaluate_guard, guard_may_hold, successors, truth};
use temper_spec::predicate::Truth;

use super::types::{
    LivenessKind, ResolvedTransition, TemperModel, TemperModelAction, TemperModelState,
};

// -- Property condition functions (bare fn pointers) -------------------------

/// Check that the current status is a declared state.
fn check_status_in_set(model: &TemperModel, state: &TemperModelState) -> bool {
    model.states.contains(&state.status)
}

/// Check that no invariant is false in this state. An invariant over values
/// the model does not track evaluates to unknown and is not a violation.
fn check_invariants(model: &TemperModel, state: &TemperModelState) -> bool {
    model
        .invariants
        .iter()
        .all(|inv| truth(&inv.assert, &model.var_kinds, state) != Truth::False)
}

/// Check that no transition is locally enabled from a terminal state.
fn check_terminal(model: &TemperModel, state: &TemperModelState) -> bool {
    if !model.terminal.contains(&state.status) {
        return true;
    }
    !model.transitions.iter().any(|t| {
        let status_ok =
            t.from_states.is_empty() || t.from_states.iter().any(|s| s == &state.status);
        status_ok && evaluate_guard(&t.guard, &model.var_kinds, state)
    })
}

// -- Liveness property functions ---------------------------------------------

/// Check liveness: from the specified states, at least one action is enabled.
/// (Deadlock freedom expressed as a safety property.)
fn check_no_deadlock(model: &TemperModel, state: &TemperModelState) -> bool {
    for live in &model.liveness {
        if let LivenessKind::NoDeadlock { ref from } = live.kind
            && from.contains(&state.status)
        {
            // Must have at least one enabled action
            let mut has_action = false;
            for t in &model.transitions {
                let status_ok =
                    t.from_states.is_empty() || t.from_states.iter().any(|s| s == &state.status);
                if status_ok && evaluate_guard(&t.guard, &model.var_kinds, state) {
                    has_action = true;
                    break;
                }
            }
            if !has_action {
                return false;
            }
        }
    }
    true
}

/// Check liveness: from the specified states, eventually reaches a target state.
///
/// Returns `true` when the current state is in any ReachesState target set.
/// Stateright's `eventually` verifies that on every acyclic path, this
/// predicate becomes true at some point.
///
/// Note: Stateright requires `fn` pointers, so we combine all ReachesState
/// properties. For specs with multiple ReachesState targets, "eventually
/// reaches any target" is verified.
fn check_reaches_state(model: &TemperModel, state: &TemperModelState) -> bool {
    for live in &model.liveness {
        if let LivenessKind::ReachesState { targets, .. } = &live.kind
            && !targets.is_empty()
            && targets.contains(&state.status)
        {
            return true;
        }
    }
    // No target state reached yet.
    // If there are no ReachesState properties, return true (vacuously satisfied).
    !model.liveness.iter().any(
        |l| matches!(&l.kind, LivenessKind::ReachesState { targets, .. } if !targets.is_empty()),
    )
}

// -- Model trait implementation ----------------------------------------------

impl TemperModel {
    /// Whether `transition` is enabled in `state`: status precondition, then guard.
    ///
    /// The single definition of "this edge may fire here", so that the action
    /// generator and any caller that applies an action out of band cannot drift
    /// apart on what the spec permits.
    fn transition_enabled(
        &self,
        transition: &ResolvedTransition,
        state: &TemperModelState,
    ) -> bool {
        let status_ok = transition.from_states.is_empty()
            || transition.from_states.iter().any(|s| s == &state.status);
        // Guard checked for *fireability*: a cross-entity guard is a free
        // boolean, so `guard_may_hold` admits the gated edge rather than
        // pruning it. See `actions` for why that is the right direction.
        status_ok && guard_may_hold(&transition.guard, &self.var_kinds, state)
    }

    /// Whether `action` may fire in `state`.
    ///
    /// [`Model::next_state`] cannot answer this: it resolves a transition by
    /// name and applies it unconditionally, which is sound under Stateright
    /// (every action it applies came from `actions` for that same state) but
    /// not for a caller that holds an action across a state change. The
    /// simulator does exactly that -- it chooses an action while an actor is in
    /// one state and delivers it, after a fault-injected delay, into another --
    /// so it must ask this first.
    ///
    /// An unknown action name is not enabled; there is no transition to fire.
    pub fn action_enabled(&self, state: &TemperModelState, action: &TemperModelAction) -> bool {
        self.transitions
            .iter()
            .find(|t| t.name == action.name)
            .is_some_and(|t| self.transition_enabled(t, state))
    }
}

impl Model for TemperModel {
    type State = TemperModelState;
    type Action = TemperModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![TemperModelState {
            status: self.initial_status.clone(),
            counters: self.initial_counters.clone(),
            booleans: self.initial_booleans.clone(),
            lists: self.initial_lists.clone(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for t in &self.transitions {
            // Status precondition and guard, via the shared predicate that
            // `action_enabled` also uses -- see there for the guard rationale.
            if !self.transition_enabled(t, state) {
                continue;
            }

            // One action per parameter assignment whose step stays within
            // the exploration bounds.
            for (params, _) in successors(self, t, state) {
                actions.push(TemperModelAction {
                    name: t.name.clone(),
                    target_state: t.to_state.clone(),
                    params,
                });
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let resolved = self.transitions.iter().find(|t| t.name == action.name)?;

        let new_status = action.target_state.unwrap_or_else(|| state.status.clone());
        let mut next = state.clone();
        next.status = new_status;
        apply_effects(&resolved.effects, &mut next, &action.name, &action.params);
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = Vec::new();

        props.push(Property::always("TypeInvariant", check_status_in_set));
        if !self.invariants.is_empty() {
            props.push(Property::always("Invariants", check_invariants));
        }
        if !self.terminal.is_empty() {
            props.push(Property::always("Terminal", check_terminal));
        }

        // Liveness: NoDeadlock (expressed as safety: "always has actions")
        let has_no_deadlock = self
            .liveness
            .iter()
            .any(|l| matches!(l.kind, LivenessKind::NoDeadlock { .. }));
        if has_no_deadlock {
            props.push(Property::always("NoDeadlock", check_no_deadlock));
        }

        // Liveness: ReachesState (Stateright's eventually — acyclic paths only)
        let has_reaches = self
            .liveness
            .iter()
            .any(|l| matches!(&l.kind, LivenessKind::ReachesState { targets, .. } if !targets.is_empty()));
        if has_reaches {
            props.push(Property::eventually("ReachesTerminal", check_reaches_state));
        }

        props
    }
}
