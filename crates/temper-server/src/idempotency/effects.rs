//! Ownership of a commit's post-dispatch pipeline, not its reaction cascade.
use tokio::sync::watch;

use super::IdempotencyCache;
use crate::entity_actor::EntityResponse;

pub(super) enum EffectsState {
    Pending,
    Claimed(watch::Sender<Option<EntityResponse>>),
    Applied,
    Historical,
}

impl EffectsState {
    pub(super) fn is_claimed(&self) -> bool {
        matches!(self, Self::Claimed(_))
    }
}

/// Outcome of [`IdempotencyCache::claim_post_dispatch_effects`].
pub enum EffectsClaim<'a> {
    /// This dispatcher must run the effects and complete the attempt. Dropping
    /// the owner releases the claim, including on task cancellation or panic.
    Owner(EffectsOwner<'a>),
    /// Another dispatcher is running the effects. Await its final result.
    Wait(EffectsWaiter),
    /// The effects already completed; return their final response.
    Replay(Box<EntityResponse>),
    /// A response rebuilt from history: no fresh transition effects. Composite
    /// triggers may still be re-run because they are idempotent by design.
    HistoricalReplay,
    /// No cached commit exists; preserve the uncached dispatch path.
    Uncached,
}

/// Exclusive, cancellation-safe ownership of one post-dispatch attempt.
///
/// Only this token can complete or release its claim. Active claims cannot be
/// expired, evicted, or replaced while callers wait for the owner's result.
pub struct EffectsOwner<'a> {
    cache: &'a IdempotencyCache,
    actor_key: String,
    idem_key: String,
    result: watch::Sender<Option<EntityResponse>>,
    completed: bool,
}

impl EffectsOwner<'_> {
    /// Publish the final response to every waiter. Cache successful results;
    /// on failure retain the original committed response for a later retry.
    pub fn complete(mut self, response: &EntityResponse) {
        let mut entries = self
            .cache
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries
            .get_mut(&self.actor_key)
            .and_then(|entries| entries.get_mut(&self.idem_key))
            .expect("active effects entry cannot be evicted");
        assert!(
            matches!(&entry.effects, EffectsState::Claimed(sender) if sender.same_channel(&self.result)),
            "only the effects owner may complete its claim"
        );
        // The final response (or recovered pending attempt) gets a full TTL,
        // even when the effects outlived the original actor-cache entry.
        entry.created_at = temper_runtime::scheduler::sim_now();
        if response.success {
            entry.response = response.clone();
            entry.effects = EffectsState::Applied;
        } else {
            entry.effects = EffectsState::Pending;
        }
        self.result.send_replace(Some(response.clone()));
        self.completed = true;
    }
}

impl Drop for EffectsOwner<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut entries = self
            .cache
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries
            .get_mut(&self.actor_key)
            .and_then(|entries| entries.get_mut(&self.idem_key))
            && matches!(&entry.effects, EffectsState::Claimed(sender) if sender.same_channel(&self.result))
        {
            entry.effects = EffectsState::Pending;
            entry.created_at = temper_runtime::scheduler::sim_now();
        }
        // Dropping the last sender wakes waiters with no result. They compete
        // for the released claim instead of hanging or inventing a success.
    }
}

/// A subscription to one owner's final result, independent of later retries.
pub struct EffectsWaiter(watch::Receiver<Option<EntityResponse>>);

impl EffectsWaiter {
    /// Wait for this attempt, returning `None` if its owner was cancelled.
    pub async fn wait(mut self) -> Option<EntityResponse> {
        loop {
            if let Some(response) = self.0.borrow().clone() {
                return Some(response);
            }
            if self.0.changed().await.is_err() {
                return None;
            }
        }
    }
}

impl IdempotencyCache {
    /// Claim fresh effects or subscribe to the current owner's final result.
    /// Historical responses never own fresh effects. The caller must validate
    /// the request binding and authorization before reaching this boundary.
    pub fn claim_post_dispatch_effects(&self, actor_key: &str, idem_key: &str) -> EffectsClaim<'_> {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = entries
            .get_mut(actor_key)
            .and_then(|entries| entries.get_mut(idem_key))
        else {
            return EffectsClaim::Uncached;
        };
        match &entry.effects {
            EffectsState::Pending => {
                let (result, _) = watch::channel(None);
                entry.effects = EffectsState::Claimed(result.clone());
                EffectsClaim::Owner(EffectsOwner {
                    cache: self,
                    actor_key: actor_key.into(),
                    idem_key: idem_key.into(),
                    result,
                    completed: false,
                })
            }
            EffectsState::Claimed(result) => EffectsClaim::Wait(EffectsWaiter(result.subscribe())),
            EffectsState::Applied => EffectsClaim::Replay(Box::new(entry.response.clone())),
            EffectsState::Historical => EffectsClaim::HistoricalReplay,
        }
    }
}
