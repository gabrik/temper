use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use temper_runtime::persistence::PersistenceError;
use temper_runtime::scheduler::install_deterministic_context;
use temper_server::storage::{
    BoxedEventStore, EntityCatalogRow, QueryPlaneStore, QueryProjectionFieldsRow, StorageStack,
};
use temper_store_sim::{DeterministicRng, SimEventStore};

use super::fixtures::{
    ENTITY_ID, build_state, projected_state, run_isolated, write_snapshot_and_tail,
};

// This spy records every attempted publication, including same-sequence upserts
// that a real monotonic catalog might hide. Journal/replay/backfill are production code.
#[derive(Default)]
struct PublicationSpy {
    writes: Mutex<Vec<String>>,
    rows: Mutex<BTreeMap<String, EntityCatalogRow>>,
}

#[async_trait::async_trait]
impl QueryPlaneStore for PublicationSpy {
    async fn upsert_projection(
        &self,
        tenant: &str,
        entity_type: &str,
        entity_id: &str,
        status: &str,
        fields: &serde_json::Value,
        state: &serde_json::Value,
        sequence_nr: u64,
    ) -> Result<(), PersistenceError> {
        assert_eq!((entity_type, entity_id), ("Order", ENTITY_ID));
        self.writes.lock().unwrap().push(tenant.to_string());
        self.rows.lock().unwrap().insert(
            tenant.to_string(),
            EntityCatalogRow {
                entity_id: entity_id.into(),
                status: status.into(),
                fields: fields.clone(),
                state: Some(state.clone()),
                sequence_nr,
            },
        );
        Ok(())
    }

    async fn remove_projection(
        &self,
        _tenant: &str,
        _entity_type: &str,
        _entity_id: &str,
    ) -> Result<(), PersistenceError> {
        panic!("live recovery must never remove a projection");
    }

    async fn query_field_index(
        &self,
        _tenant: &str,
        _entity_type: &str,
        _where_clause: &str,
        _params: Vec<String>,
    ) -> Result<Option<Vec<String>>, PersistenceError> {
        panic!("boot recovery must not read-repair through the query plane");
    }

    async fn load_projection_fields_many(
        &self,
        _tenant: &str,
        _entity_type: &str,
        _entity_ids: &[String],
        _field_names: &[&str],
    ) -> Result<Option<Vec<QueryProjectionFieldsRow>>, PersistenceError> {
        panic!("boot recovery must not read-repair through the query plane");
    }

    async fn projected_entity_counts_by_tenant(
        &self,
    ) -> Result<Option<Vec<(String, u64)>>, PersistenceError> {
        panic!("backfill must enumerate the journal, not the catalog");
    }
}

#[test]
fn seeded_journal_read_failures_publish_nothing_until_explicit_rerun() {
    // Includes permanent regression seed 519. Vary tail lengths, missing/stale
    // catalogs and repeated read failures with a reproducible seeded schedule.
    for seed in 519..619 {
        let journal = SimEventStore::no_faults(seed);
        let mut rng = DeterministicRng::new(seed);
        let tail_len = 1 + (rng.next_u64() % 5) as usize;
        let failures = 1 + (rng.next_u64() % 3) as usize;
        let stale = rng.chance(0.5);
        let checkpoint = run_isolated(async {
            let (_guard, _clock, _ids) = install_deterministic_context(seed);
            let writer = build_state(StorageStack::from_sim(journal.clone(), None));
            write_snapshot_and_tail(
                &writer,
                &BoxedEventStore::new(journal.clone()),
                "tenant-a",
                tail_len,
            )
            .await
        });
        run_isolated(async {
            let (_guard, _clock, _ids) = install_deterministic_context(seed);
            let projection = Arc::new(PublicationSpy::default());
            if stale {
                let snapshot = &checkpoint.snapshot;
                projection.rows.lock().unwrap().insert(
                    "tenant-a".into(),
                    EntityCatalogRow {
                        entity_id: ENTITY_ID.into(),
                        status: snapshot.status.clone(),
                        fields: snapshot.fields.clone(),
                        state: Some(projected_state(snapshot)),
                        sequence_nr: snapshot.sequence_nr,
                    },
                );
            }
            let before = projection.rows.lock().unwrap().clone();
            let mut storage = StorageStack::from_sim(journal.clone(), None);
            storage.query_plane = Some(projection.clone());
            let reader = build_state(storage);
            reader.populate_index_from_store(&checkpoint.tenant).await;
            checkpoint
                .assert_durable_tail(&BoxedEventStore::new(journal.clone()))
                .await;
            journal.fail_next_reads(&checkpoint.persistence_id(), failures);
            for attempt in 0..failures {
                reader
                    .populate_field_index_from_snapshots(&checkpoint.tenant)
                    .await;
                assert!(
                    projection.writes.lock().unwrap().is_empty(),
                    "seed={seed} attempt={attempt} S={} T={}: failed journal recovery published",
                    checkpoint.snapshot.sequence_nr,
                    checkpoint.current.sequence_nr
                );
                assert_eq!(
                    *projection.rows.lock().unwrap(),
                    before,
                    "seed={seed}: failed recovery must leave stale/missing catalog untouched"
                );
                assert!(reader.actor_registry.read().unwrap().is_empty());
            }
            // No automatic retry guarantee: the caller deliberately runs boot repair again.
            reader
                .populate_field_index_from_snapshots(&checkpoint.tenant)
                .await;
            assert_eq!(
                *projection.writes.lock().unwrap(),
                ["tenant-a"],
                "seed={seed}"
            );
            let row = projection.rows.lock().unwrap()["tenant-a"].clone();
            assert_eq!(
                row.sequence_nr, checkpoint.current.sequence_nr,
                "seed={seed}"
            );
            assert_eq!(row.fields, checkpoint.current.fields, "seed={seed}");
            assert_eq!(
                row.state.as_ref().unwrap()["counters"]["items"],
                checkpoint.current.counters["items"],
                "seed={seed}"
            );
            assert!(reader.actor_registry.read().unwrap().is_empty());
            reader
                .populate_field_index_from_snapshots(&checkpoint.tenant)
                .await;
            assert_eq!(
                projection.rows.lock().unwrap()["tenant-a"],
                row,
                "seed={seed}"
            );
        });
    }
}
