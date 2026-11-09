use kolibrie::{database::KolibrieDB, ml_policy::MlExecutionContext};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn database() -> KolibrieDB {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "kolibrie-local-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    KolibrieDB::with_ml_context(MlExecutionContext::trusted_local(&root).unwrap())
}
