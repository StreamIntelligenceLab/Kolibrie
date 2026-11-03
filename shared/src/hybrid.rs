/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.
 */

//! Identity-stable probabilistic lineage and hybrid WMC evaluation.
//!
//! The lineage DAG is the source of truth.  Top-k is a certified lower-bound
//! evaluator for positive, independent lineage; SDD compilation is the exact
//! fallback for every supported acyclic lineage.

use crate::dictionary::Dictionary;
use crate::provenance::Provenance;
use crate::quoted_triple_store::QuotedTripleStore;
use crate::sdd::{BoolOp, SddBudgetError, SddId, SddManager, SddOperationBudget, VarKind};
use crate::seed_spec::SeedSpec;
use crate::triple::Triple;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Clock abstraction used to make latency-budget behavior deterministic in tests.
pub trait HybridClock: Send + Sync {
    /// The current instant. A test clock returns a value it controls, so a latency
    /// budget can be driven past its deadline without sleeping.
    fn now(&self) -> Instant;
}

/// The real clock, reading [`Instant::now`].
#[derive(Debug, Default)]
pub struct SystemHybridClock;

impl HybridClock for SystemHybridClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Stable identifier for a probabilistic seed. IDs are allocated monotonically
/// and are never recycled by [`SeedRegistry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeedId(u32);

impl SeedId {
    /// The raw identifier, for use as a variable index in a formula.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// Identity of one arrival before it is fanned out to overlapping windows.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventKey {
    /// Stream the event arrived on, normalised by [`normalize_stream_iri`].
    pub stream_iri: String,
    /// The event's own timestamp, not the time it was processed.
    pub event_time: usize,
    /// Arrival order, breaking ties between events sharing a timestamp.
    ///
    /// Needed because two events can carry the same `event_time` and must still be
    /// distinguishable: one arrival is one seed, however many overlapping windows
    /// it later appears in.
    pub sequence: u64,
}

/// Whether a seed stands alone or competes with others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedKind {
    /// Independent of every other seed.
    Independent,
    /// One alternative in a mutually exclusive group, by group id. At most one
    /// member of a group holds, so members must not be multiplied as if
    /// independent.
    ExclusiveGroup(u32),
}

/// One registered seed: a fact, its prior, and its identity.
#[derive(Debug, Clone)]
pub struct SeedRecord {
    /// Stable identifier, also this seed's variable index in a formula.
    pub id: SeedId,
    /// The fact the probability attaches to.
    pub triple: Triple,
    /// Prior probability, in `[0, 1]`.
    pub probability: f64,
    /// Independent, or a member of an exclusive group.
    pub kind: SeedKind,
    /// The arrival this seed came from, for streaming seeds. `None` for a static
    /// one declared against the dataset.
    pub event: Option<EventKey>,
}

/// Why a hybrid evaluation could not proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HybridError {
    /// A probability was not finite, or fell outside `[0, 1]`.
    InvalidProbability,
    /// The configuration is internally inconsistent; the message says how.
    InvalidConfig(String),
    /// The seed id space is used up. Ids are never recycled, so a long-running
    /// process registering per-event seeds will eventually reach this.
    SeedIdExhausted,
    /// A caller supplied an id already registered.
    DuplicateSeedId(SeedId),
    /// A formula referenced a seed that was never registered.
    UnknownSeed(SeedId),
    /// The lineage is cyclic. Recursion is not supported, so a recursive rule is
    /// rejected rather than evaluated to a fixpoint.
    UnsupportedRecursion(String),
    /// The rule cannot be expressed as probabilistic lineage; the message says why.
    UnsupportedRule(String),
    /// A shared lock was poisoned by a panic in another thread, so the state can no
    /// longer be trusted.
    PoisonedState,
}

impl std::fmt::Display for HybridError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProbability => write!(f, "probability must be finite and in [0, 1]"),
            Self::InvalidConfig(msg) => write!(f, "invalid hybrid configuration: {msg}"),
            Self::SeedIdExhausted => write!(f, "seed ID space exhausted"),
            Self::DuplicateSeedId(id) => write!(f, "seed ID {} is already registered", id.get()),
            Self::UnknownSeed(id) => write!(f, "unknown seed ID {}", id.get()),
            Self::UnsupportedRecursion(msg) => {
                write!(f, "hybrid v1 does not support recursion: {msg}")
            }
            Self::UnsupportedRule(msg) => write!(f, "unsupported hybrid rule: {msg}"),
            Self::PoisonedState => write!(f, "hybrid shared state lock is poisoned"),
        }
    }
}

impl std::error::Error for HybridError {}

/// Process-lifetime registry for stable seed identities.
///
/// Identity is the point. One arrival becomes one seed, even when it is fanned out
/// to several overlapping windows — otherwise the same uncertainty would be counted
/// twice and a derived probability would come out too high. Ids are allocated
/// monotonically and never recycled, so an id remains valid for as long as the
/// process lives.
#[derive(Debug, Default)]
pub struct SeedRegistry {
    next_id: u64,
    next_sequence: u64,
    records: BTreeMap<SeedId, SeedRecord>,
    by_event: HashMap<EventKey, SeedId>,
    static_ids: HashMap<Triple, SeedId>,
    groups: HashMap<u32, BTreeSet<SeedId>>,
}

impl SeedRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    fn validate_probability(probability: f64) -> Result<(), HybridError> {
        if probability.is_finite() && (0.0..=1.0).contains(&probability) {
            Ok(())
        } else {
            Err(HybridError::InvalidProbability)
        }
    }

    fn allocate_id(&mut self) -> Result<SeedId, HybridError> {
        if self.next_id > u32::MAX as u64 {
            return Err(HybridError::SeedIdExhausted);
        }
        let id = SeedId(self.next_id as u32);
        self.next_id += 1;
        Ok(id)
    }

    /// Mints a key for a new arrival, taking the next sequence number.
    ///
    /// Call once per arrival. Calling twice for the same event yields two keys and
    /// therefore two seeds, which double-counts the uncertainty.
    pub fn next_event_key(&mut self, stream_iri: &str, event_time: usize) -> EventKey {
        let key = EventKey {
            stream_iri: normalize_stream_iri(stream_iri),
            event_time,
            sequence: self.next_sequence,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);
        key
    }

    /// Registers a streaming seed, or returns the existing id if this event is
    /// already registered.
    ///
    /// Idempotent per [`EventKey`], which is what keeps one arrival to one seed
    /// across overlapping windows.
    pub fn register_occurrence(
        &mut self,
        event: EventKey,
        triple: Triple,
        probability: f64,
    ) -> Result<SeedId, HybridError> {
        Self::validate_probability(probability)?;
        if let Some(id) = self.by_event.get(&event).copied() {
            return Ok(id);
        }
        let id = self.allocate_id()?;
        let record = SeedRecord {
            id,
            triple,
            probability,
            kind: SeedKind::Independent,
            event: Some(event.clone()),
        };
        self.by_event.insert(event, id);
        self.records.insert(id, record);
        Ok(id)
    }

    /// Register a non-streaming seed. Re-registering the same triple returns the
    /// original identity and updates its immutable-run probability assignment.
    pub fn register_static(
        &mut self,
        triple: Triple,
        probability: f64,
    ) -> Result<SeedId, HybridError> {
        Self::validate_probability(probability)?;
        if let Some(id) = self.static_ids.get(&triple).copied() {
            if let Some(record) = self.records.get_mut(&id) {
                record.probability = probability;
            }
            return Ok(id);
        }
        let id = self.allocate_id()?;
        self.static_ids.insert(triple.clone(), id);
        self.records.insert(
            id,
            SeedRecord {
                id,
                triple,
                probability,
                kind: SeedKind::Independent,
                event: None,
            },
        );
        Ok(id)
    }

    /// Registers one alternative of a mutually exclusive group.
    ///
    /// Unlike [`Self::register_static`] this always allocates a fresh id: two
    /// alternatives may assert the same triple, and collapsing them would lose the
    /// distinction the group exists to express.
    pub fn register_exclusive(
        &mut self,
        group_id: u32,
        triple: Triple,
        probability: f64,
    ) -> Result<SeedId, HybridError> {
        Self::validate_probability(probability)?;
        let id = self.allocate_id()?;
        self.records.insert(
            id,
            SeedRecord {
                id,
                triple,
                probability,
                kind: SeedKind::ExclusiveGroup(group_id),
                event: None,
            },
        );
        self.groups.entry(group_id).or_default().insert(id);
        Ok(id)
    }

    fn insert_explicit(
        &mut self,
        raw_id: u32,
        triple: Triple,
        probability: f64,
        kind: SeedKind,
    ) -> Result<SeedId, HybridError> {
        Self::validate_probability(probability)?;
        let id = SeedId(raw_id);
        if self.records.contains_key(&id) {
            return Err(HybridError::DuplicateSeedId(id));
        }
        self.next_id = self.next_id.max(raw_id as u64 + 1);
        self.records.insert(
            id,
            SeedRecord {
                id,
                triple,
                probability,
                kind,
                event: None,
            },
        );
        if let SeedKind::ExclusiveGroup(group) = kind {
            self.groups.entry(group).or_default().insert(id);
        }
        Ok(id)
    }

    /// An immutable view of every registered seed, for one evaluation.
    ///
    /// Evaluation reads a snapshot rather than the live registry so that seeds
    /// arriving mid-evaluation cannot change the answer underneath it.
    pub fn snapshot_all(&self) -> SeedSnapshot {
        SeedSnapshot::from_records(self.records.values().cloned())
    }

    /// A snapshot restricted to `ids`, failing with
    /// [`HybridError::UnknownSeed`] if any is unregistered.
    pub fn snapshot_for_ids<I>(&self, ids: I) -> Result<SeedSnapshot, HybridError>
    where
        I: IntoIterator<Item = SeedId>,
    {
        let requested: BTreeSet<SeedId> = ids.into_iter().collect();
        let mut expanded = requested.clone();
        for id in &requested {
            let record = self.records.get(id).ok_or(HybridError::UnknownSeed(*id))?;
            if let SeedKind::ExclusiveGroup(group) = record.kind {
                if let Some(members) = self.groups.get(&group) {
                    expanded.extend(members.iter().copied());
                }
            }
        }
        Ok(SeedSnapshot::from_records(
            expanded
                .into_iter()
                .filter_map(|id| self.records.get(&id).cloned()),
        ))
    }
}

/// Canonical form of a stream IRI, so the same stream written two ways yields one
/// identity.
///
/// [`EventKey`] holds the normalised form; comparing raw spellings would let one
/// arrival register as two seeds.
pub fn normalize_stream_iri(value: &str) -> String {
    let trimmed = value.trim().trim_start_matches('<').trim_end_matches('>');
    trimmed.strip_prefix(':').unwrap_or(trimmed).to_string()
}

/// Immutable probability and identity view used by one materialisation snapshot.
#[derive(Debug, Clone, Default)]
pub struct SeedSnapshot {
    records: BTreeMap<SeedId, SeedRecord>,
    by_triple: HashMap<Triple, Vec<SeedId>>,
    groups: HashMap<u32, Vec<SeedId>>,
}

impl SeedSnapshot {
    fn from_records<I: IntoIterator<Item = SeedRecord>>(records: I) -> Self {
        let mut snapshot = Self::default();
        for record in records {
            snapshot
                .by_triple
                .entry(record.triple.clone())
                .or_default()
                .push(record.id);
            if let SeedKind::ExclusiveGroup(group) = record.kind {
                snapshot.groups.entry(group).or_default().push(record.id);
            }
            snapshot.records.insert(record.id, record);
        }
        for ids in snapshot.by_triple.values_mut() {
            ids.sort_unstable();
        }
        for ids in snapshot.groups.values_mut() {
            ids.sort_unstable();
        }
        snapshot
    }

    /// A snapshot from a plain fact-to-probability map, treating every seed as
    /// independent.
    pub fn from_probability_seeds(seeds: &HashMap<Triple, f64>) -> Result<Self, HybridError> {
        let mut registry = SeedRegistry::new();
        let mut sorted: Vec<_> = seeds.iter().collect();
        sorted.sort_by_key(|(triple, _)| *triple);
        for (triple, probability) in sorted {
            registry.register_static(triple.clone(), *probability)?;
        }
        Ok(registry.snapshot_all())
    }

    /// A snapshot from declarations, preserving exclusive groups.
    ///
    /// Prefer this over [`Self::from_probability_seeds`] where the seeds include a
    /// distribution over labels: the map form cannot express exclusivity.
    pub fn from_seed_specs(specs: &[SeedSpec]) -> Result<Self, HybridError> {
        let mut registry = SeedRegistry::new();
        for spec in specs {
            match spec {
                SeedSpec::Independent {
                    triple,
                    prob,
                    seed_id,
                } => {
                    registry.insert_explicit(
                        *seed_id,
                        triple.clone(),
                        *prob,
                        SeedKind::Independent,
                    )?;
                }
                SeedSpec::ExclusiveGroup { group_id, choices } => {
                    for choice in choices {
                        registry.insert_explicit(
                            choice.choice_id,
                            choice.triple.clone(),
                            choice.prob,
                            SeedKind::ExclusiveGroup(*group_id),
                        )?;
                    }
                }
            }
        }
        Ok(registry.snapshot_all())
    }

    /// The record for `id`, or `None` if it is not in this snapshot.
    pub fn record(&self, id: SeedId) -> Option<&SeedRecord> {
        self.records.get(&id)
    }
    /// Every seed asserting `triple`. More than one is normal: separate arrivals,
    /// or competing alternatives in an exclusive group.
    pub fn ids_for_triple(&self, triple: &Triple) -> &[SeedId] {
        self.by_triple.get(triple).map(Vec::as_slice).unwrap_or(&[])
    }
    /// Every record, in id order.
    pub fn records(&self) -> impl Iterator<Item = &SeedRecord> {
        self.records.values()
    }
    /// Each distinct fact with the seeds asserting it.
    pub fn triples(&self) -> impl Iterator<Item = (&Triple, &[SeedId])> {
        self.by_triple
            .iter()
            .map(|(triple, ids)| (triple, ids.as_slice()))
    }
    /// Members of an exclusive group, or `None` if no such group exists.
    pub fn group(&self, group: u32) -> Option<&[SeedId]> {
        self.groups.get(&group).map(Vec::as_slice)
    }
}

/// Handle to a node in a [`LineageStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LineageId(u32);

impl LineageId {
    /// The constant false node, always id 0.
    pub const FALSE: Self = Self(0);
    /// The constant true node, always id 1.
    pub const TRUE: Self = Self(1);
    /// The raw index.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// One node of the lineage DAG: how a fact was derived, in terms of seeds.
///
/// `And` and `Or` take a list rather than two operands, so a conjunction of several
/// premises is a single node. That keeps the DAG shallow and the unique table
/// effective.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LineageNode {
    /// Cannot hold.
    False,
    /// Holds unconditionally — a fact with no probabilistic dependency.
    True,
    /// Holds exactly when this seed does.
    Literal(SeedId),
    /// Holds when every operand does.
    And(Vec<LineageId>),
    /// Holds when at least one operand does. This is where alternative derivations
    /// of the same fact meet.
    Or(Vec<LineageId>),
    /// Holds when the operand does not. Negation makes the lineage non-monotone,
    /// which rules out the top-k lower bound.
    Not(LineageId),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Structural properties of a lineage DAG that decide which evaluator may run.
pub struct LineageMetadata {
    /// Whether every connective is positive, so adding a proof can only raise the
    /// probability. Required for the top-k lower bound to be sound.
    pub monotone: bool,
    /// Whether any `Not` appears.
    pub has_negation: bool,
    /// Whether any seed belongs to an exclusive group, which rules out treating
    /// seeds as independent.
    pub has_exclusive_group: bool,
    /// Whether the lineage is cyclic. Recursion is unsupported, so this means the
    /// evaluation must be refused rather than attempted.
    pub has_cycle: bool,
}

/// Hash-consed canonical DAG arena.
#[derive(Debug)]
pub struct LineageStore {
    nodes: Vec<LineageNode>,
    unique: HashMap<LineageNode, LineageId>,
}

impl Default for LineageStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LineageStore {
    /// A store holding only the two constant nodes.
    pub fn new() -> Self {
        let nodes = vec![LineageNode::False, LineageNode::True];
        let unique = nodes
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, node)| (node, LineageId(index as u32)))
            .collect();
        Self { nodes, unique }
    }

    /// The node `id` refers to.
    ///
    /// # Panics
    ///
    /// If `id` did not come from this store.
    pub fn node(&self, id: LineageId) -> &LineageNode {
        &self.nodes[id.0 as usize]
    }
    /// Number of nodes, including the two constants.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    /// Whether the store holds no nodes. A store built by [`Self::new`] is not
    /// empty: it already contains the constants.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    fn intern(&mut self, node: LineageNode) -> LineageId {
        if let Some(id) = self.unique.get(&node).copied() {
            return id;
        }
        let id = LineageId(self.nodes.len() as u32);
        self.nodes.push(node.clone());
        self.unique.insert(node, id);
        id
    }

    /// The node for `seed`, interning it if new.
    pub fn literal(&mut self, seed: SeedId) -> LineageId {
        self.intern(LineageNode::Literal(seed))
    }

    /// Negation of `id`, folding double negation and the constants rather than
    /// building a node for them.
    pub fn not(&mut self, id: LineageId) -> LineageId {
        match self.node(id).clone() {
            LineageNode::False => LineageId::TRUE,
            LineageNode::True => LineageId::FALSE,
            LineageNode::Not(inner) => inner,
            _ => self.intern(LineageNode::Not(id)),
        }
    }

    /// Conjunction of `items`. An empty conjunction is [`LineageId::TRUE`].
    pub fn and<I: IntoIterator<Item = LineageId>>(&mut self, items: I) -> LineageId {
        self.canonical_nary(true, items)
    }

    /// Disjunction of `items`. An empty disjunction is [`LineageId::FALSE`].
    pub fn or<I: IntoIterator<Item = LineageId>>(&mut self, items: I) -> LineageId {
        self.canonical_nary(false, items)
    }

    fn canonical_nary<I: IntoIterator<Item = LineageId>>(
        &mut self,
        is_and: bool,
        items: I,
    ) -> LineageId {
        let identity = if is_and {
            LineageId::TRUE
        } else {
            LineageId::FALSE
        };
        let annihilator = if is_and {
            LineageId::FALSE
        } else {
            LineageId::TRUE
        };
        let mut flattened = Vec::new();
        for item in items {
            if item == annihilator {
                return annihilator;
            }
            if item == identity {
                continue;
            }
            match (is_and, self.node(item)) {
                (true, LineageNode::And(children)) | (false, LineageNode::Or(children)) => {
                    flattened.extend(children.iter().copied());
                }
                _ => flattened.push(item),
            }
        }
        flattened.sort_unstable();
        flattened.dedup();
        let set: HashSet<LineageId> = flattened.iter().copied().collect();
        for item in &flattened {
            if let LineageNode::Not(inner) = self.node(*item) {
                if set.contains(inner) {
                    return annihilator;
                }
            } else if let Some(negated) = self.unique.get(&LineageNode::Not(*item)) {
                if set.contains(negated) {
                    return annihilator;
                }
            }
        }
        match flattened.as_slice() {
            [] => identity,
            [only] => *only,
            _ if is_and => self.intern(LineageNode::And(flattened)),
            _ => self.intern(LineageNode::Or(flattened)),
        }
    }

    /// Properties of the DAG rooted at `root`, which decide which evaluator may
    /// run: top-k needs monotone lineage, and a cycle must be refused.
    pub fn metadata(&self, root: LineageId, seeds: &SeedSnapshot) -> LineageMetadata {
        fn visit(
            store: &LineageStore,
            id: LineageId,
            seeds: &SeedSnapshot,
            visiting: &mut HashSet<LineageId>,
            done: &mut HashSet<LineageId>,
            metadata: &mut LineageMetadata,
        ) {
            if done.contains(&id) {
                return;
            }
            if !visiting.insert(id) {
                metadata.has_cycle = true;
                return;
            }
            match store.node(id) {
                LineageNode::Literal(seed) => {
                    if matches!(
                        seeds.record(*seed).map(|r| r.kind),
                        Some(SeedKind::ExclusiveGroup(_))
                    ) {
                        metadata.has_exclusive_group = true;
                    }
                }
                LineageNode::Not(child) => {
                    metadata.has_negation = true;
                    visit(store, *child, seeds, visiting, done, metadata);
                }
                LineageNode::And(children) | LineageNode::Or(children) => {
                    for child in children {
                        visit(store, *child, seeds, visiting, done, metadata);
                    }
                }
                LineageNode::False | LineageNode::True => {}
            }
            visiting.remove(&id);
            done.insert(id);
        }
        let mut metadata = LineageMetadata {
            monotone: true,
            ..Default::default()
        };
        visit(
            self,
            root,
            seeds,
            &mut HashSet::new(),
            &mut HashSet::new(),
            &mut metadata,
        );
        metadata.monotone = !metadata.has_negation && !metadata.has_cycle;
        metadata
    }
}

/// Provenance semiring whose tags are handles into a shared full lineage DAG.
#[derive(Clone)]
pub struct LineageProvenance {
    store: Arc<Mutex<LineageStore>>,
    seeds: Arc<SeedSnapshot>,
}

impl std::fmt::Debug for LineageProvenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineageProvenance")
            .field("nodes", &self.store.lock().map(|s| s.len()).unwrap_or(0))
            .finish()
    }
}

impl LineageProvenance {
    /// A provenance semiring that records lineage instead of computing a number.
    ///
    /// Its `Tag` is a [`LineageId`], so derivation builds a DAG and the probability
    /// is only computed later by [`evaluate_hybrid`]. That is what lets one
    /// derivation be evaluated exactly or approximately without re-deriving.
    pub fn new(seeds: Arc<SeedSnapshot>) -> Self {
        Self {
            store: Arc::new(Mutex::new(LineageStore::new())),
            seeds,
        }
    }
    /// The shared DAG this semiring writes into.
    pub fn store(&self) -> &Arc<Mutex<LineageStore>> {
        &self.store
    }
    /// The seeds its literals refer to.
    pub fn seeds(&self) -> &Arc<SeedSnapshot> {
        &self.seeds
    }
    /// The lineage node for `seed`.
    pub fn literal(&self, seed: SeedId) -> LineageId {
        self.store.lock().unwrap().literal(seed)
    }
}

impl Provenance for LineageProvenance {
    type Tag = LineageId;
    fn zero(&self) -> Self::Tag {
        LineageId::FALSE
    }
    fn one(&self) -> Self::Tag {
        LineageId::TRUE
    }
    fn disjunction(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag {
        self.store.lock().unwrap().or([*a, *b])
    }
    fn conjunction(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag {
        self.store.lock().unwrap().and([*a, *b])
    }
    fn negate(&self, a: &Self::Tag) -> Self::Tag {
        self.store.lock().unwrap().not(*a)
    }
    fn saturate(&self, a: &Self::Tag) -> Self::Tag {
        *a
    }
    fn tag_from_probability(&self, prob: f64) -> Self::Tag {
        let id = self
            .seeds
            .records()
            .find(|r| (r.probability - prob).abs() < f64::EPSILON)
            .map(|r| r.id)
            .expect("LineageProvenance requires registered seed identities");
        self.literal(id)
    }
    fn tag_from_probability_with_id(&self, _prob: f64, id: usize) -> Self::Tag {
        self.literal(SeedId(id as u32))
    }
    fn recover_probability(&self, tag: &Self::Tag) -> f64 {
        let config = HybridConfig {
            threshold: 0.0,
            ..HybridConfig::default()
        };
        match evaluate_hybrid(&self.store, &self.seeds, *tag, &config) {
            HybridProbabilityResult::Exact { probability, .. } => probability,
            HybridProbabilityResult::LowerBound { lower_bound, .. } => lower_bound,
            HybridProbabilityResult::Bounded { interval, .. } => interval.lower,
            HybridProbabilityResult::NeedsExact { lower_bound, .. } => lower_bound.unwrap_or(0.0),
            HybridProbabilityResult::UnsafeApproximation { estimate, .. } => estimate,
        }
    }
    fn is_saturated(&self, old: &Self::Tag, new: &Self::Tag) -> bool {
        old == new
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Thresholds and budgets governing the escalation from approximate to exact.
///
/// The defaults aim at interactive latency: top-k gets 25 ms, exact compilation
/// 250 ms. Validate with [`Self::validate`] before use.
pub struct HybridConfig {
    /// Probability a fact must reach to be reported. The point of a threshold is
    /// that a lower bound above it, or an upper bound below it, settles the question
    /// without an exact count.
    pub threshold: f64,
    /// Whether `threshold` is taken literally or derived from cost.
    pub threshold_policy: ThresholdPolicyKind,
    /// Half-width of the band around `threshold` within which an approximate answer
    /// is considered too close to call, forcing escalation to exact evaluation.
    pub band_epsilon: f64,
    /// Smallest improvement a further proof must promise for the top-k search to
    /// continue. Stops the search spending its budget on negligible terms.
    pub marginal_gain_floor: f64,
    /// Proofs to enumerate on the first top-k attempt.
    pub k_initial: usize,
    /// Ceiling on proofs enumerated before giving up on top-k.
    pub k_max: usize,
    /// Factor `k` grows by between attempts.
    pub k_growth: usize,
    /// Wall-clock budget for top-k search.
    pub topk_budget: Duration,
    /// Wall-clock budget for SDD compilation.
    pub sdd_budget: Duration,
    /// Node budget for SDD compilation. Compilation can blow up
    /// combinatorially, so both a time and a size limit apply.
    pub sdd_node_budget: usize,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            threshold: 0.5,
            threshold_policy: ThresholdPolicyKind::Explicit,
            band_epsilon: 0.02,
            marginal_gain_floor: 1e-4,
            k_initial: 8,
            k_max: 64,
            k_growth: 2,
            topk_budget: Duration::from_millis(25),
            sdd_budget: Duration::from_millis(250),
            sdd_node_budget: 100_000,
        }
    }
}

/// How the reporting threshold is decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThresholdPolicyKind {
    /// Use [`HybridConfig::threshold`] as given.
    #[default]
    Explicit,
    /// Derive it from the relative cost of approximate and exact evaluation, so
    /// cheap questions get a tighter threshold than expensive ones.
    CostRatio,
}

impl ThresholdPolicyKind {
    /// The policy's name as it appears in a `PROB(...)` annotation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::CostRatio => "auto:cost",
        }
    }
}

/// A closed interval known to contain the true probability.
///
/// Returned when neither bound settles the question against the threshold: the
/// answer is somewhere inside, and the caller decides whether that is good enough.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProbabilityInterval {
    /// Greatest lower bound established.
    pub lower: f64,
    /// Least upper bound established.
    pub upper: f64,
}

impl ProbabilityInterval {
    /// An interval, rejecting bounds that are not finite, not in `[0, 1]`, or
    /// inverted.
    pub fn new(lower: f64, upper: f64) -> Result<Self, HybridError> {
        if !lower.is_finite()
            || !upper.is_finite()
            || !(0.0..=1.0).contains(&lower)
            || !(0.0..=1.0).contains(&upper)
            || lower > upper
        {
            return Err(HybridError::InvalidConfig(
                "probability interval must satisfy 0 <= lower <= upper <= 1".into(),
            ));
        }
        Ok(Self { lower, upper })
    }

    /// How much uncertainty remains.
    pub fn width(self) -> f64 {
        self.upper - self.lower
    }

    /// Whether `probability` lies in the interval, inclusive.
    pub fn contains(self, probability: f64) -> bool {
        self.lower <= probability && probability <= self.upper
    }
}

impl HybridConfig {
    /// Checks the configuration is internally consistent.
    ///
    /// Call before evaluating: an inconsistent configuration would otherwise
    /// surface as a wrong answer rather than an error.
    pub fn validate(&self) -> Result<(), HybridError> {
        if !self.threshold.is_finite() || !(0.0..=1.0).contains(&self.threshold) {
            return Err(HybridError::InvalidConfig(
                "threshold must be in [0, 1]".into(),
            ));
        }
        if !self.band_epsilon.is_finite() || self.band_epsilon < 0.0 || self.band_epsilon > 1.0 {
            return Err(HybridError::InvalidConfig(
                "band_epsilon must be in [0, 1]".into(),
            ));
        }
        if !self.marginal_gain_floor.is_finite() || self.marginal_gain_floor < 0.0 {
            return Err(HybridError::InvalidConfig(
                "marginal_gain_floor must be non-negative".into(),
            ));
        }
        if self.k_initial == 0 || self.k_initial > self.k_max {
            return Err(HybridError::InvalidConfig(
                "require 1 <= k_initial <= k_max".into(),
            ));
        }
        if self.k_growth < 2 {
            return Err(HybridError::InvalidConfig(
                "k_growth must be at least 2".into(),
            ));
        }
        if self.topk_budget.is_zero() || self.sdd_budget.is_zero() || self.sdd_node_budget < 2 {
            return Err(HybridError::InvalidConfig(
                "budgets must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Whether a fact clears the reporting threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertDecision {
    /// Established at or above the threshold.
    Alert,
    /// Established below it.
    NoAlert,
    /// Neither could be established within budget. Distinct from `NoAlert`: the
    /// question is open, not answered in the negative, and a caller that conflates
    /// them silently turns "do not know" into "no".
    Indeterminate,
}

/// Why evaluation produced the result it did.
///
/// This is the audit trail for the escalation policy: it records which rule decided
/// the outcome, so an answer that looks surprising can be explained without
/// re-running it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HybridReason {
    /// Every proof was enumerated, so the lower bound is in fact exact.
    TopKExhausted,
    /// The lower bound reached the threshold, which settles the decision upward
    /// regardless of how much probability remains unaccounted for.
    LowerBoundCrossedThreshold,
    /// The upper bound fell below the threshold, settling it downward.
    UpperBoundBelowThreshold,
    /// Answered by exact SDD compilation.
    ExactSdd,
    /// Negation makes the lineage non-monotone, so the top-k bound is not valid and
    /// exact evaluation is required.
    NegationRequiresExact,
    /// Seeds in an exclusive group cannot be combined as independent, so exact
    /// evaluation is required.
    ExclusivityRequiresExact,
    /// The bounds straddle the threshold within `band_epsilon`, so approximation
    /// cannot decide it.
    NearThreshold,
    /// Further proofs promised less than `marginal_gain_floor`, so the search
    /// stopped rather than spend its budget on negligible terms.
    MarginalGain,
    /// Top-k ran out of time.
    TopKBudget,
    /// SDD compilation ran out of time.
    SddBudget,
    /// SDD compilation ran out of nodes.
    SddNodeBudget,
    /// The lineage referenced a seed absent from the snapshot, so no probability
    /// could be assigned.
    MissingSeed,
    /// Produced for explanation only; not a decision.
    DiagnosticOnly,
}

impl HybridReason {
    /// The reason as a stable kebab-case string, for logs and RDF output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TopKExhausted => "top-k-exhausted",
            Self::LowerBoundCrossedThreshold => "lower-bound-crossed-threshold",
            Self::UpperBoundBelowThreshold => "upper-bound-below-threshold",
            Self::ExactSdd => "exact-sdd",
            Self::NegationRequiresExact => "negation-requires-exact",
            Self::ExclusivityRequiresExact => "exclusivity-requires-exact",
            Self::NearThreshold => "near-threshold",
            Self::MarginalGain => "marginal-gain",
            Self::TopKBudget => "top-k-budget",
            Self::SddBudget => "sdd-budget",
            Self::SddNodeBudget => "sdd-node-budget",
            Self::MissingSeed => "missing-seed",
            Self::DiagnosticOnly => "diagnostic-only",
        }
    }
}

/// What one evaluation cost and how far it got.
#[derive(Debug, Clone, Default)]
pub struct HybridMetrics {
    /// Proofs actually enumerated.
    pub k_used: usize,
    /// Whether exact SDD evaluation ran.
    pub exact_used: bool,
    /// Whether the proof search exhausted the frontier, which makes the lower bound
    /// exact.
    pub frontier_exhausted: bool,
    /// Whether `k_max` was reached before the frontier was exhausted.
    pub cap_hit: bool,
    /// Improvement the last enumerated proof contributed, for comparison against
    /// `marginal_gain_floor`.
    pub marginal_gain: f64,
    /// Time spent in top-k search.
    pub topk_latency: Duration,
    /// Time spent in SDD compilation.
    pub sdd_latency: Duration,
    /// Nodes the SDD reached.
    pub sdd_nodes: usize,
    /// Threshold actually applied, which differs from the configured one under
    /// [`ThresholdPolicyKind::CostRatio`].
    pub effective_threshold: Option<f64>,
    /// Which policy produced that threshold.
    pub threshold_policy: Option<ThresholdPolicyKind>,
    /// Width of the final interval; 0 for an exact answer.
    pub interval_width: f64,
}

impl HybridMetrics {
    /// Time spent across both evaluators.
    pub fn total_latency(&self) -> Duration {
        self.topk_latency + self.sdd_latency
    }
}

/// The outcome of evaluating one fact's probability.
///
/// The variants differ in what was *established*, not merely in precision, and a
/// caller that collapses them to a number loses the distinction between a proven
/// probability and a guess. Only the first three carry a decision; the other two
/// mean the question is open.
#[derive(Debug, Clone)]
pub enum HybridProbabilityResult {
    /// The probability is known exactly.
    Exact {
        /// The probability.
        probability: f64,
        /// Whether it clears the threshold.
        decision: AlertDecision,
        /// Which rule produced this outcome.
        reason: HybridReason,
        /// Cost and progress.
        metrics: HybridMetrics,
    },
    /// Only a lower bound was established, and it was enough to decide.
    ///
    /// Sound because the lineage is monotone: unenumerated proofs can only raise
    /// the probability, so a lower bound above the threshold settles it.
    LowerBound {
        /// Greatest lower bound established.
        lower_bound: f64,
        /// Whether it clears the threshold.
        decision: AlertDecision,
        /// Which rule produced this outcome.
        reason: HybridReason,
        /// Cost and progress.
        metrics: HybridMetrics,
    },
    /// Both bounds were established and together they decide the question.
    Bounded {
        /// The interval containing the true probability.
        interval: ProbabilityInterval,
        /// Whether it clears the threshold.
        decision: AlertDecision,
        /// Which rule produced this outcome.
        reason: HybridReason,
        /// Cost and progress.
        metrics: HybridMetrics,
    },
    /// Approximation could not decide, and exact evaluation has not run.
    ///
    /// Either it was never attempted or it exhausted its budget. Whatever bounds
    /// were reached are reported, but they do not settle the threshold — the
    /// decision is [`AlertDecision::Indeterminate`].
    NeedsExact {
        /// Lower bound reached, if any.
        lower_bound: Option<f64>,
        /// Upper bound reached, if any.
        upper_bound: Option<f64>,
        /// Which rule produced this outcome.
        reason: HybridReason,
        /// Cost and progress.
        metrics: HybridMetrics,
    },
    /// A number was produced, but under conditions that make it unsound as a bound.
    ///
    /// Non-monotone lineage or exclusive groups invalidate the top-k argument, so
    /// this is an estimate with no guaranteed direction of error. Treat it as a
    /// diagnostic, not an answer.
    UnsafeApproximation {
        /// The estimate.
        estimate: f64,
        /// Which rule produced this outcome.
        reason: HybridReason,
        /// Cost and progress.
        metrics: HybridMetrics,
    },
}

impl HybridProbabilityResult {
    /// The variant's name, for logs and RDF output.
    pub fn status(&self) -> &'static str {
        match self {
            Self::Exact { .. } => "Exact",
            Self::LowerBound { .. } => "LowerBound",
            Self::Bounded { .. } => "Bounded",
            Self::NeedsExact { .. } => "NeedsExact",
            Self::UnsafeApproximation { .. } => "UnsafeApproximation",
        }
    }
    /// The threshold decision, which is
    /// [`AlertDecision::Indeterminate`] for the two variants that establish
    /// nothing.
    pub fn decision(&self) -> AlertDecision {
        match self {
            Self::Exact { decision, .. }
            | Self::LowerBound { decision, .. }
            | Self::Bounded { decision, .. } => *decision,
            Self::NeedsExact { .. } | Self::UnsafeApproximation { .. } => {
                AlertDecision::Indeterminate
            }
        }
    }
    /// Which rule produced this outcome.
    pub fn reason(&self) -> &HybridReason {
        match self {
            Self::Exact { reason, .. }
            | Self::LowerBound { reason, .. }
            | Self::Bounded { reason, .. }
            | Self::NeedsExact { reason, .. }
            | Self::UnsafeApproximation { reason, .. } => reason,
        }
    }
    /// Cost and progress of the evaluation that produced this result.
    pub fn metrics(&self) -> &HybridMetrics {
        match self {
            Self::Exact { metrics, .. }
            | Self::LowerBound { metrics, .. }
            | Self::Bounded { metrics, .. }
            | Self::NeedsExact { metrics, .. }
            | Self::UnsafeApproximation { metrics, .. } => metrics,
        }
    }

    /// The bounds established, where that is meaningful.
    ///
    /// `None` for a result that bounded nothing.
    pub fn interval(&self) -> Option<ProbabilityInterval> {
        match self {
            Self::Exact { probability, .. } => Some(ProbabilityInterval {
                lower: *probability,
                upper: *probability,
            }),
            Self::LowerBound { lower_bound, .. } => Some(ProbabilityInterval {
                lower: *lower_bound,
                upper: 1.0,
            }),
            Self::Bounded { interval, .. } => Some(*interval),
            Self::NeedsExact {
                lower_bound,
                upper_bound,
                ..
            } => lower_bound
                .zip(*upper_bound)
                .map(|(lower, upper)| ProbabilityInterval { lower, upper }),
            Self::UnsafeApproximation { .. } => None,
        }
    }
}

type Proof = BTreeSet<SeedId>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ResidualMass {
    Exhausted,
    Bounded(f64),
    Unknown,
}

struct ProofEnumeration {
    proofs: Vec<Proof>,
    residual: ResidualMass,
}

fn proof_probability(proof: &Proof, seeds: &SeedSnapshot) -> Option<f64> {
    proof.iter().try_fold(1.0, |acc, id| {
        seeds.record(*id).map(|r| acc * r.probability)
    })
}

#[derive(Clone)]
struct ProofSearchState {
    pending: Vec<LineageId>,
    proof: Proof,
    upper_bound: f64,
    sequence: u64,
}

impl PartialEq for ProofSearchState {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl Eq for ProofSearchState {}

impl PartialOrd for ProofSearchState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProofSearchState {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper_bound
            .partial_cmp(&other.upper_bound)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

fn enumerate_proofs(
    store: &LineageStore,
    seeds: &SeedSnapshot,
    root: LineageId,
    cap: usize,
    deadline: Instant,
    clock: &dyn HybridClock,
) -> Result<ProofEnumeration, HybridReason> {
    if cap == 0 {
        return Ok(ProofEnumeration {
            proofs: Vec::new(),
            residual: ResidualMass::Bounded(1.0),
        });
    }
    let mut frontier = BinaryHeap::new();
    let mut sequence = 0u64;
    frontier.push(ProofSearchState {
        pending: vec![root],
        proof: Proof::new(),
        upper_bound: 1.0,
        sequence,
    });
    let mut emitted: Vec<Proof> = Vec::with_capacity(cap);

    while let Some(mut state) = frontier.pop() {
        if clock.now() >= deadline {
            return Ok(ProofEnumeration {
                proofs: emitted,
                residual: ResidualMass::Unknown,
            });
        }
        let Some(next) = state.pending.pop() else {
            if emitted
                .iter()
                .any(|existing| existing.is_subset(&state.proof))
            {
                continue;
            }
            emitted.retain(|existing| !state.proof.is_subset(existing));
            emitted.push(state.proof);
            if emitted.len() == cap {
                let residual = frontier
                    .iter()
                    .map(|state| state.upper_bound)
                    .sum::<f64>()
                    .clamp(0.0, 1.0);
                return Ok(ProofEnumeration {
                    proofs: emitted,
                    residual: ResidualMass::Bounded(residual),
                });
            }
            continue;
        };

        match store.node(next) {
            LineageNode::False => {}
            LineageNode::True => {
                sequence = sequence.saturating_add(1);
                state.sequence = sequence;
                frontier.push(state);
            }
            LineageNode::Literal(seed) => {
                state.proof.insert(*seed);
                state.upper_bound =
                    proof_probability(&state.proof, seeds).ok_or(HybridReason::MissingSeed)?;
                sequence = sequence.saturating_add(1);
                state.sequence = sequence;
                frontier.push(state);
            }
            LineageNode::Not(_) => return Err(HybridReason::NegationRequiresExact),
            LineageNode::And(children) => {
                state.pending.extend(children.iter().rev().copied());
                sequence = sequence.saturating_add(1);
                state.sequence = sequence;
                frontier.push(state);
            }
            LineageNode::Or(children) => {
                for child in children {
                    let mut branch = state.clone();
                    branch.pending.push(*child);
                    sequence = sequence.saturating_add(1);
                    branch.sequence = sequence;
                    frontier.push(branch);
                }
            }
        }
    }
    Ok(ProofEnumeration {
        proofs: emitted,
        residual: ResidualMass::Exhausted,
    })
}

fn interval_from_enumeration(
    lower_bound: f64,
    proofs: &[Proof],
    retained_count: usize,
    residual: ResidualMass,
    seeds: &SeedSnapshot,
) -> Result<Option<ProbabilityInterval>, HybridReason> {
    let frontier_mass = match residual {
        ResidualMass::Exhausted => 0.0,
        ResidualMass::Bounded(mass) => mass,
        ResidualMass::Unknown => return Ok(None),
    };
    let probe_mass = proofs[retained_count..]
        .iter()
        .try_fold(0.0, |sum, proof| {
            proof_probability(proof, seeds)
                .map(|probability| sum + probability)
                .ok_or(HybridReason::MissingSeed)
        })?;
    let upper = (lower_bound + probe_mass + frontier_mass).clamp(lower_bound, 1.0);
    ProbabilityInterval::new(lower_bound, upper)
        .map(Some)
        .map_err(|_| HybridReason::DiagnosticOnly)
}

#[derive(Debug)]
enum CompileFailure {
    Deadline,
    Nodes,
    MissingSeed,
}

fn retained_proof_wmc(
    proofs: &[Proof],
    seeds: &SeedSnapshot,
    deadline: Instant,
    node_budget: usize,
    clock: &dyn HybridClock,
) -> Result<(f64, usize), CompileFailure> {
    let mut manager = SddManager::new();
    let ids: BTreeSet<SeedId> = proofs.iter().flat_map(|p| p.iter().copied()).collect();
    for id in ids {
        let record = seeds.record(id).ok_or(CompileFailure::MissingSeed)?;
        manager.ensure_variable(id.get(), record.probability);
    }
    let mut deadline_available = || clock.now() < deadline;
    let mut budget = SddOperationBudget::new(node_budget, &mut deadline_available);
    let mut formula = SddId::FALSE;
    for proof in proofs {
        let mut clause = SddId::TRUE;
        for seed in proof {
            let literal = manager
                .try_literal(seed.get(), true, &mut budget)
                .map_err(map_compile_budget_error)?;
            clause = manager
                .try_apply(clause, literal, BoolOp::And, &mut budget)
                .map_err(map_compile_budget_error)?;
        }
        formula = manager
            .try_apply(formula, clause, BoolOp::Or, &mut budget)
            .map_err(map_compile_budget_error)?;
    }
    Ok((manager.wmc(formula).clamp(0.0, 1.0), manager.node_count()))
}

fn map_compile_budget_error(error: SddBudgetError) -> CompileFailure {
    match error {
        SddBudgetError::DeadlineExceeded => CompileFailure::Deadline,
        SddBudgetError::NodeBudgetExceeded => CompileFailure::Nodes,
    }
}

fn map_hybrid_budget_error(error: SddBudgetError) -> HybridReason {
    match error {
        SddBudgetError::DeadlineExceeded => HybridReason::SddBudget,
        SddBudgetError::NodeBudgetExceeded => HybridReason::SddNodeBudget,
    }
}

#[derive(Debug, Clone)]
/// What the top-k proof search established.
pub struct TopKEvaluation {
    /// Probability accounted for by the proofs enumerated. A lower bound on the
    /// truth, because the lineage is monotone.
    pub lower_bound: f64,
    /// Bounds on the true probability.
    pub interval: ProbabilityInterval,
    /// Proofs enumerated.
    pub k_used: usize,
    /// Whether every proof was found, which makes `lower_bound` exact.
    pub frontier_exhausted: bool,
    /// Whether `k_max` stopped the search first.
    pub cap_hit: bool,
    /// Contribution of the last proof, against `marginal_gain_floor`.
    pub marginal_gain: f64,
}

/// Evaluate one positive, independent lineage cone at a fixed `k`. The WMC is
/// exact for the retained proof formula and therefore a certified lower bound.
pub fn evaluate_topk(
    store: &LineageStore,
    seeds: &SeedSnapshot,
    root: LineageId,
    k: usize,
    budget: Duration,
    node_budget: usize,
) -> Result<TopKEvaluation, HybridReason> {
    if k == 0 {
        return Err(HybridReason::DiagnosticOnly);
    }
    let clock = SystemHybridClock;
    let deadline = clock.now() + budget;
    let metadata = store.metadata(root, seeds);
    if metadata.has_negation || !metadata.monotone {
        return Err(HybridReason::NegationRequiresExact);
    }
    if metadata.has_exclusive_group {
        return Err(HybridReason::ExclusivityRequiresExact);
    }
    let enumeration = enumerate_proofs(store, seeds, root, k.saturating_add(1), deadline, &clock)?;
    if enumeration.residual == ResidualMass::Unknown {
        return Err(HybridReason::TopKBudget);
    }
    let proofs = enumeration.proofs;
    let retained_count = proofs.len().min(k);
    let retained = &proofs[..retained_count];
    let lower_bound = retained_proof_wmc(retained, seeds, deadline, node_budget, &clock)
        .map_err(|failure| match failure {
            CompileFailure::Deadline => HybridReason::TopKBudget,
            CompileFailure::Nodes => HybridReason::SddNodeBudget,
            CompileFailure::MissingSeed => HybridReason::MissingSeed,
        })?
        .0;
    let marginal_gain = if proofs.len() > k {
        retained_proof_wmc(&proofs[..=k], seeds, deadline, node_budget, &clock)
            .map(|(with_probe, _)| (with_probe - lower_bound).max(0.0))
            .unwrap_or(0.0)
    } else {
        0.0
    };
    let interval = interval_from_enumeration(
        lower_bound,
        &proofs,
        retained_count,
        enumeration.residual,
        seeds,
    )?
    .ok_or(HybridReason::TopKBudget)?;
    let frontier_exhausted = enumeration.residual == ResidualMass::Exhausted && proofs.len() <= k;
    Ok(TopKEvaluation {
        lower_bound,
        interval,
        k_used: retained_count,
        frontier_exhausted,
        cap_hit: proofs.len() > k || !frontier_exhausted,
        marginal_gain,
    })
}

/// A lineage DAG compiled to an SDD, ready for exact weighted model counting.
pub struct CompiledSdd {
    /// The manager owning the compiled nodes.
    pub manager: SddManager,
    /// Root node corresponding to the lineage root.
    pub root: SddId,
}

// Cannot derive: `SddManager` has no `Debug`, and would not want one — it owns every
// compiled node. The node count is the figure that matters, since it is what the
// compilation budget is spent on.
impl std::fmt::Debug for CompiledSdd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSdd")
            .field("nodes", &self.manager.node_count())
            .field("root", &self.root)
            .finish()
    }
}

fn collect_seed_ids(store: &LineageStore, root: LineageId, output: &mut BTreeSet<SeedId>) {
    match store.node(root) {
        LineageNode::Literal(seed) => {
            output.insert(*seed);
        }
        LineageNode::Not(child) => collect_seed_ids(store, *child, output),
        LineageNode::And(children) | LineageNode::Or(children) => {
            for child in children {
                collect_seed_ids(store, *child, output);
            }
        }
        LineageNode::False | LineageNode::True => {}
    }
}

/// Compiles lineage to an SDD, within a time and node budget.
///
/// Returns the reason on failure rather than an error type, so the caller can record
/// why exact evaluation was unavailable alongside whatever bound it already had.
pub fn compile_lineage_to_sdd(
    store: &LineageStore,
    seeds: &SeedSnapshot,
    root: LineageId,
    budget: Duration,
    node_budget: usize,
) -> Result<CompiledSdd, HybridReason> {
    compile_lineage_to_sdd_with_clock(store, seeds, root, budget, node_budget, &SystemHybridClock)
}

/// [`compile_lineage_to_sdd`] against a caller-supplied clock, so a budget can be
/// driven past its deadline deterministically in tests.
pub fn compile_lineage_to_sdd_with_clock(
    store: &LineageStore,
    seeds: &SeedSnapshot,
    root: LineageId,
    budget: Duration,
    node_budget: usize,
    clock: &dyn HybridClock,
) -> Result<CompiledSdd, HybridReason> {
    let deadline = clock.now() + budget;
    let mut referenced = BTreeSet::new();
    collect_seed_ids(store, root, &mut referenced);
    let mut expanded = referenced.clone();
    for seed in &referenced {
        let record = seeds.record(*seed).ok_or(HybridReason::MissingSeed)?;
        if let SeedKind::ExclusiveGroup(group) = record.kind {
            if let Some(members) = seeds.group(group) {
                expanded.extend(members.iter().copied());
            }
        }
    }

    let mut manager = SddManager::new();
    for seed in &expanded {
        let record = seeds.record(*seed).ok_or(HybridReason::MissingSeed)?;
        match record.kind {
            SeedKind::Independent => manager.ensure_variable(seed.get(), record.probability),
            SeedKind::ExclusiveGroup(group) => manager.ensure_variable_weights(
                seed.get(),
                record.probability,
                1.0,
                VarKind::ExclusiveGroup(group),
            ),
        }
    }

    let mut deadline_available = || clock.now() < deadline;
    let mut operation_budget = SddOperationBudget::new(node_budget, &mut deadline_available);

    fn compile(
        store: &LineageStore,
        id: LineageId,
        manager: &mut SddManager,
        memo: &mut HashMap<LineageId, SddId>,
        budget: &mut SddOperationBudget<'_>,
    ) -> Result<SddId, HybridReason> {
        if let Some(value) = memo.get(&id).copied() {
            return Ok(value);
        }
        let result = match store.node(id) {
            LineageNode::False => SddId::FALSE,
            LineageNode::True => SddId::TRUE,
            LineageNode::Literal(seed) => manager
                .try_literal(seed.get(), true, budget)
                .map_err(map_hybrid_budget_error)?,
            LineageNode::Not(child) => {
                let compiled = compile(store, *child, manager, memo, budget)?;
                manager
                    .try_negate(compiled, budget)
                    .map_err(map_hybrid_budget_error)?
            }
            LineageNode::And(children) | LineageNode::Or(children) => {
                let operation = if matches!(store.node(id), LineageNode::And(_)) {
                    BoolOp::And
                } else {
                    BoolOp::Or
                };
                let identity = if operation == BoolOp::And {
                    SddId::TRUE
                } else {
                    SddId::FALSE
                };
                let mut accumulated = identity;
                for child in children {
                    let compiled = compile(store, *child, manager, memo, budget)?;
                    accumulated = manager
                        .try_apply(accumulated, compiled, operation, budget)
                        .map_err(map_hybrid_budget_error)?;
                }
                accumulated
            }
        };
        memo.insert(id, result);
        Ok(result)
    }

    let mut root_sdd = compile(
        store,
        root,
        &mut manager,
        &mut HashMap::new(),
        &mut operation_budget,
    )?;
    let groups: BTreeSet<u32> = referenced
        .iter()
        .filter_map(|id| match seeds.record(*id).map(|r| r.kind) {
            Some(SeedKind::ExclusiveGroup(group)) => Some(group),
            _ => None,
        })
        .collect();
    for group in groups {
        let vars: Vec<u32> = seeds
            .group(group)
            .unwrap_or(&[])
            .iter()
            .map(|id| id.get())
            .collect();
        let exactly_one = manager
            .try_exactly_one(&vars, &mut operation_budget)
            .map_err(map_hybrid_budget_error)?;
        root_sdd = manager
            .try_apply(root_sdd, exactly_one, BoolOp::And, &mut operation_budget)
            .map_err(map_hybrid_budget_error)?;
    }
    Ok(CompiledSdd {
        manager,
        root: root_sdd,
    })
}

/// Evaluates the probability of the fact at `root`, escalating as needed.
///
/// Tries the cheap path first and only compiles exactly when it must: top-k gives a
/// certified lower bound for monotone lineage, and that settles the threshold
/// whenever the bound lands clear of it. Negation, exclusive groups, and a result too
/// near the threshold all force exact compilation.
///
/// Always returns a result rather than failing: a budget that runs out yields
/// [`HybridProbabilityResult::NeedsExact`] carrying whatever bounds were reached, so
/// the caller can distinguish "below threshold" from "could not determine".
pub fn evaluate_hybrid(
    store: &Arc<Mutex<LineageStore>>,
    seeds: &Arc<SeedSnapshot>,
    root: LineageId,
    config: &HybridConfig,
) -> HybridProbabilityResult {
    evaluate_hybrid_with_clock(store, seeds, root, config, &SystemHybridClock)
}

/// [`evaluate_hybrid`] against a caller-supplied clock, for deterministic tests of
/// budget expiry.
pub fn evaluate_hybrid_with_clock(
    store: &Arc<Mutex<LineageStore>>,
    seeds: &Arc<SeedSnapshot>,
    root: LineageId,
    config: &HybridConfig,
    clock: &dyn HybridClock,
) -> HybridProbabilityResult {
    HybridEscalationController {
        store,
        seeds,
        config,
        clock,
    }
    .evaluate(root)
}

struct HybridEscalationController<'a> {
    store: &'a Arc<Mutex<LineageStore>>,
    seeds: &'a Arc<SeedSnapshot>,
    config: &'a HybridConfig,
    clock: &'a dyn HybridClock,
}

impl HybridEscalationController<'_> {
    fn evaluate(&self, root: LineageId) -> HybridProbabilityResult {
        evaluate_hybrid_controlled(self.store, self.seeds, root, self.config, self.clock)
    }
}

fn evaluate_hybrid_controlled(
    store: &Arc<Mutex<LineageStore>>,
    seeds: &Arc<SeedSnapshot>,
    root: LineageId,
    config: &HybridConfig,
    clock: &dyn HybridClock,
) -> HybridProbabilityResult {
    if let Err(_) = config.validate() {
        return HybridProbabilityResult::NeedsExact {
            lower_bound: None,
            upper_bound: None,
            reason: HybridReason::DiagnosticOnly,
            metrics: HybridMetrics::default(),
        };
    }
    let guard = match store.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return HybridProbabilityResult::NeedsExact {
                lower_bound: None,
                upper_bound: None,
                reason: HybridReason::DiagnosticOnly,
                metrics: HybridMetrics::default(),
            }
        }
    };
    let metadata = guard.metadata(root, seeds);
    let mut metrics = HybridMetrics {
        effective_threshold: Some(config.threshold),
        threshold_policy: Some(config.threshold_policy),
        ..HybridMetrics::default()
    };
    let topk_start = clock.now();
    let topk_deadline = topk_start + config.topk_budget;
    let mut lower_bound = None;
    let mut last_interval = None;
    let supported_topk = metadata.monotone && !metadata.has_exclusive_group && !metadata.has_cycle;

    if supported_topk {
        let mut k = config.k_initial;
        loop {
            let cap = k.saturating_add(1);
            let enumeration = enumerate_proofs(&guard, seeds, root, cap, topk_deadline, clock);
            let enumeration = match enumeration {
                Ok(value) => value,
                Err(_) => break,
            };
            if enumeration.residual == ResidualMass::Unknown {
                break;
            }
            let proofs = enumeration.proofs;
            let retained_count = proofs.len().min(k);
            let retained = &proofs[..retained_count];
            let wmc = match retained_proof_wmc(
                retained,
                seeds,
                topk_deadline,
                config.sdd_node_budget,
                clock,
            ) {
                Ok((value, _)) => value,
                Err(_) => break,
            };
            lower_bound = Some(wmc);
            metrics.k_used = retained_count;
            metrics.frontier_exhausted =
                enumeration.residual == ResidualMass::Exhausted && proofs.len() <= k;
            metrics.cap_hit = proofs.len() > k || !metrics.frontier_exhausted;
            metrics.marginal_gain = if proofs.len() > k {
                retained_proof_wmc(
                    &proofs[..=k],
                    seeds,
                    topk_deadline,
                    config.sdd_node_budget,
                    clock,
                )
                .map(|(with_probe, _)| (with_probe - wmc).max(0.0))
                .unwrap_or(0.0)
            } else {
                0.0
            };

            let Some(interval) = interval_from_enumeration(
                wmc,
                &proofs,
                retained_count,
                enumeration.residual,
                seeds,
            )
            .ok()
            .flatten() else {
                break;
            };
            last_interval = Some(interval);
            metrics.interval_width = interval.width();

            if metrics.frontier_exhausted {
                metrics.topk_latency = clock.now().saturating_duration_since(topk_start);
                return HybridProbabilityResult::Exact {
                    probability: wmc,
                    decision: if wmc >= config.threshold {
                        AlertDecision::Alert
                    } else {
                        AlertDecision::NoAlert
                    },
                    reason: HybridReason::TopKExhausted,
                    metrics,
                };
            }
            if wmc >= config.threshold {
                metrics.topk_latency = clock.now().saturating_duration_since(topk_start);
                return HybridProbabilityResult::Bounded {
                    interval,
                    decision: AlertDecision::Alert,
                    reason: HybridReason::LowerBoundCrossedThreshold,
                    metrics,
                };
            }
            if interval.upper < config.threshold {
                metrics.topk_latency = clock.now().saturating_duration_since(topk_start);
                return HybridProbabilityResult::Bounded {
                    interval,
                    decision: AlertDecision::NoAlert,
                    reason: HybridReason::UpperBoundBelowThreshold,
                    metrics,
                };
            }
            let near = (config.threshold - wmc).abs() <= config.band_epsilon;
            let climbing = metrics.marginal_gain >= config.marginal_gain_floor;
            if k >= config.k_max || (!near && !climbing) || clock.now() >= topk_deadline {
                break;
            }
            k = k.saturating_mul(config.k_growth).min(config.k_max);
        }
    }
    metrics.topk_latency = clock.now().saturating_duration_since(topk_start);

    let sdd_start = clock.now();
    match compile_lineage_to_sdd_with_clock(
        &guard,
        seeds,
        root,
        config.sdd_budget,
        config.sdd_node_budget,
        clock,
    ) {
        Ok(compiled) => {
            let probability = compiled.manager.wmc(compiled.root).clamp(0.0, 1.0);
            metrics.exact_used = true;
            metrics.sdd_nodes = compiled.manager.node_count();
            metrics.interval_width = 0.0;
            metrics.sdd_latency = clock.now().saturating_duration_since(sdd_start);
            HybridProbabilityResult::Exact {
                probability,
                decision: if probability >= config.threshold {
                    AlertDecision::Alert
                } else {
                    AlertDecision::NoAlert
                },
                reason: HybridReason::ExactSdd,
                metrics,
            }
        }
        Err(reason) => {
            metrics.exact_used = true;
            metrics.sdd_latency = clock.now().saturating_duration_since(sdd_start);
            HybridProbabilityResult::NeedsExact {
                lower_bound: last_interval.map(|interval| interval.lower).or(lower_bound),
                upper_bound: last_interval.map(|interval| interval.upper),
                reason,
                metrics,
            }
        }
    }
}

/// Encode decision-safe hybrid results as RDF-star annotations. Exact values and
/// lower bounds deliberately use different predicates.
pub fn encode_hybrid_results_as_rdf_star(
    results: &HashMap<Triple, HybridProbabilityResult>,
    dict: &mut Dictionary,
    quoted: &mut QuotedTripleStore,
) -> Vec<Triple> {
    let status_pred = dict.encode("http://www.w3.org/ns/prob#status");
    let decision_pred = dict.encode("http://www.w3.org/ns/prob#decision");
    let reason_pred = dict.encode("http://www.w3.org/ns/prob#reason");
    let value_pred = dict.encode("http://www.w3.org/ns/prob#value");
    let lower_pred = dict.encode("http://www.w3.org/ns/prob#lowerBound");
    let upper_pred = dict.encode("http://www.w3.org/ns/prob#upperBound");
    let estimate_pred = dict.encode("http://www.w3.org/ns/prob#estimate");
    let k_pred = dict.encode("http://www.w3.org/ns/prob#kUsed");
    let exact_pred = dict.encode("http://www.w3.org/ns/prob#exactUsed");
    let latency_pred = dict.encode("http://www.w3.org/ns/prob#latencyMicros");
    let nodes_pred = dict.encode("http://www.w3.org/ns/prob#sddNodes");
    let threshold_pred = dict.encode("http://www.w3.org/ns/prob#effectiveThreshold");
    let threshold_policy_pred = dict.encode("http://www.w3.org/ns/prob#thresholdPolicy");
    fn push_string(
        dict: &mut Dictionary,
        subject: u32,
        predicate: u32,
        value: &str,
        output: &mut Vec<Triple>,
    ) {
        let object = dict.encode(&format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#string>",
            value
        ));
        output.push(Triple {
            subject,
            predicate,
            object,
        });
    }
    fn push_double(
        dict: &mut Dictionary,
        subject: u32,
        predicate: u32,
        value: f64,
        output: &mut Vec<Triple>,
    ) {
        let object = dict.encode(&format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#double>",
            value
        ));
        output.push(Triple {
            subject,
            predicate,
            object,
        });
    }
    let mut output = Vec::new();
    for (triple, result) in results {
        let subject = quoted.encode(triple.subject, triple.predicate, triple.object);
        push_string(dict, subject, status_pred, result.status(), &mut output);
        let decision = match result.decision() {
            AlertDecision::Alert => "Alert",
            AlertDecision::NoAlert => "NoAlert",
            AlertDecision::Indeterminate => "Indeterminate",
        };
        push_string(dict, subject, decision_pred, decision, &mut output);
        push_string(
            dict,
            subject,
            reason_pred,
            result.reason().as_str(),
            &mut output,
        );
        match result {
            HybridProbabilityResult::Exact { probability, .. } => {
                push_double(dict, subject, value_pred, *probability, &mut output);
            }
            HybridProbabilityResult::LowerBound { lower_bound, .. } => {
                push_double(dict, subject, lower_pred, *lower_bound, &mut output);
            }
            HybridProbabilityResult::Bounded { interval, .. } => {
                push_double(dict, subject, lower_pred, interval.lower, &mut output);
                push_double(dict, subject, upper_pred, interval.upper, &mut output);
            }
            HybridProbabilityResult::NeedsExact {
                lower_bound,
                upper_bound,
                ..
            } => {
                if let Some(lower) = lower_bound {
                    push_double(dict, subject, lower_pred, *lower, &mut output);
                }
                if let Some(upper) = upper_bound {
                    push_double(dict, subject, upper_pred, *upper, &mut output);
                }
            }
            HybridProbabilityResult::UnsafeApproximation { estimate, .. } => {
                push_double(dict, subject, estimate_pred, *estimate, &mut output);
            }
        }
        let metrics = result.metrics();
        if let Some(effective_threshold) = metrics.effective_threshold {
            push_double(
                dict,
                subject,
                threshold_pred,
                effective_threshold,
                &mut output,
            );
        }
        if let Some(threshold_policy) = metrics.threshold_policy {
            push_string(
                dict,
                subject,
                threshold_policy_pred,
                threshold_policy.as_str(),
                &mut output,
            );
        }
        for (predicate, value) in [
            (k_pred, metrics.k_used as u128),
            (latency_pred, metrics.total_latency().as_micros()),
            (nodes_pred, metrics.sdd_nodes as u128),
        ] {
            let object = dict.encode(&format!(
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                value
            ));
            output.push(Triple {
                subject,
                predicate,
                object,
            });
        }
        let exact_object = dict.encode(&format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            metrics.exact_used
        ));
        output.push(Triple {
            subject,
            predicate: exact_pred,
            object: exact_object,
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StepClock {
        current: Mutex<Instant>,
        step: Duration,
    }

    impl StepClock {
        fn new(step: Duration) -> Self {
            Self {
                current: Mutex::new(Instant::now()),
                step,
            }
        }
    }

    impl HybridClock for StepClock {
        fn now(&self) -> Instant {
            let mut current = self.current.lock().unwrap();
            let value = *current;
            *current += self.step;
            value
        }
    }

    fn triple(n: u32) -> Triple {
        Triple {
            subject: n,
            predicate: 10,
            object: 20,
        }
    }

    #[test]
    fn registry_distinguishes_occurrences_and_never_reuses_ids() {
        let mut registry = SeedRegistry::new();
        let e1 = registry.next_event_key("<stream>", 10);
        let e2 = registry.next_event_key("stream", 10);
        let a = registry
            .register_occurrence(e1.clone(), triple(1), 0.5)
            .unwrap();
        let same = registry.register_occurrence(e1, triple(1), 0.5).unwrap();
        let b = registry.register_occurrence(e2, triple(1), 0.5).unwrap();
        assert_eq!(a, same);
        assert_ne!(a, b);
        assert!(b > a);
    }

    #[test]
    fn lineage_canonicalizes_and_detects_complements() {
        let mut store = LineageStore::new();
        let x = store.literal(SeedId(0));
        let y = store.literal(SeedId(1));
        assert_eq!(store.and([x, LineageId::TRUE, x]), x);
        assert_eq!(store.or([y, LineageId::FALSE, y]), y);
        let nx = store.not(x);
        assert_eq!(store.and([x, nx]), LineageId::FALSE);
        assert_eq!(store.or([x, nx]), LineageId::TRUE);
    }

    fn overlap_fixture() -> (Arc<Mutex<LineageStore>>, Arc<SeedSnapshot>, LineageId) {
        let seeds: HashMap<Triple, f64> = [(triple(1), 0.8), (triple(2), 0.6), (triple(3), 0.5)]
            .into_iter()
            .collect();
        let snapshot = Arc::new(SeedSnapshot::from_probability_seeds(&seeds).unwrap());
        let mut store = LineageStore::new();
        let ids: Vec<_> = snapshot.records().map(|r| r.id).collect();
        let x = store.literal(ids[0]);
        let y = store.literal(ids[1]);
        let z = store.literal(ids[2]);
        let xy = store.and([x, y]);
        let xz = store.and([x, z]);
        let root = store.or([xy, xz]);
        (Arc::new(Mutex::new(store)), snapshot, root)
    }

    #[test]
    fn overlapping_proofs_use_exact_retained_wmc() {
        let (store, seeds, root) = overlap_fixture();
        let result = evaluate_hybrid(&store, &seeds, root, &HybridConfig::default());
        match result {
            HybridProbabilityResult::Exact { probability, .. } => {
                assert!((probability - 0.64).abs() < 1e-9)
            }
            other => panic!("expected exact result, got {other:?}"),
        }
    }

    #[test]
    fn certified_lower_bound_alert_avoids_sdd() {
        let (store, seeds, root) = overlap_fixture();
        let config = HybridConfig {
            threshold: 0.3,
            k_initial: 1,
            k_max: 1,
            ..Default::default()
        };
        let result = evaluate_hybrid(&store, &seeds, root, &config);
        assert!(matches!(
            result,
            HybridProbabilityResult::Bounded {
                decision: AlertDecision::Alert,
                metrics: HybridMetrics {
                    exact_used: false,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn certified_upper_bound_no_alert_avoids_sdd() {
        let (store, seeds, root) = overlap_fixture();
        let config = HybridConfig {
            threshold: 0.9,
            k_initial: 1,
            k_max: 1,
            ..Default::default()
        };
        match evaluate_hybrid(&store, &seeds, root, &config) {
            HybridProbabilityResult::Bounded {
                interval,
                decision,
                metrics,
                reason,
            } => {
                assert_eq!(decision, AlertDecision::NoAlert);
                assert_eq!(reason, HybridReason::UpperBoundBelowThreshold);
                assert!(interval.upper < config.threshold);
                assert!(!metrics.exact_used);
            }
            other => panic!("expected certified bounded no-alert, got {other:?}"),
        }
    }

    #[test]
    fn uncertain_interval_escalates_to_exact_sdd() {
        let (store, seeds, root) = overlap_fixture();
        let config = HybridConfig {
            threshold: 0.6,
            k_initial: 1,
            k_max: 1,
            ..Default::default()
        };
        match evaluate_hybrid(&store, &seeds, root, &config) {
            HybridProbabilityResult::Exact {
                probability,
                metrics,
                ..
            } => {
                assert!((probability - 0.64).abs() < 1e-9);
                assert!(metrics.exact_used);
            }
            other => panic!("expected exact SDD escalation, got {other:?}"),
        }
    }

    #[test]
    fn increasing_k_monotonically_tightens_the_lower_bound() {
        let (store, seeds, root) = overlap_fixture();
        let guard = store.lock().unwrap();
        let k1 = evaluate_topk(&guard, &seeds, root, 1, Duration::from_secs(1), 10_000).unwrap();
        let k2 = evaluate_topk(&guard, &seeds, root, 2, Duration::from_secs(1), 10_000).unwrap();
        assert!(k1.lower_bound <= k2.lower_bound);
        assert!(k1.interval.contains(0.64));
        assert!((k2.lower_bound - 0.64).abs() < 1e-9);
        assert!((k2.interval.upper - 0.64).abs() < 1e-9);
        assert!(k2.frontier_exhausted);
    }

    #[test]
    fn lazy_probe_reports_cap_hit_and_exact_marginal_wmc() {
        let (store, seeds, root) = overlap_fixture();
        let guard = store.lock().unwrap();
        let result =
            evaluate_topk(&guard, &seeds, root, 1, Duration::from_secs(1), 10_000).unwrap();
        assert!((result.lower_bound - 0.48).abs() < 1e-9);
        assert!((result.marginal_gain - 0.16).abs() < 1e-9);
        assert!(result.cap_hit);
        assert!(!result.frontier_exhausted);
        assert!(result.interval.contains(0.64));
    }

    #[test]
    fn full_remaining_heap_and_probe_are_in_the_upper_bound() {
        let probabilities = [0.9, 0.8, 0.1];
        let seeds = SeedSnapshot::from_probability_seeds(
            &probabilities
                .iter()
                .enumerate()
                .map(|(index, probability)| (triple(index as u32), *probability))
                .collect(),
        )
        .unwrap();
        let mut store = LineageStore::new();
        let literals: Vec<_> = seeds
            .records()
            .map(|record| store.literal(record.id))
            .collect();
        let root = store.or(literals);
        let clock = SystemHybridClock;
        let enumeration = enumerate_proofs(
            &store,
            &seeds,
            root,
            2,
            clock.now() + Duration::from_secs(1),
            &clock,
        )
        .unwrap();
        assert_eq!(enumeration.proofs.len(), 2);
        match enumeration.residual {
            ResidualMass::Bounded(mass) => assert!((mass - 0.1).abs() < 1e-9),
            other => panic!("expected bounded remaining heap, got {other:?}"),
        }
        let retained = retained_proof_wmc(
            &enumeration.proofs[..1],
            &seeds,
            clock.now() + Duration::from_secs(1),
            10_000,
            &clock,
        )
        .unwrap()
        .0;
        let interval = interval_from_enumeration(
            retained,
            &enumeration.proofs,
            1,
            enumeration.residual,
            &seeds,
        )
        .unwrap()
        .unwrap();
        assert_eq!(interval.upper, 1.0);
    }

    #[test]
    fn subsumed_proofs_do_not_inflate_an_exhausted_interval() {
        let seeds = SeedSnapshot::from_probability_seeds(
            &[(triple(1), 0.8), (triple(2), 0.5)].into_iter().collect(),
        )
        .unwrap();
        let ids: Vec<_> = seeds.records().map(|record| record.id).collect();
        let mut store = LineageStore::new();
        let x = store.literal(ids[0]);
        let y = store.literal(ids[1]);
        let xy = store.and([x, y]);
        let root = store.or([x, xy]);
        let result =
            evaluate_topk(&store, &seeds, root, 1, Duration::from_secs(1), 10_000).unwrap();
        assert!(result.frontier_exhausted);
        assert!((result.interval.lower - 0.8).abs() < 1e-9);
        assert!((result.interval.upper - 0.8).abs() < 1e-9);
    }

    #[test]
    fn adaptive_controller_tightens_until_frontier_exhaustion() {
        let (store, seeds, root) = overlap_fixture();
        let config = HybridConfig {
            threshold: 0.7,
            k_initial: 1,
            k_max: 2,
            ..HybridConfig::default()
        };
        match evaluate_hybrid(&store, &seeds, root, &config) {
            HybridProbabilityResult::Exact {
                probability,
                decision,
                metrics,
                ..
            } => {
                assert!((probability - 0.64).abs() < 1e-9);
                assert_eq!(decision, AlertDecision::NoAlert);
                assert_eq!(metrics.k_used, 2);
                assert!(metrics.frontier_exhausted);
            }
            other => panic!("expected exhausted exact result, got {other:?}"),
        }
    }

    #[test]
    fn topk_rejects_negated_lineage_instead_of_synthesizing_a_proof() {
        let (store, seeds, root) = overlap_fixture();
        let negated = store.lock().unwrap().not(root);
        let guard = store.lock().unwrap();
        assert_eq!(
            evaluate_topk(&guard, &seeds, negated, 4, Duration::from_secs(1), 10_000,).unwrap_err(),
            HybridReason::NegationRequiresExact,
        );
    }

    #[test]
    fn negation_uses_exact_sdd() {
        let (store, seeds, root) = overlap_fixture();
        let negated = store.lock().unwrap().not(root);
        let result = evaluate_hybrid(&store, &seeds, negated, &HybridConfig::default());
        match result {
            HybridProbabilityResult::Exact {
                probability,
                metrics,
                ..
            } => {
                assert!((probability - 0.36).abs() < 1e-9);
                assert!(metrics.exact_used);
            }
            other => panic!("expected exact result, got {other:?}"),
        }
    }

    fn brute_force(store: &LineageStore, seeds: &SeedSnapshot, root: LineageId) -> f64 {
        fn truth(store: &LineageStore, id: LineageId, assignment: &HashMap<SeedId, bool>) -> bool {
            match store.node(id) {
                LineageNode::False => false,
                LineageNode::True => true,
                LineageNode::Literal(seed) => assignment.get(seed).copied().unwrap_or(false),
                LineageNode::Not(child) => !truth(store, *child, assignment),
                LineageNode::And(children) => children
                    .iter()
                    .all(|child| truth(store, *child, assignment)),
                LineageNode::Or(children) => children
                    .iter()
                    .any(|child| truth(store, *child, assignment)),
            }
        }
        let records: Vec<_> = seeds.records().collect();
        let mut total = 0.0;
        for mask in 0..(1usize << records.len()) {
            let mut assignment = HashMap::new();
            let mut weight = 1.0;
            for (index, record) in records.iter().enumerate() {
                let value = mask & (1 << index) != 0;
                assignment.insert(record.id, value);
                weight *= if value {
                    record.probability
                } else {
                    1.0 - record.probability
                };
            }
            if truth(store, root, &assignment) {
                total += weight;
            }
        }
        total
    }

    #[test]
    fn selective_sdd_matches_bruteforce_oracle() {
        let (store, seeds, root) = overlap_fixture();
        let guard = store.lock().unwrap();
        let oracle = brute_force(&guard, &seeds, root);
        let compiled =
            compile_lineage_to_sdd(&guard, &seeds, root, Duration::from_secs(1), 10_000).unwrap();
        assert!((compiled.manager.wmc(compiled.root) - oracle).abs() < 1e-9);
    }

    #[test]
    fn random_monotone_dag_intervals_contain_exact_sdd_probability() {
        let seed_map: HashMap<_, _> = (0..5)
            .map(|index| (triple(index), 0.15 + index as f64 * 0.13))
            .collect();
        let seeds = SeedSnapshot::from_probability_seeds(&seed_map).unwrap();
        let ids: Vec<_> = seeds.records().map(|record| record.id).collect();
        let mut state = 0x5eed_u64;
        for _case in 0..32 {
            let mut store = LineageStore::new();
            let literals: Vec<_> = ids.iter().map(|id| store.literal(*id)).collect();
            let mut proofs = Vec::new();
            for _ in 0..6 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let mut mask = ((state >> 32) as usize) & 0x1f;
                if mask == 0 {
                    mask = 1;
                }
                proofs.push(store.and(literals.iter().enumerate().filter_map(
                    |(index, literal)| (mask & (1 << index) != 0).then_some(*literal),
                )));
            }
            let root = store.or(proofs);
            let topk =
                evaluate_topk(&store, &seeds, root, 2, Duration::from_secs(1), 100_000).unwrap();
            let compiled =
                compile_lineage_to_sdd(&store, &seeds, root, Duration::from_secs(1), 100_000)
                    .unwrap();
            let exact = compiled.manager.wmc(compiled.root);
            assert!(
                topk.interval.contains(exact),
                "exact {exact} escaped interval {:?}",
                topk.interval
            );
            if topk.frontier_exhausted {
                assert!((topk.interval.lower - exact).abs() < 1e-9);
                assert!((topk.interval.upper - exact).abs() < 1e-9);
            }
            let store = Arc::new(Mutex::new(store));
            let seeds = Arc::new(seeds.clone());
            let config = HybridConfig {
                threshold: 0.5,
                k_initial: 2,
                k_max: 4,
                ..HybridConfig::default()
            };
            let result = evaluate_hybrid(&store, &seeds, root, &config);
            assert_eq!(
                result.decision(),
                if exact >= config.threshold {
                    AlertDecision::Alert
                } else {
                    AlertDecision::NoAlert
                }
            );
        }
    }

    #[test]
    fn failed_retained_wmc_does_not_publish_an_interval() {
        let (store, seeds, root) = overlap_fixture();
        let config = HybridConfig {
            threshold: 0.6,
            k_initial: 1,
            k_max: 1,
            sdd_node_budget: 2,
            ..HybridConfig::default()
        };
        match evaluate_hybrid(&store, &seeds, root, &config) {
            HybridProbabilityResult::NeedsExact {
                lower_bound,
                upper_bound,
                ..
            } => {
                assert!(lower_bound.is_none());
                assert!(upper_bound.is_none());
            }
            other => panic!("expected NeedsExact without an interval, got {other:?}"),
        }
    }

    #[test]
    fn node_budget_exhaustion_is_indeterminate() {
        let (store, seeds, root) = overlap_fixture();
        let negated = store.lock().unwrap().not(root);
        let config = HybridConfig {
            sdd_node_budget: 2,
            ..HybridConfig::default()
        };
        let result = evaluate_hybrid(&store, &seeds, negated, &config);
        assert!(matches!(
            result,
            HybridProbabilityResult::NeedsExact {
                reason: HybridReason::SddNodeBudget,
                ..
            }
        ));
    }

    #[test]
    fn injected_clock_makes_deadline_exhaustion_deterministic() {
        let (store, seeds, root) = overlap_fixture();
        let clock = StepClock::new(Duration::from_millis(10));
        let config = HybridConfig {
            topk_budget: Duration::from_millis(1),
            sdd_budget: Duration::from_millis(1),
            ..HybridConfig::default()
        };
        let result = evaluate_hybrid_with_clock(&store, &seeds, root, &config, &clock);
        assert!(matches!(
            result,
            HybridProbabilityResult::NeedsExact {
                reason: HybridReason::SddBudget,
                ..
            }
        ));
    }

    #[test]
    fn exclusive_group_is_compiled_with_exactly_one_constraint() {
        let specs = vec![SeedSpec::ExclusiveGroup {
            group_id: 7,
            choices: vec![
                crate::seed_spec::ExclusiveChoice {
                    triple: triple(1),
                    prob: 0.2,
                    choice_id: 0,
                },
                crate::seed_spec::ExclusiveChoice {
                    triple: triple(2),
                    prob: 0.3,
                    choice_id: 1,
                },
                crate::seed_spec::ExclusiveChoice {
                    triple: triple(3),
                    prob: 0.5,
                    choice_id: 2,
                },
            ],
        }];
        let seeds = Arc::new(SeedSnapshot::from_seed_specs(&specs).unwrap());
        let mut store = LineageStore::new();
        let root = store.literal(SeedId(0));
        let store = Arc::new(Mutex::new(store));
        let result = evaluate_hybrid(&store, &seeds, root, &HybridConfig::default());
        match result {
            HybridProbabilityResult::Exact {
                probability,
                metrics,
                ..
            } => {
                assert!((probability - 0.2).abs() < 1e-9);
                assert!(metrics.exact_used);
            }
            other => panic!("expected exact result, got {other:?}"),
        }
    }

    #[test]
    fn bounded_rdf_contains_both_bounds_and_threshold_metadata() {
        let interval = ProbabilityInterval::new(0.1, 0.15).unwrap();
        let metrics = HybridMetrics {
            effective_threshold: Some(0.2),
            threshold_policy: Some(ThresholdPolicyKind::CostRatio),
            interval_width: interval.width(),
            ..HybridMetrics::default()
        };
        let results = [(
            triple(42),
            HybridProbabilityResult::Bounded {
                interval,
                decision: AlertDecision::NoAlert,
                reason: HybridReason::UpperBoundBelowThreshold,
                metrics,
            },
        )]
        .into_iter()
        .collect();
        let mut dictionary = Dictionary::new();
        let mut quoted = QuotedTripleStore::new();
        let encoded = encode_hybrid_results_as_rdf_star(&results, &mut dictionary, &mut quoted);
        let lower = dictionary.encode("http://www.w3.org/ns/prob#lowerBound");
        let upper = dictionary.encode("http://www.w3.org/ns/prob#upperBound");
        let value = dictionary.encode("http://www.w3.org/ns/prob#value");
        let threshold = dictionary.encode("http://www.w3.org/ns/prob#effectiveThreshold");
        let policy = dictionary.encode("http://www.w3.org/ns/prob#thresholdPolicy");
        assert_eq!(
            encoded
                .iter()
                .filter(|triple| triple.predicate == lower)
                .count(),
            1
        );
        assert_eq!(
            encoded
                .iter()
                .filter(|triple| triple.predicate == upper)
                .count(),
            1
        );
        assert_eq!(
            encoded
                .iter()
                .filter(|triple| triple.predicate == value)
                .count(),
            0
        );
        assert_eq!(
            encoded
                .iter()
                .filter(|triple| triple.predicate == threshold)
                .count(),
            1
        );
        let policy_object = encoded
            .iter()
            .find(|triple| triple.predicate == policy)
            .and_then(|triple| dictionary.decode(triple.object))
            .unwrap();
        assert!(policy_object.contains("auto:cost"));
    }
}
