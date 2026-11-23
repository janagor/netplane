//! Immutable routing policies for ordinary request selectors.

use std::collections::HashSet;

use thiserror::Error;

/// A named backend and its configured percentage of sticky-key buckets.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WeightedTarget {
    /// Backend label returned by selection.
    name: String,
    /// Integer percentage, including zero for a disabled target.
    weight: u8,
}

/// An invalid weighted policy, independent of graph topology validation.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum WeightedRoutingError {
    /// At least one target is required.
    #[error("weighted routing requires at least one target")]
    EmptyTargets,
    /// Target names must not be empty.
    #[error("weighted routing target names must not be empty")]
    EmptyName,
    /// Each target name must occur once, including zero-weight targets.
    #[error("duplicate weighted routing target: {0}")]
    DuplicateTarget(String),
    /// Percentages must total exactly 100.
    #[error("weighted routing weights must total 100, got {total}")]
    InvalidTotal {
        /// Sum of the configured weights.
        total: usize,
    },
}

/// Deterministic selection over 100 sticky-key buckets in configured order.
///
/// Selection hashes arbitrary key bytes with 64-bit FNV-1a modulo 100, then
/// chooses the first cumulative weight greater than that bucket. Zero-weight
/// targets never receive keys. An unchanged policy always selects the same
/// label for the same key. Changing weights or order can reassign keys.
/// Increasing the first target's weight expands its range monotonically.
///
/// Weights allocate buckets, not exact request traffic: key frequencies and
/// request rates differ. FNV-1a is non-cryptographic; selection is not an
/// authorization or security boundary. Key validation belongs to the caller.
/// This policy performs no I/O and owns no services or runtime workers.
#[derive(Clone, Debug)]
pub struct WeightedRouting {
    targets: Box<[WeightedTarget]>,
    cumulative: Box<[usize]>,
}

impl WeightedRouting {
    /// Validate ordered targets before constructing graph workers.
    ///
    /// # Errors
    /// Returns [`WeightedRoutingError`] for no targets, empty or duplicate names,
    /// or weights whose sum differs from 100. Zero weights are allowed.
    pub fn try_new<I, S>(targets: I) -> Result<Self, WeightedRoutingError>
    where
        I: IntoIterator<Item = (S, u8)>,
        S: Into<String>,
    {
        let targets: Box<[_]> = targets
            .into_iter()
            .map(|(name, weight)| WeightedTarget {
                name: name.into(),
                weight,
            })
            .collect();
        if targets.is_empty() {
            return Err(WeightedRoutingError::EmptyTargets);
        }
        let mut names = HashSet::new();
        let mut total = 0_usize;
        let mut cumulative = Vec::with_capacity(targets.len());
        for target in &targets {
            if target.name.is_empty() {
                return Err(WeightedRoutingError::EmptyName);
            }
            if !names.insert(&target.name) {
                return Err(WeightedRoutingError::DuplicateTarget(target.name.clone()));
            }
            total = total.saturating_add(usize::from(target.weight));
            cumulative.push(total);
        }
        if total != 100 {
            return Err(WeightedRoutingError::InvalidTotal { total });
        }
        Ok(Self {
            targets,
            cumulative: cumulative.into_boxed_slice(),
        })
    }

    /// Select a backend label without allocation, accepting any key bytes.
    #[must_use]
    pub fn select(&self, key: &[u8]) -> &str {
        let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
        let bucket = hash % 100;
        // Validation guarantees a final threshold of 100, greater than bucket.
        let index = self
            .cumulative
            .partition_point(|&threshold| (threshold as u64) <= bucket);
        &self.targets[index].name
    }

    /// Iterate labels in configured order, including zero-weight targets.
    ///
    /// Register each label with `GraphBuilder::route` to keep graph declarations
    /// aligned with this policy. Graph validation checks the referenced services.
    pub fn targets(&self) -> impl Iterator<Item = &str> {
        self.targets.iter().map(|target| target.name.as_str())
    }
}
