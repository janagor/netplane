use crate::Capacity;

/// The supported kind of a declared node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    /// A Tower service executed by a bounded endpoint.
    Service,
    /// A request selector with declared routes and a fallback.
    Router,
}
/// One named graph node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeMetadata {
    /// Unique node identifier.
    pub name: String,
    /// Node kind.
    pub kind: NodeKind,
}
/// One declared route from a router to a service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteMetadata {
    /// Selector label.
    pub label: String,
    /// Target service name.
    pub target: String,
}
/// The effective endpoint capacity of one named backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendCapacityMetadata {
    /// Registered backend name.
    pub name: String,
    /// Capacity used by this backend's endpoint.
    pub capacity: Capacity,
}
/// Deterministic, read-only description of one graph version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphMetadata {
    /// Entry node name.
    pub entry: String,
    /// Nodes sorted by name.
    pub nodes: Vec<NodeMetadata>,
    /// Routes sorted by label.
    pub routes: Vec<RouteMetadata>,
    /// Explicit fallback target, if a router exists.
    pub fallback: Option<String>,
    /// Capacity of the outer version endpoint.
    pub capacity: Capacity,
    /// Configured default backend capacity, if a router exists.
    pub backend_capacity: Option<Capacity>,
    /// Effective backend capacities sorted by name; empty for a single root.
    pub backend_capacities: Vec<BackendCapacityMetadata>,
}

impl GraphMetadata {
    pub(crate) fn single_root(capacity: Capacity) -> Self {
        Self {
            entry: "root".into(),
            nodes: vec![NodeMetadata {
                name: "root".into(),
                kind: NodeKind::Service,
            }],
            routes: Vec::new(),
            fallback: None,
            capacity,
            backend_capacity: None,
            backend_capacities: Vec::new(),
        }
    }
}
