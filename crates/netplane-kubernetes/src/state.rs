use kube::{ResourceExt, runtime::watcher::Event};
use std::collections::BTreeMap;

pub struct ResourceCache<K> {
    pub committed: BTreeMap<(String, String), K>,
    staging: Option<BTreeMap<(String, String), K>>,
    initialized: bool,
}
impl<K> Default for ResourceCache<K> {
    fn default() -> Self {
        Self {
            committed: BTreeMap::new(),
            staging: None,
            initialized: false,
        }
    }
}
impl<K: ResourceExt + PartialEq> ResourceCache<K> {
    pub const fn synchronized(&self) -> bool {
        self.initialized && self.staging.is_none()
    }
    pub fn apply(&mut self, event: Event<K>) -> bool {
        match event {
            Event::Init => {
                self.staging = Some(BTreeMap::new());
                false
            }
            Event::InitApply(value) => {
                if let Some(staging) = &mut self.staging {
                    staging.insert(
                        (value.namespace().unwrap_or_default(), value.name_any()),
                        value,
                    );
                }
                false
            }
            Event::InitDone => {
                if let Some(staging) = self.staging.take() {
                    self.committed = staging;
                    self.initialized = true;
                    return true;
                }
                false
            }
            Event::Apply(value) => {
                let key = (value.namespace().unwrap_or_default(), value.name_any());
                if self.committed.get(&key) == Some(&value) {
                    return false;
                }
                self.committed.insert(key, value);
                true
            }
            Event::Delete(value) => {
                let key = (value.namespace().unwrap_or_default(), value.name_any());
                if self
                    .committed
                    .get(&key)
                    .is_some_and(|old| old.uid() == value.uid())
                {
                    self.committed.remove(&key);
                    return true;
                }
                false
            }
        }
    }
}
