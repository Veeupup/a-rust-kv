use std::collections::HashMap;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// store.remove("key".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, None);
/// ```
pub struct KvStore {
    kvs: HashMap<String, String>,
}

impl KvStore {
    /// create a kv store
    pub fn new() -> KvStore {
        KvStore {
            kvs: HashMap::new(),
        }
    }

    /// set a kv pair to the store
    pub fn set(&mut self, key: String, val: String) {
        self.kvs.insert(key, val);
    }

    /// get value from kvs by a key
    pub fn get(&mut self, key: String) -> Option<String> {
        // if let Some(val) = self.kvs.get(&key) {
        //     return Some(val.clone());
        // }
        // return None;
        self.kvs.get(&key).cloned() // Option<&T> -> Option<T>
    }

    /// remove a value by a key
    pub fn remove(&mut self, key: String) {
        self.kvs.remove(&key);
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
