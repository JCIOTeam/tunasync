//! Layered key-value context — mirrors Go's `worker/context.go`.
//!
//! Each `Enter` creates a new overlay layer; `Exit` pops it.
//! `Get` searches from the top layer downwards.
//! This is used by `loglimit_hook` (to set the current log file path) and
//! `docker_hook` (to inject runtime volume mounts) via the `_LogFileKey`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Well-known key: current log-file path (`"log_file"`).
pub const LOG_FILE_KEY: &str = "log_file";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

type AnyVal = Arc<dyn Any + Send + Sync>;

/// One layer of the context stack.
#[derive(Default)]
struct Layer {
    store: HashMap<String, AnyVal>,
}

/// Layered key-value context.
///
/// Cheap to clone — all layers share the same `Arc<Mutex<...>>` chain.
#[derive(Clone)]
pub struct Context {
    inner: Arc<Mutex<ContextInner>>,
}

struct ContextInner {
    layers: Vec<Layer>,
}

impl Context {
    /// Create a new context with a single root layer.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ContextInner {
                layers: vec![Layer::default()],
            })),
        }
    }

    /// Push a new overlay layer. Returns `self` for chaining.
    pub fn enter(&self) {
        self.inner.lock().unwrap().layers.push(Layer::default());
    }

    /// Pop the top overlay layer.
    pub fn exit(&self) {
        let mut guard = self.inner.lock().unwrap();
        if guard.layers.len() > 1 {
            guard.layers.pop();
        }
    }

    /// Set a value in the **current** (top) layer.
    pub fn set<V: Any + Send + Sync + 'static>(&self, key: &str, value: V) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(layer) = guard.layers.last_mut() {
            layer.store.insert(key.to_owned(), Arc::new(value));
        }
    }

    /// Get a value by searching from the top layer downwards.
    pub fn get<V: Any + Send + Sync + Clone + 'static>(&self, key: &str) -> Option<V> {
        let guard = self.inner.lock().unwrap();
        for layer in guard.layers.iter().rev() {
            if let Some(val) = layer.store.get(key) {
                if let Some(v) = val.downcast_ref::<V>() {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    /// Convenience: get the current log-file path string.
    pub fn log_file(&self) -> Option<String> {
        self.get::<String>(LOG_FILE_KEY)
    }

    /// Convenience: set the current log-file path string.
    pub fn set_log_file(&self, path: &str) {
        self.set(LOG_FILE_KEY, path.to_owned());
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enter_exit_get() {
        let ctx = Context::new();
        ctx.set("x", 1u32);
        assert_eq!(ctx.get::<u32>("x"), Some(1));

        ctx.enter();
        ctx.set("x", 2u32);
        assert_eq!(ctx.get::<u32>("x"), Some(2)); // top layer wins

        ctx.exit();
        assert_eq!(ctx.get::<u32>("x"), Some(1)); // back to root
    }

    #[test]
    fn log_file_convenience() {
        let ctx = Context::new();
        ctx.enter();
        ctx.set_log_file("/var/log/tunasync/ubuntu.log");
        assert_eq!(ctx.log_file().as_deref(), Some("/var/log/tunasync/ubuntu.log"));
        ctx.exit();
        assert_eq!(ctx.log_file(), None);
    }
}
