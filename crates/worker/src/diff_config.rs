//! Mirror config diff — used by the worker's hot-reload (`Reload` command).
//!
//! Mirrors Go's `worker/config_diff.go`.
//!
//! Produces a sequence of `MirrorCfgTrans` that, when applied to `old_list`,
//! yields a list equivalent to `new_list`. Algorithm: merge-sort over
//! name-sorted lists, O(n+m).

use crate::config::MirrorConfig;

/// Operation kind in a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffOp {
    Add,
    Delete,
    Modify,
}

/// One diff unit — an operation + the config it applies to.
#[derive(Debug, Clone)]
pub struct MirrorCfgTrans {
    pub op: DiffOp,
    pub config: MirrorConfig,
}

impl std::fmt::Display for MirrorCfgTrans {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self.op {
            DiffOp::Add => "Add",
            DiffOp::Delete => "Del",
            DiffOp::Modify => "Mod",
        };
        write!(f, "{{{label}, {}}}", self.config.name)
    }
}

/// Compute the difference between `old_list` and `new_list`.
///
/// Returns a list of operations that transform `old_list` into `new_list`.
/// Mirrors Go's `diffMirrorConfig` exactly.
pub fn diff_mirror_config(
    old_list: &[MirrorConfig],
    new_list: &[MirrorConfig],
) -> Vec<MirrorCfgTrans> {
    let mut ops = Vec::new();

    let mut old = old_list.to_vec();
    let mut new = new_list.to_vec();
    old.sort_by(|a, b| a.name.cmp(&b.name));
    new.sort_by(|a, b| a.name.cmp(&b.name));

    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        // Determine which side is "smaller" (or only one side remains).
        let cmp = match (old.get(i), new.get(j)) {
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (Some(o), Some(n)) => o.name.cmp(&n.name),
            (None, None) => break,
        };

        match cmp {
            std::cmp::Ordering::Greater => {
                // new[j] not in old → Add
                ops.push(MirrorCfgTrans {
                    op: DiffOp::Add,
                    config: new[j].clone(),
                });
                j += 1;
            }
            std::cmp::Ordering::Less => {
                // old[i] not in new → Delete
                ops.push(MirrorCfgTrans {
                    op: DiffOp::Delete,
                    config: old[i].clone(),
                });
                i += 1;
            }
            std::cmp::Ordering::Equal => {
                // Same name — check for modification via PartialEq.
                if !mirrors_equal(&old[i], &new[j]) {
                    ops.push(MirrorCfgTrans {
                        op: DiffOp::Modify,
                        config: new[j].clone(),
                    });
                }
                i += 1;
                j += 1;
            }
        }
    }

    ops
}

/// Field-by-field equality for the config fields that affect provider
/// construction. Mirrors Go's `reflect.DeepEqual`.
///
/// We compare only the fields that, if changed, require the provider to be
/// rebuilt. This is a conservative superset — any field change triggers a
/// Modify, which is safe (just causes a brief re-schedule).
fn mirrors_equal(a: &MirrorConfig, b: &MirrorConfig) -> bool {
    // Serialize both to JSON and compare byte-for-byte.
    // This is equivalent to Go's reflect.DeepEqual and is zero-boilerplate.
    serde_json::to_vec(a).ok() == serde_json::to_vec(b).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mc(name: &str) -> MirrorConfig {
        MirrorConfig {
            name: name.into(),
            ..Default::default()
        }
    }

    fn mc_with_upstream(name: &str, up: &str) -> MirrorConfig {
        MirrorConfig {
            name: name.into(),
            upstream: up.into(),
            ..Default::default()
        }
    }

    #[test]
    fn add_new() {
        let old = vec![mc("a"), mc("b")];
        let new = vec![mc("a"), mc("b"), mc("c")];
        let diff = diff_mirror_config(&old, &new);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].op, DiffOp::Add);
        assert_eq!(diff[0].config.name, "c");
    }

    #[test]
    fn delete_old() {
        let old = vec![mc("a"), mc("b"), mc("c")];
        let new = vec![mc("a"), mc("c")];
        let diff = diff_mirror_config(&old, &new);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].op, DiffOp::Delete);
        assert_eq!(diff[0].config.name, "b");
    }

    #[test]
    fn modify() {
        let old = vec![mc_with_upstream("ubuntu", "rsync://old/")];
        let new = vec![mc_with_upstream("ubuntu", "rsync://new/")];
        let diff = diff_mirror_config(&old, &new);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].op, DiffOp::Modify);
    }

    #[test]
    fn no_change() {
        let list = vec![mc("a"), mc("b")];
        assert!(diff_mirror_config(&list, &list).is_empty());
    }

    #[test]
    fn unsorted_input_works() {
        let old = vec![mc("c"), mc("a")];
        let new = vec![mc("a"), mc("b"), mc("c")];
        let diff = diff_mirror_config(&old, &new);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].op, DiffOp::Add);
        assert_eq!(diff[0].config.name, "b");
    }
}
