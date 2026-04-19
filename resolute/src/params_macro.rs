/// Build a positional parameter slice for runtime `query` / `execute` calls.
///
/// Equivalent to `&[&x, &y, ...]` passed to an API that expects
/// `&[&dyn SqlParam]`. Rust coerces `&T` to `&dyn SqlParam` at the slice site
/// when the target type is known, so the raw form usually works without any
/// cast. This macro is a convenience for call sites where inference is
/// ambiguous (empty slices, generic code, values mixed with `None`) and for
/// readers who prefer explicit intent. Values that do not implement
/// `SqlParam` fail to compile.
///
/// # Example
///
/// ```ignore
/// use resolute::params;
///
/// let rows = client
///     .query("SELECT * FROM users WHERE org = $1 AND id = $2", params![org_id, user_id])
///     .await?;
/// ```
#[macro_export]
macro_rules! params {
    () => {
        (&[] as &[&dyn $crate::SqlParam])
    };
    ($($value:expr),+ $(,)?) => {
        (&[
            $(&$value as &dyn $crate::SqlParam),+
        ] as &[&dyn $crate::SqlParam])
    };
}

/// Build a named parameter slice for `query_named` / `execute_named`.
///
/// Same rationale as [`params!`]: Rust coerces per-tuple-element when the
/// target type is known, so `&[("a", &x), ("b", &y)]` usually works without
/// any cast. This macro is a convenience for ambiguous call sites and
/// readers who prefer explicit intent. Values that do not implement
/// `SqlParam` fail to compile.
///
/// # Example
///
/// ```ignore
/// use resolute::params_named;
///
/// let rows = client
///     .query_named(
///         "SELECT * FROM users WHERE org = :org AND id = :id",
///         params_named![("org", org_id), ("id", user_id)],
///     )
///     .await?;
/// ```
#[macro_export]
macro_rules! params_named {
    () => {
        (&[] as &[(&str, &dyn $crate::SqlParam)])
    };
    ($(($name:expr, $value:expr)),+ $(,)?) => {
        (&[
            $(($name, &$value as &dyn $crate::SqlParam)),+
        ] as &[(&str, &dyn $crate::SqlParam)])
    };
}

#[cfg(test)]
mod tests {
    use crate as resolute;
    use crate::SqlParam;

    #[test]
    fn params_empty() {
        let p: &[&dyn SqlParam] = resolute::params![];
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn params_mixed_types() {
        let id: i32 = 42;
        let name: String = "alice".into();
        let active: bool = true;
        let p: &[&dyn SqlParam] = resolute::params![id, name, active];
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn params_trailing_comma() {
        let id: i32 = 1;
        let p: &[&dyn SqlParam] = resolute::params![id,];
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn params_named_empty() {
        let p: &[(&str, &dyn SqlParam)] = resolute::params_named![];
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn params_named_mixed_types() {
        let org: i32 = 7;
        let id: i64 = 99;
        let p: &[(&str, &dyn SqlParam)] = resolute::params_named![("org", org), ("id", id)];
        assert_eq!(p.len(), 2);
        assert_eq!(p[0].0, "org");
        assert_eq!(p[1].0, "id");
    }

    #[test]
    fn params_named_trailing_comma() {
        let id: i32 = 1;
        let p: &[(&str, &dyn SqlParam)] = resolute::params_named![("id", id),];
        assert_eq!(p.len(), 1);
    }
}
