//! Named parameter rewriting: `:name` → `$1, $2, ...`
//!
//! PostgreSQL only supports positional `$N` parameters at the wire level.
//! This module rewrites SQL containing `:name` placeholders into positional
//! form, correctly handling `::` casts, string literals, dollar-quoted strings,
//! quoted identifiers, and comments.
//!
//! Duplicate names reuse the same positional index:
//! ```text
//! "SELECT :id WHERE :id > 0"  →  "SELECT $1 WHERE $1 > 0", ["id"]
//! ```

use std::collections::HashMap;

/// Rewrite `:name` named params to `$N` positional params.
///
/// Returns `(rewritten_sql, ordered_param_names)` where `ordered_param_names[i]`
/// is the name that corresponds to `$i+1`.
pub fn rewrite(sql: &str) -> (String, Vec<String>) {
    let mut result = String::with_capacity(sql.len());
    let mut names: Vec<String> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // -- Single-line comment: skip to end of line.
        if i + 1 < len && chars[i] == '-' && chars[i + 1] == '-' {
            while i < len && chars[i] != '\n' {
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // /* Block comment */: skip to closing */.
        if i + 1 < len && chars[i] == '/' && chars[i + 1] == '*' {
            result.push('/');
            result.push('*');
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                result.push(chars[i]);
                i += 1;
            }
            if i + 1 < len {
                result.push('*');
                result.push('/');
                i += 2;
            }
            continue;
        }

        // 'String literal' with '' escaping.
        if chars[i] == '\'' {
            result.push('\'');
            i += 1;
            while i < len {
                result.push(chars[i]);
                if chars[i] == '\'' {
                    if i + 1 < len && chars[i + 1] == '\'' {
                        result.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // "Quoted identifier": skip contents.
        if chars[i] == '"' {
            result.push('"');
            i += 1;
            while i < len {
                result.push(chars[i]);
                if chars[i] == '"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // $tag$Dollar-quoted string$tag$: skip contents.
        if chars[i] == '$' {
            // Try to parse a dollar-quote tag: $[tag]$
            let tag_start = i;
            i += 1; // skip first $
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            if i < len && chars[i] == '$' {
                // Found opening $tag$
                let tag: String = chars[tag_start..=i].iter().collect();
                for c in tag.chars() {
                    result.push(c);
                }
                i += 1;
                // Find closing $tag$
                loop {
                    if i >= len {
                        break;
                    }
                    if chars[i] == '$' {
                        let remaining: String = chars[i..].iter().collect();
                        if remaining.starts_with(&tag) {
                            for c in tag.chars() {
                                result.push(c);
                            }
                            i += tag.len();
                            break;
                        }
                    }
                    result.push(chars[i]);
                    i += 1;
                }
                continue;
            } else {
                // Not a dollar-quote tag, it's a positional param like $1.
                // Rewind and emit as-is.
                i = tag_start;
                result.push(chars[i]);
                i += 1;
                continue;
            }
        }

        // :: cast operator: pass through.
        if chars[i] == ':' && i + 1 < len && chars[i + 1] == ':' {
            result.push(':');
            result.push(':');
            i += 2;
            continue;
        }

        // :name — named parameter.
        if chars[i] == ':'
            && i + 1 < len
            && (chars[i + 1].is_alphabetic() || chars[i + 1] == '_')
        {
            i += 1; // skip ':'
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let name: String = chars[start..i].iter().collect();

            let pos = if let Some(&existing) = positions.get(&name) {
                existing
            } else {
                names.push(name.clone());
                let pos = names.len();
                positions.insert(name, pos);
                pos
            };
            result.push('$');
            result.push_str(&pos.to_string());
            continue;
        }

        // Regular character.
        result.push(chars[i]);
        i += 1;
    }

    (result, names)
}

/// Check if SQL contains any `:name` named parameters (not `::` casts).
/// Quick heuristic check — doesn't do full parsing.
pub fn has_named_params(sql: &str) -> bool {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;
    while i < len {
        if chars[i] == '\'' {
            i += 1;
            while i < len {
                if chars[i] == '\'' {
                    if i + 1 < len && chars[i + 1] == '\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if chars[i] == ':' && i + 1 < len && chars[i + 1] == ':' {
            i += 2;
        } else if chars[i] == ':'
            && i + 1 < len
            && (chars[i + 1].is_alphabetic() || chars[i + 1] == '_')
        {
            return true;
        } else {
            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_rewrite() {
        let (sql, names) = rewrite("SELECT * FROM users WHERE id = :id AND name = :name");
        assert_eq!(sql, "SELECT * FROM users WHERE id = $1 AND name = $2");
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn test_duplicate_params() {
        let (sql, names) = rewrite("SELECT * FROM t WHERE a = :id OR b = :id");
        assert_eq!(sql, "SELECT * FROM t WHERE a = $1 OR b = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_cast_preserved() {
        let (sql, names) = rewrite("SELECT :value::int4");
        assert_eq!(sql, "SELECT $1::int4");
        assert_eq!(names, vec!["value"]);
    }

    #[test]
    fn test_double_cast_no_param() {
        let (sql, names) = rewrite("SELECT 1::int4::text");
        assert_eq!(sql, "SELECT 1::int4::text");
        assert!(names.is_empty());
    }

    #[test]
    fn test_string_literal_skipped() {
        let (sql, names) = rewrite("SELECT ':not_a_param' WHERE id = :id");
        assert_eq!(sql, "SELECT ':not_a_param' WHERE id = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_escaped_string_literal() {
        let (sql, names) = rewrite("SELECT 'it''s :fine' WHERE id = :id");
        assert_eq!(sql, "SELECT 'it''s :fine' WHERE id = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_quoted_identifier_skipped() {
        let (sql, names) = rewrite(r#"SELECT ":not_a_param" WHERE id = :id"#);
        assert_eq!(sql, r#"SELECT ":not_a_param" WHERE id = $1"#);
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_line_comment_skipped() {
        let (sql, names) = rewrite("SELECT :id -- :not_a_param\nFROM t");
        assert_eq!(sql, "SELECT $1 -- :not_a_param\nFROM t");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_block_comment_skipped() {
        let (sql, names) = rewrite("SELECT :id /* :not_a_param */ FROM t");
        assert_eq!(sql, "SELECT $1 /* :not_a_param */ FROM t");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_dollar_quoted_skipped() {
        let (sql, names) = rewrite("SELECT $$ :not_a_param $$ WHERE id = :id");
        assert_eq!(sql, "SELECT $$ :not_a_param $$ WHERE id = $1");
        assert_eq!(names, vec!["id"]);
    }

    #[test]
    fn test_no_params() {
        let (sql, names) = rewrite("SELECT 1::int4");
        assert_eq!(sql, "SELECT 1::int4");
        assert!(names.is_empty());
    }

    #[test]
    fn test_underscore_param() {
        let (sql, names) = rewrite("SELECT :_private, :my_param");
        assert_eq!(sql, "SELECT $1, $2");
        assert_eq!(names, vec!["_private", "my_param"]);
    }

    #[test]
    fn test_mixed_positional_preserved() {
        // If user mixes $1 and :name, positional $1 passes through
        let (sql, names) = rewrite("SELECT $1, :name");
        assert_eq!(sql, "SELECT $1, $1");
        assert_eq!(names, vec!["name"]);
    }

    #[test]
    fn test_has_named_params_true() {
        assert!(has_named_params("SELECT :id"));
        assert!(has_named_params("SELECT :id::int4"));
    }

    #[test]
    fn test_has_named_params_false() {
        assert!(!has_named_params("SELECT $1"));
        assert!(!has_named_params("SELECT 1::int4"));
        assert!(!has_named_params("SELECT ':nope'"));
    }

    #[test]
    fn test_many_params() {
        let (sql, names) = rewrite("INSERT INTO t (a, b, c) VALUES (:a, :b, :c)");
        assert_eq!(sql, "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)");
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
