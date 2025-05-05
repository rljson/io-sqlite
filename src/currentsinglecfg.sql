
WITH versions AS (
  SELECT
    _hash_col,
    key_col,
    MAX(json_each.key) AS max_val
  FROM tableCfgs_tbl,
     json_each(columns_col)
  WHERE json_each.value IS NOT NULL
    AND key_col = 'table0'
  GROUP BY _hash_col, key_col
)
SELECT *
FROM tableCfgs_tbl tt
LEFT JOIN versions
ON tt._hash_col = versions._hash_col
WHERE versions.max_val = (SELECT MAX(max_val) FROM versions);
