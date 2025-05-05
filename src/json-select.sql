
SELECT _hash_col, key_col,  COUNT(key_value) AS key_count FROM
(SELECT _hash_col, key_col, json_each.value AS key_value
FROM tableCfgs_tbl,
json_each(columns_col)
WHERE json_each.value IS NOT NULL) as cols
GROUP BY _hash_col, key_col;


SELECT key_col, COUNT(key_value) AS key_count FROM
(SELECT key_col, json_each.value AS key_value
FROM tableCfgs_tbl,
json_each(columns_col)
WHERE json_each.value IS NOT NULL) as cols
GROUP BY key_col;

