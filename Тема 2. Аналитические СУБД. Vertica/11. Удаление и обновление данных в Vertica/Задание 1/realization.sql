SET SESSION AUTOCOMMIT TO off;

DELETE FROM STV2023070319.members WHERE age > 45;

SELECT node_name, projection_name, deleted_row_count 
FROM DELETE_VECTORS
WHERE projection_name like 'members%'
ORDER BY projection_name
;

SELECT SUM(deleted_row_count)
FROM DELETE_VECTORS
where projection_name like 'members%'
GROUP BY projection_name
LIMIT 1
;

ROLLBACK; 