SELECT 
	count(*) AS all_rows,
	count(DISTINCT id) AS id_unique
FROM STV2023070319__STAGING.users;