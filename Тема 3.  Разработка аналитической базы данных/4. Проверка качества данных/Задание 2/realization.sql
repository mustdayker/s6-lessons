SELECT 
	count(*)           AS total,
	count(DISTINCT id) AS uniq,
	'users'            AS dataset	
FROM STV2023070319__STAGING.users
UNION ALL
SELECT 
	count(*)           AS total,
	count(DISTINCT id) AS uniq,
	'groups'           AS dataset	
FROM STV2023070319__STAGING.groups 
UNION ALL
SELECT 
	count(*)                   AS total,
	count(DISTINCT message_id) AS uniq,
	'dialogs'                  AS dataset	
FROM STV2023070319__STAGING.dialogs 
;