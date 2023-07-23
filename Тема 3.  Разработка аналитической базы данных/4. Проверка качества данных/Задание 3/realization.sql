SELECT 
	count(hash(g.group_name))          AS total,
	count(DISTINCT hash(g.group_name)) AS uniq	
FROM STV2023070319__STAGING.groups AS g
;