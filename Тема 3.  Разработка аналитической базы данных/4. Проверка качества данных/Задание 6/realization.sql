SELECT count(*)
FROM STV2023070319__STAGING.groups AS g
LEFT JOIN STV2023070319__STAGING.users AS u ON g.admin_id = u.id 
WHERE g.admin_id <> u.id
;