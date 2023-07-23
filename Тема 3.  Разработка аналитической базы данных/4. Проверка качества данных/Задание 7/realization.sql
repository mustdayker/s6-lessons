(SELECT count(1), 'missing group admin info' as info
FROM      STV2023070319__STAGING.groups AS g
LEFT JOIN STV2023070319__STAGING.users  AS u ON g.admin_id = u.id 
WHERE g.admin_id <> u.id)
UNION ALL
(SELECT COUNT(1), 'missing sender info'  AS info
FROM      STV2023070319__STAGING.dialogs AS d 
LEFT JOIN STV2023070319__STAGING.users   AS u ON d.message_from = u.id 
WHERE d.message_from <> u.id)
UNION ALL
(SELECT COUNT(1), 'missing receiver info' AS info
FROM      STV2023070319__STAGING.dialogs  AS d 
LEFT JOIN STV2023070319__STAGING.users    AS u ON d.message_to = u.id 
WHERE d.message_to <> u.id)
UNION ALL 
(SELECT COUNT(1), 'norm receiver info'   AS info
FROM      STV2023070319__STAGING.dialogs AS d 
LEFT JOIN STV2023070319__STAGING.users   AS u ON d.message_to  = u.id 
WHERE d.message_to = u.id);
