(SELECT 
    max(u.registration_dt) < now()        as 'no future dates',
    min(u.registration_dt) > '2020-09-03' as 'no false-start dates',
    'users'                               as dataset
FROM STV2023070319__STAGING.users u)
UNION ALL
(SELECT
    max(g.registration_dt) < now()        as 'no future dates',
    min(g.registration_dt) > '2020-09-03' as 'no false-start dates',
    'groups' as dataset
FROM STV2023070319__STAGING.groups g)
UNION ALL
(SELECT
    max(d.message_ts) < now()        as 'no future dates',
    min(d.message_ts) > '2020-09-03' as 'no false-start dates',
    'dialogs' as dataset
FROM STV2023070319__STAGING.dialogs d); 