(SELECT MIN(u.registration_dt)       AS datestamp,
        'earliest user registration' AS info
FROM STV2023070319__STAGING.users    AS u)
UNION ALL
(SELECT MAX(u.registration_dt)     AS datestamp,
        'latest user registration' AS info
FROM STV2023070319__STAGING.users  AS u)
UNION ALL
(SELECT MIN(g.registration_dt)     AS datestamp,
        'earliest group creation'  AS info
FROM STV2023070319__STAGING.groups AS g)
UNION ALL
(SELECT MAX(g.registration_dt)     AS datestamp,
        'latest group creation'    AS info
FROM STV2023070319__STAGING.groups AS g)
UNION ALL
(SELECT MIN(d.message_ts)           AS datestamp,
        'earliest dialog message'   AS info
FROM STV2023070319__STAGING.dialogs AS d)
UNION ALL
(SELECT MAX(d.message_ts)           AS datestamp,
        'latest dialog message'     AS info
FROM STV2023070319__STAGING.dialogs AS d)
;