
----------- INSERT DWH.l_user_group_activity 
----------- FROM   DWH.h_users, DWH.h_groups

INSERT INTO STV2023070319__DWH.l_user_group_activity(
	hk_l_user_group_activity, 
	hk_user_id,
	hk_group_id,
	load_dt,
	load_src)
SELECT DISTINCT
	hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() AS load_dt,
	's3'  AS load_src
FROM      STV2023070319__STAGING.group_log AS gl
LEFT JOIN STV2023070319__DWH.h_users       AS hu ON gl.user_id = hu.user_id 
LEFT JOIN STV2023070319__DWH.h_groups      AS hg ON gl.group_id = hg.group_id
WHERE hash(hu.hk_user_id, hg.hk_group_id)
	NOT IN (SELECT hk_l_user_group_activity FROM STV2023070319__DWH.l_user_group_activity)
; 



--- INSERT DWH.s_auth_history
--- FROM STAGING.group_log, DWH.l_user_group_activity

INSERT INTO STV2023070319__DWH.s_auth_history(
	hk_l_user_group_activity, 
	user_id_from,
	event,
	event_dt,
	load_dt,
	load_src
	)
SELECT
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl."datetime" AS event_dt,
	now()         AS load_dt,
	's3'          AS load_src
FROM STV2023070319__STAGING.group_log              AS gl
LEFT JOIN STV2023070319__DWH.h_groups              AS hg   ON gl.group_id = hg.group_id
LEFT JOIN STV2023070319__DWH.h_users               AS hu   ON gl.user_id = hu.user_id
LEFT JOIN STV2023070319__DWH.l_user_group_activity AS luga ON hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
; 