SELECT age, count(1)
FROM STV2023070319__DWH.s_user_socdem
WHERE hk_user_id IN (SELECT hk_user_id
                     FROM STV2023070319__DWH.l_user_message
                     WHERE hk_message_id IN (SELECT hk_message_id
                                             FROM STV2023070319__DWH.l_groups_dialogs
                                             WHERE hk_group_id IN (SELECT hk_group_id
                                                                   FROM STV2023070319__DWH.h_groups
                                                                   ORDER BY registration_dt limit 10)))
GROUP BY age
ORDER BY 2 DESC
;
