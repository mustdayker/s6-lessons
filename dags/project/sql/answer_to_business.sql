
------- CTE user_group_messages

WITH user_group_messages AS (
    SELECT
    	hg.hk_group_id,
   		-- Для подсчета только уникальных пользователей применяем DISTINCT внутри COUNT
		COUNT(DISTINCT d.message_from) AS cnt_users_in_group_with_messages
	-- Количество написавших будем считать в таблице dialogs
	FROM STV2023070319__STAGING.dialogs AS d
	-- Заджойним таблицу h_groups чтобы вытащить оттуда хэши ключей
	LEFT JOIN STV2023070319__DWH.h_groups AS hg ON d.message_group = hg.group_id
	-- Оставим только те строки где есть упоминания групп
	-- Там где значения NULL пользователи писали напрямую друг другу
	WHERE d.message_group IS NOT NULL
	-- Сгруппируем по группам, чтобы подсчитать количество сообщений внутри них
	GROUP BY d.message_group, hg.hk_group_id
)
SELECT 
	hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages AS ugm
ORDER BY cnt_users_in_group_with_messages
; 


------- CTE user_group_log

WITH user_group_log AS (
    SELECT 
    	hg.hk_group_id,
    	-- Для подсчета только уникальных пользователей применяем DISTINCT внутри COUNT
    	count(DISTINCT gl.user_id) AS cnt_added_users
    -- Количество пользователей будем считать в таблице group_log
    FROM STV2023070319__STAGING.group_log AS gl 
    -- Делаем условие, что пользователь вступил самостоятельно
    LEFT JOIN STV2023070319__DWH.h_groups AS hg ON gl.group_id = hg.group_id
    WHERE gl.event = 'add' AND gl.user_id_from IS NULL 
    GROUP BY gl.group_id, hg.hk_group_id, hg.registration_dt
    -- Оставляем только 10 самых ранних созданных групп
    ORDER BY registration_dt ASC
    LIMIT 10
)
SELECT 
	hk_group_id,
	cnt_added_users
FROM user_group_log AS ugl
ORDER BY cnt_added_users
;


-------  ANSWER TO BUSINESS
 
WITH 
user_group_log AS (
    SELECT 
    	hg.hk_group_id,
    	count(DISTINCT gl.user_id) AS cnt_added_users
    FROM STV2023070319__STAGING.group_log AS gl 
    LEFT JOIN STV2023070319__DWH.h_groups AS hg ON gl.group_id = hg.group_id
    WHERE gl.event = 'add' AND gl.user_id_from IS NULL 
    GROUP BY gl.group_id, hg.hk_group_id, hg.registration_dt
    ORDER BY registration_dt ASC
    LIMIT 10
),
user_group_messages AS (
    SELECT
    	hg.hk_group_id,
		COUNT(DISTINCT d.message_from) AS cnt_users_in_group_with_messages
	FROM STV2023070319__STAGING.dialogs AS d
	LEFT JOIN STV2023070319__DWH.h_groups AS hg ON d.message_group = hg.group_id
	WHERE d.message_group IS NOT NULL
	GROUP BY d.message_group, hg.hk_group_id
)
SELECT  
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC 