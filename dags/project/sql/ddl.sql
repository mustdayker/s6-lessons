
------------ DDL STAGING.group_log

DROP TABLE IF EXISTS STV2023070319__STAGING.group_log;

CREATE TABLE STV2023070319__STAGING.group_log(
    group_id     int PRIMARY KEY,
    user_id      int,
    user_id_from int,
    event        varchar(100),
    "datetime"   timestamp
)
ORDER     BY group_id, user_id, "datetime"
SEGMENTED BY hash(group_id) ALL nodes
PARTITION BY "datetime"::date
GROUP     BY calendar_hierarchy_day("datetime"::date, 3, 2)
;


------------ DDL DWH.l_user_group_activity

DROP TABLE IF EXISTS STV2023070319__DWH.l_user_group_activity;

CREATE TABLE STV2023070319__DWH.l_user_group_activity(
    hk_l_user_group_activity  bigint primary key,
    hk_user_id                bigint not null CONSTRAINT fk_l_user_group_activity_user    REFERENCES STV2023070319__DWH.h_users (hk_user_id),
    hk_group_id               bigint not null CONSTRAINT fk_l_user_group_activity_group   REFERENCES STV2023070319__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
ORDER     BY load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP     BY calendar_hierarchy_day(load_dt::date, 3, 2)
;


------------ DDL DWH.s_auth_history

DROP TABLE IF EXISTS STV2023070319__DWH.s_auth_history;

CREATE TABLE STV2023070319__DWH.s_auth_history(
    hk_l_user_group_activity bigint not null, 
    user_id_from int,
    event        varchar(100),
    event_dt     timestamp,
    load_dt      timestamp,
    load_src     varchar(20)
    )
ORDER     BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP     BY calendar_hierarchy_day(load_dt::date, 3, 2)
;


