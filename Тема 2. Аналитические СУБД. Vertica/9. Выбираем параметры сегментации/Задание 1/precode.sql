Прекод
create table STV2023070319.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES users(id),
    message_to   int REFERENCES users(id),
    message      varchar(1000),
    message_type varchar(100)
)
SEGMENTED BY hash(message_id) all nodes