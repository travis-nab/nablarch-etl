SELECT_ALL=
select cast(id as numeric ) as id, mail_address, name from etl_merge_multi_key_work


SELECT_ALL_WITH_RANGE=
select cast(id as numeric ) as id, mail_address, name from etl_merge_multi_key_work
  where line_number between ? and ?
