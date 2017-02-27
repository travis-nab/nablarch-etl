SELECT_INPUT_FILE2=
SELECT
  col1,
  col2,
  col3
FROM
  input_file2_table

SELECT_OUTPUT_TABLE2_FROM_INPUT_FILE3=
SELECT
  user_id as col1,
  col2,
  cast(col3 as numeric)
FROM
  input_file3_table