CREATE OR REPLACE EXTERNAL TABLE `{{ projectId }}.{{ dataset }}.GROUP_NEW_TMP`
(
  group_uuid STRING,
  tags STRING,
  compliance STRING,
  prompt STRING,
  description STRING,
  marketing_copy STRING
)
OPTIONS (
  format = 'CSV',  -- 數據格式（支援 CSV、JSON、Avro 等）
  uris = ['gs://{{ gcs_bucket }}/{{ group_file }}'],
  skip_leading_rows = 1
);

CREATE OR REPLACE TABLE `{{ projectId }}.{{ dataset }}.GROUP_NEW`
(
  group_uuid STRING,
  tags ARRAY<STRING>,
  compliance STRING,
  prompt STRING,
  description STRING,
  marketing_copy STRING
)
AS
SELECT 
  group_uuid,
  SPLIT(REPLACE(REPLACE(REPLACE(tags, '[', ''), ']', ''), "'", ""), ",") AS tags,
  compliance, prompt, description, marketing_copy
FROM `{{ projectId }}.{{ dataset }}.GROUP_NEW_TMP`;

DROP TABLE `{{ projectId }}.{{ dataset }}.GROUP_NEW_TMP`;