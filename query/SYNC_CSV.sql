CREATE OR REPLACE EXTERNAL TABLE `{{ projectId }}.{{ dataset }}.EXTERNAL_CSV`
(
  cust_uuid STRING,
  tag_name1 FLOAT64, tag_name2 FLOAT64, tag_name3 FLOAT64, tag_name4 FLOAT64, tag_name5 FLOAT64,
  tag_name6 FLOAT64, tag_name7 FLOAT64, tag_name8 FLOAT64, tag_name9 FLOAT64, tag_name10 FLOAT64,
  tag_name11 FLOAT64, tag_name12 FLOAT64, tag_name13 FLOAT64, tag_name14 FLOAT64, tag_name15 FLOAT64,
  tag_name16 FLOAT64, tag_name17 FLOAT64, tag_name18 FLOAT64, tag_name19 FLOAT64, tag_name20 FLOAT64,
  tag_name21 FLOAT64, tag_name22 FLOAT64, tag_name23 FLOAT64, tag_name24 FLOAT64, tag_name25 FLOAT64,
  tag_name26 FLOAT64, tag_name27 FLOAT64, tag_name28 FLOAT64, tag_name29 FLOAT64, tag_name30 FLOAT64,
  tag_name31 FLOAT64, tag_name32 FLOAT64, tag_name33 FLOAT64, tag_name34 FLOAT64, tag_name35 FLOAT64,
  tag_name36 FLOAT64, tag_name37 FLOAT64, tag_name38 FLOAT64, tag_name39 FLOAT64, tag_name40 FLOAT64,
  tag_name41 FLOAT64, tag_name42 FLOAT64, tag_name43 FLOAT64, tag_name44 FLOAT64, tag_name45 FLOAT64,
  tag_name46 FLOAT64, tag_name47 FLOAT64, tag_name48 FLOAT64, tag_name49 FLOAT64, tag_name50 FLOAT64
)
OPTIONS (
  format = 'CSV',  -- 數據格式（支援 CSV、JSON、Avro 等）
  uris = ['gs://{{ gcs_bucket }}/{{ daily_file }}'],
  skip_leading_rows = 1
);