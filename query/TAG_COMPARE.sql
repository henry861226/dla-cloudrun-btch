CREATE OR REPLACE TABLE `{{ projectId}}.{{ dataset }}.CUST_TAGS_{{ date }}`
(
cust_uuid STRING,
tags ARRAY<STRING>
)
AS
-- [SHOW CUST TAG_NAME AND VALUE]
WITH cus_tag_tb AS (
SELECT
    cust_uuid,
    tag_name,
    value
FROM
    `{{ dataset }}.EXTERNAL_CSV`
UNPIVOT (
    value FOR tag_name IN (
    tag_name1, tag_name2, tag_name3, tag_name4, tag_name5,tag_name6, tag_name7, tag_name8, tag_name9, tag_name10,
    tag_name11, tag_name12, tag_name13, tag_name14, tag_name15,tag_name16, tag_name17, tag_name18, tag_name19, tag_name20,
    tag_name21, tag_name22, tag_name23, tag_name24, tag_name25,tag_name26, tag_name27, tag_name28, tag_name29, tag_name30,
    tag_name31, tag_name32, tag_name33, tag_name34, tag_name35,tag_name36, tag_name37, tag_name38, tag_name39, tag_name40,
    tag_name41, tag_name42, tag_name43, tag_name44, tag_name45,tag_name46, tag_name47, tag_name48, tag_name49, tag_name50
    )
)
),
-- [COMPARE TAG THRESHOLD]
tag_comparison AS (
SELECT
    t1.cust_uuid,
    t1.tag_name,
    t1.value AS table1_value,
    t2.threshold AS table2_value
FROM
    cus_tag_tb AS t1
JOIN
    `{{ dataset }}.TAG_META` AS t2
ON
    t1.tag_name = t2.tag_name
WHERE t2.enabled=true AND t1.value > t2.threshold
)
-- [ADD TAG_NAME TO CUS_TAG TB]
SELECT
    cust_uuid,
    ARRAY_AGG(tag_name) AS tags  -- 使用 ARRAY_AGG 將較大的 tag_name 聚合成一個陣列
FROM
    tag_comparison
GROUP BY
    cust_uuid
