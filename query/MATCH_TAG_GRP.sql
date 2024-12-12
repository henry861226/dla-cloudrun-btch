CREATE OR REPLACE TABLE `{{ projectId}}.{{ dataset }}.CUST_GRP_MAP_{{ date }}`
AS
WITH tag_matches AS (
SELECT
    u.cust_uuid,
    g.group_uuid,
    ARRAY_LENGTH(
    ARRAY(
        SELECT TRIM(tag)
        FROM UNNEST(u.tags) AS tag
        INTERSECT DISTINCT
        SELECT TRIM(tag)
        FROM UNNEST(g.tags) AS tag
    )
    ) AS tag_overlap,
    u.tags AS uT,
    g.tags AS gT
FROM
    `{{ projectId}}.{{ dataset }}.CUST_TAGS_{{ date }}` u
CROSS JOIN
    `gcs_bq_sync.GROUP_META` g
),
ranked_groups AS (
SELECT
    cust_uuid,
    tag_overlap,
    CASE WHEN tag_overlap = 0 THEN '' ELSE group_uuid END AS group_uuid,
    ROW_NUMBER() OVER (PARTITION BY cust_uuid ORDER BY tag_overlap DESC) AS rn
FROM
    tag_matches
)
SELECT
    cust_uuid,
    group_uuid
FROM
    ranked_groups
WHERE
    rn = 1;
