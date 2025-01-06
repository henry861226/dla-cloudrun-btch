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
    `{{ dataset }}.TRANS`
UNPIVOT (
    value FOR tag_name IN (
        MY_NEWPARENT,
        MY_ELDERLY,
        MY_LANDLORD,
        MY_SOCIALITE,
        MY_ENTREPRENEUR,
        MY_HWHITECOLLAR,
        MY_WUSHIH,
        MY_PUBLICSERVANT,
        MY_FINANCEELITE, 
        MY_TECHELITE,
        MY_VIPHEIR,
        MY_VIPRELATIVE,
        MY_LOWSPENDING,
        I_DONTMKT,
        I_MKTBLACKLIST,
        I_NOINSTALLMENT,
        I_SALARYACCT,
        I_CCPREAPPROVE,
        I_FA,
        P_CASHADVANCE,
        P_INSTALLMENT,
        P_REVOLVINGCREDIT,
        P_CUBECARD,
        P_CUBECARD_NA,
        P_WORLDCARD,
        P_WORLDCARD_NA,
        P_STOCKTRX,
        P_POTENTIALBORROWER,
        P_LINELINKED,
        CALL_CCTRXLIST,
        CALL_CCLOST,
        CALL_CCTRXDISPUTE,
        CALL_CCBILL,
        CALL_CCLIMIT,
        CALL_CCLIMITRAISE,
        CALL_NOLEVEL4RECORD,
        I_YOUTH,
        I_VIP,
        P_XYZCouponLTD,
        MY_ECSHOPPER,
        MY_GAMING,
        MY_3C,
        MY_FOODIE,
        MY_CVS,
        MY_HYPERMARKET,
        MY_SHOPPINGMALL,
        MY_BUDGETITEM,
        MY_TRAVELER,
        MY_PRICESENSITIVE,
        MY_MILEAGE,
        MY_HIGHSPENDING,
        MY_POTENTIAL_FUND,
        MY_POTENTIAL_WHOLELIFEINSURANCE,
        MY_POTENTIAL_INVESTINSURANCE,
        MY_POTENTIAL_GENERALINSURANCE,
        MY_POTENTIAL_IFX,
        MY_POTENTIAL_IFXFD,
        MY_POTENTIAL_INSTALLMENT,
        MY_POTENTIAL_MOBILEPAYMENT,
        MY_TWDSTEADYINCOME,
        MY_LARGEINCOMECURRENTMONTH,
        MY_TWDINCOMEINCREASE,
        MY_TWDINCOMEDECREASE,
        MY_FUNDINVESTINCREASE,
        MY_FUNDINVESTDECREASE,
        MY_HIGHDIVIDEND,
        MY_HIGHFREQTRADING
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
WHERE t2.enabled=true AND t1.value >= t2.threshold
)
-- [ADD TAG_NAME TO CUS_TAG TB]
SELECT
    cust_uuid,
    ARRAY_AGG(tag_name) AS tags  -- 使用 ARRAY_AGG 將較大的 tag_name 聚合成一個陣列
FROM
    tag_comparison
GROUP BY
    cust_uuid
