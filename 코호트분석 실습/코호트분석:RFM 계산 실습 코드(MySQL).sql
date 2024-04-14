Use Schema1;

-- drop table online_retail_clean;
-- drop table cohort;

# 1.전처리 
CREATE TABLE online_retail_clean AS

SELECT * FROM (
    WITH retails AS (
        SELECT * FROM retail WHERE customerID IS NOT null
        ),
    quantity_unit_price AS (
        SELECT * FROM retail WHERE quantity > 0 AND unitprice > 0
    ),
    dup_check AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY invoiceno, stockcode, quantity 
                               ORDER BY CAST(invoicedate AS DATE)) AS dup 
        FROM quantity_unit_price
    )
    SELECT * FROM dup_check WHERE dup = 1
) AS subquery_alias; # CREATE TABLE AS SELECT 문에서 서브쿼리의 결과를 새 테이블로 생성할 때, 이를 AS 키워드와 함께 별칭을 지정하여 사용합니다.

#2. Cohort Analysis 
SELECT customerid, MIN(cast(invoicedate AS date)) AS first_purchase_date,
	DATE_FORMAT(MIN(CAST(invoicedate AS DATE)), '%Y-%m-01') AS Cohort_Date
FROM online_retail_clean
GROUP BY customerid;

#3 create a second temp table based on our first cohort analysis query and name it #cohort.

CREATE TABLE cohort AS
SELECT * FROM(
SELECT customerid, MIN(CAST(invoiceDate AS Date)) AS first_purchase_date,
	DATE_FORMAT(MIN(CAST(invoicedate AS DATE)), '%Y-%m-01')	AS Cohort_Date
FROM online_retail_clean
GROUP BY customerID)
AS subquery_alias;

#4 Create Cohort Index
SELECT 
	c.cohort_date,
	year(CAST(o.InvoiceDate AS DATE)) AS invoice_year,
    month(CAST(o.InvoiceDate AS DATE)) AS invoice_month,
	year(CAST(c.cohort_date AS DATE)) AS cohort_year,
    month(CAST(c.cohort_date AS DATE)) AS cohort_month
FROM online_retail_clean AS o
LEFT JOIN cohort AS c
on o.Customerid = c.customerid;


#5. calculate the year and month difference between the first purchase date and the cohort date.

WITH CTE AS (
SELECT 
	c.cohort_date,
	year(CAST(o.InvoiceDate AS DATE)) AS invoice_year,
    month(CAST(o.InvoiceDate AS DATE)) AS invoice_month,
	year(CAST(c.cohort_date AS DATE)) AS cohort_year,
    month(CAST(c.cohort_date AS DATE)) AS cohort_month
FROM online_retail_clean AS o
LEFT JOIN cohort AS c
on o.Customerid = c.customerid
) 
SELECT *, (invoice_year-cohort_year) AS year_diff,
(invoice_month - cohort_month) AS month_diff
FROM CTE;

#6. cohort_index = (year_difference * 12) + month_difference + 1, 
#   cohorts_retention 테이블에 저장 
CREATE TABLE cohorts_retention AS 
SELECT * FROM( 
WITH CTE AS (
SELECT 
	o.*,
	c.cohort_date,
	year(CAST(o.InvoiceDate AS DATE)) AS invoice_year,
    month(CAST(o.InvoiceDate AS DATE)) AS invoice_month,
	year(CAST(c.cohort_date AS DATE)) AS cohort_year,
    month(CAST(c.cohort_date AS DATE)) AS cohort_month
FROM online_retail_clean AS o
LEFT JOIN cohort AS c
on o.Customerid = c.customerid
), 
CTE2 AS(
SELECT 
	CTE.*,
	(invoice_year - cohort_year) AS year_diff, 
    (invoice_month - cohort_month) AS month_diff
FROM CTE
)
SELECT cte2.*, year_diff * 12 + month_diff + 1 AS cohort_index 
FROM CTE2 
) AS a;

#7. pivot 
CREATE TABLE cohort_pivot AS
SELECT
    cohort_date,
    COUNT(CASE WHEN cohort_index = 1 THEN customerid ELSE NULL END) AS '1',
    COUNT(CASE WHEN cohort_index = 2 THEN customerid ELSE NULL END) AS '2',
    COUNT(CASE WHEN cohort_index = 3 THEN customerid ELSE NULL END) AS '3',
    COUNT(CASE WHEN cohort_index = 4 THEN customerid ELSE NULL END) AS '4',
    COUNT(CASE WHEN cohort_index = 5 THEN customerid ELSE NULL END) AS '5',
    COUNT(CASE WHEN cohort_index = 6 THEN customerid ELSE NULL END) AS '6',
    COUNT(CASE WHEN cohort_index = 7 THEN customerid ELSE NULL END) AS '7',
    COUNT(CASE WHEN cohort_index = 8 THEN customerid ELSE NULL END) AS '8',
    COUNT(CASE WHEN cohort_index = 9 THEN customerid ELSE NULL END) AS '9',
    COUNT(CASE WHEN cohort_index = 10 THEN customerid ELSE NULL END) AS '10',
    COUNT(CASE WHEN cohort_index = 11 THEN customerid ELSE NULL END) AS '11',
    COUNT(CASE WHEN cohort_index = 12 THEN customerid ELSE NULL END) AS '12',
    COUNT(CASE WHEN cohort_index = 13 THEN customerid ELSE NULL END) AS '13'
FROM
    cohorts_retention
GROUP BY
    cohort_date
ORDER BY
    cohort_date;

SELECT * FROM cohort_pivot;

# RETENTION 
SELECT 
	cohort_date,
    100.0 * `1` / `1` AS `1`,
    100.0 * `2` / `1` AS `2`,
    100.0 * `3` / `1` AS `3`,
    100.0 * `4` / `1` AS `4`,
    100.0 * `5` / `1` AS `5`,
    100.0 * `6` / `1` AS `6`,
    100.0 * `7` / `1` AS `7`,
    100.0 * `8` / `1` AS `8`,
    100.0 * `9` / `1` AS `9`,
    100.0 * `10` / `1` AS `10`,
    100.0 * `11` / `1` AS `11`,
    100.0 * `12` / `1` AS `12`,
    100.0 * `13` / `1` AS `13`
FROM cohort_pivot;



-- RFM 분석 
SELECT * FROM online_retail_clean;

SELECT DATE_ADD(MAX((invoicedate)), INTERVAL 1 DAY) from retail; 


WITH RFM AS(
Select customerid,
	   MAX(SUBSTR(invoicedate,1,10)) as recent_date,
	   DATEDIFF('2011-12-10', MAX(SUBSTR(invoicedate,1,10))) AS Recency,
       COUNT(InvoiceNO) as Frequency,
       ROUND(SUM(unitprice*Quantity),0) AS MonetaryValue
FROM online_retail_clean
GROUP BY customerid
ORDER BY customerid
)
select * from RFM;


WITH RFM AS(
Select customerid,
	   MAX(SUBSTR(invoicedate,1,10)) as recent_date,
	   DATEDIFF('2011-12-10', MAX(SUBSTR(invoicedate,1,10))) AS Recency,
       COUNT(DISTINCT(InvoiceNO)) as Frequency,
       ROUND(SUM(unitprice*Quantity),0) AS Monetary
FROM (SELECT *, quantity * unitprice amount
      FROM online_retail_clean
      ) sales
GROUP BY CustomerID
ORDER BY CustomerID
),rfm_score AS (
SELECT CustomerID
     , recent_date
     , recency
     , frequency
     , monetary
     , CASE WHEN recency <= 18 THEN 4
            WHEN recency <= 51 THEN 3
            WHEN recency <= 142 THEN 2
            ELSE 1 END AS R
	 , CASE WHEN frequency >= 5 THEN 4
            WHEN frequency >= 2 THEN 3
            WHEN frequency >= 1 THEN 2
            ELSE 1 END AS F
	 , CASE WHEN monetary >= 1660 THEN 4
            WHEN monetary >= 668 THEN 3
            WHEN monetary >= 306 THEN 2
            ELSE 1 END AS M
FROM rfm
)SELECT *
     , R+F+M RFM
FROM rfm_score;

segment AS(
	SELECT
		CASE WHEN RFM >9 THEN "GOLD"
        WHEN RFM >5 AND RFM <=9 THEN "SILVER"
        ELSE BRONZE END AS SEGMT
	FROM rfm_score
)
SELECT FROM RFM Score;