CREATE TABLE `{project_id}.{category_name}.product_change_history` (
    change_date DATE,
    product_id INT64,
    new_total_results INT64,
    old_total_results INT64,
    new_price FLOAT64,
    old_price FLOAT64,
    new_conditions ARRAY<STRUCT<value STRING, count INT64>>,
    old_conditions ARRAY<STRUCT<value STRING, count INT64>>,
    new_listing_types ARRAY<STRUCT<value STRING, count INT64>>,
    old_listing_types ARRAY<STRUCT<value STRING, count INT64>>,
    new_printings ARRAY<STRUCT<value STRING, count INT64>>,
    old_printings ARRAY<STRUCT<value STRING, count INT64>>,
    change_type STRING
);

CREATE TABLE `{project_id}.{category_name}.seller_change_history` (
    change_date DATE,
    seller_key STRING,
    old_rating FLOAT64,
    new_rating FLOAT64,
    old_sales INT64,
    new_sales INT64,
    old_verified BOOL,
    new_verified BOOL,
    change_type STRING
);

CREATE MATERIALIZED VIEW `{project_id}.{category_name}.product_changes`
PARTITION BY change_date
OPTIONS (refresh_interval_minutes = 1440)
AS
WITH DailyProductData AS (
    SELECT 
        p.last_updated AS update_date,
        p.product_id,
        p.total_results,
        p.price,
        ARRAY_AGG(STRUCT(c.value, c.count) ORDER BY c.value) AS conditions,
        ARRAY_AGG(STRUCT(lt.value, lt.count) ORDER BY lt.value) AS listing_types,
        ARRAY_AGG(STRUCT(pr.value, pr.count) ORDER BY pr.value) AS printings,
        ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p.last_updated DESC) AS rn
    FROM `{project_id}.{category_name}.products` AS p,
        UNNEST(p.conditions) AS c,
            UNNEST(p.listing_types) AS lt,
                UNNEST(p.printings) AS pr 
    GROUP BY 1, 2, 3, 4
)

SELECT
    CURRENT_DATE() AS change_date,
    today.product_id,
    today.totalResults AS new_totalResults,
    yesterday.totalResults AS old_totalResults,
    today.price AS new_price,
    yesterday.price AS old_price,
    today.conditions AS new_conditions,
    yesterday.conditions AS old_conditions,
    today.listingTypes AS new_listingTypes,
    yesterday.listingTypes AS old_listingTypes,
    today.printings AS new_printings,
    yesterday.printings AS old_printings,
    CASE 
        WHEN today.price != yesterday.price THEN 'price_update'
        WHEN today.totalResults != yesterday.totalResults THEN 'total_results_update'
        WHEN today.conditions != yesterday.conditions THEN 'conditions_update'
        WHEN today.listingTypes != yesterday.listingTypes THEN 'listing_types_update'
        WHEN today.printings != yesterday.printings THEN 'printings_update'
        ELSE 'no_change' 
    END AS change_type
FROM DailyProductData AS today
LEFT JOIN DailyProductData AS yesterday 
  ON today.product_id = yesterday.product_id 
  AND today.update_date = DATE_SUB(yesterday.update_date, INTERVAL 1 DAY)
WHERE today.rn = 1;

CREATE MATERIALIZED VIEW `{project_id}.{category_name}.seller_changes`
PARTITION BY change_date
OPTIONS (refresh_interval_minutes = 1440)
AS
WITH DailySellerData AS (
    SELECT 
        s.last_updated AS update_date,
        s.seller_key,
        s.seller_id,
        s.seller_name,
        s.seller_rating,
        s.seller_sales,
        s.verified,
        ROW_NUMBER() OVER (PARTITION BY s.seller_key ORDER BY s.last_updated DESC) AS rn 
    FROM `{project_id}.{category_name}.sellers` AS s
)

SELECT
    CURRENT_DATE() AS change_date,
    today.seller_key,
    today.seller_id AS new_seller_id,
    yesterday.seller_id AS old_seller_id,
    today.seller_name AS new_seller_name,
    yesterday.seller_name AS old_seller_name,
    today.seller_rating AS new_seller_rating,
    yesterday.seller_rating AS old_seller_rating,
    today.seller_sales AS new_seller_sales,
    yesterday.seller_sales AS old_seller_sales,
    today.verified AS new_verified,
    yesterday.verified AS old_verified,
    CASE
        WHEN today.seller_id != yesterday.seller_id THEN 'seller_id_update'
        WHEN today.seller_name != yesterday.seller_name THEN 'seller_name_update'
        WHEN today.seller_rating != yesterday.seller_rating THEN 'seller_rating_update'
        WHEN today.seller_sales != yesterday.seller_sales THEN 'seller_sales_update'
        WHEN today.verified != yesterday.verified THEN 'verification_status_update'
        ELSE 'no_change'
    END AS change_type
FROM DailySellerData AS today
LEFT JOIN DailySellerData AS yesterday
  ON today.seller_key = yesterday.seller_key
  AND today.update_date = DATE_SUB(yesterday.update_date, INTERVAL 1 DAY)
WHERE today.rn = 1;