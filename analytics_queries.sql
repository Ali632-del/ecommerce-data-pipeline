-- =============================================================
-- analytics_queries.sql
-- E-Commerce Data Warehouse — Business Intelligence Queries
-- =============================================================
-- All queries run against the dw schema in PostgreSQL.
-- Designed for import into Power BI / Tableau as named datasets.
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. EXECUTIVE OVERVIEW — KPI snapshot
-- ─────────────────────────────────────────────────────────────
-- KPIs: total SKUs, avg rating, avg discount %, revenue potential
SELECT
    COUNT(DISTINCT product_id)                          AS total_skus,
    ROUND(AVG(rating)::NUMERIC, 2)                      AS avg_rating,
    ROUND(AVG(discount)::NUMERIC, 1)                    AS avg_discount_pct,
    ROUND(AVG(final_price)::NUMERIC, 2)                 AS avg_selling_price,
    ROUND(SUM(final_price)::NUMERIC, 2)                 AS total_revenue_potential,
    COUNT(*) FILTER (WHERE is_discounted)               AS discounted_products,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_discounted)
        / NULLIF(COUNT(*), 0), 1
    )                                                   AS pct_discounted
FROM dw.fact_product_listing;


-- ─────────────────────────────────────────────────────────────
-- 2. CATEGORY PERFORMANCE — revenue & rating by category
-- ─────────────────────────────────────────────────────────────
SELECT
    c.category_name,
    COUNT(f.product_id)                                 AS product_count,
    ROUND(AVG(f.rating)::NUMERIC, 2)                    AS avg_rating,
    ROUND(AVG(f.final_price)::NUMERIC, 2)               AS avg_price,
    ROUND(SUM(f.final_price)::NUMERIC, 2)               AS total_revenue_potential,
    ROUND(AVG(f.discount)::NUMERIC, 1)                  AS avg_discount_pct,
    ROUND(AVG(f.discount_amount)::NUMERIC, 2)           AS avg_discount_amount
FROM dw.fact_product_listing f
JOIN dw.dim_category c ON f.category_slug = c.category_slug
GROUP BY c.category_name
ORDER BY total_revenue_potential DESC;


-- ─────────────────────────────────────────────────────────────
-- 3. PRICE SEGMENTATION — how products distribute across tiers
-- ─────────────────────────────────────────────────────────────
SELECT
    price_bucket,
    COUNT(*)                                            AS product_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_catalog,
    ROUND(AVG(rating)::NUMERIC, 2)                      AS avg_rating,
    ROUND(AVG(ratings_count)::NUMERIC, 0)               AS avg_review_count,
    ROUND(AVG(discount)::NUMERIC, 1)                    AS avg_discount_pct
FROM dw.fact_product_listing
GROUP BY price_bucket
ORDER BY
    CASE price_bucket
        WHEN 'budget'    THEN 1
        WHEN 'mid_range' THEN 2
        WHEN 'premium'   THEN 3
        WHEN 'luxury'    THEN 4
        ELSE 5
    END;


-- ─────────────────────────────────────────────────────────────
-- 4. SELLER ANALYSIS — top 10 sellers by catalog size
-- ─────────────────────────────────────────────────────────────
SELECT
    s.seller_name,
    COUNT(f.product_id)                                 AS products_listed,
    ROUND(AVG(f.rating)::NUMERIC, 2)                    AS avg_product_rating,
    ROUND(AVG(f.final_price)::NUMERIC, 2)               AS avg_price,
    ROUND(SUM(f.discount_amount)::NUMERIC, 2)           AS total_discount_given,
    ROUND(AVG(f.discount)::NUMERIC, 1)                  AS avg_discount_pct
FROM dw.fact_product_listing f
JOIN dw.dim_seller s ON f.seller_name = s.seller_name
WHERE s.seller_name <> 'Unknown Seller'
GROUP BY s.seller_name
ORDER BY products_listed DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- 5. RATING DISTRIBUTION — product quality funnel
-- ─────────────────────────────────────────────────────────────
SELECT
    ratings_tier,
    COUNT(*)                                            AS product_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_catalog,
    ROUND(AVG(final_price)::NUMERIC, 2)                 AS avg_price,
    ROUND(AVG(ratings_count)::NUMERIC, 0)               AS avg_reviews
FROM dw.fact_product_listing
GROUP BY ratings_tier
ORDER BY
    CASE ratings_tier
        WHEN 'excellent' THEN 1
        WHEN 'good'      THEN 2
        WHEN 'average'   THEN 3
        WHEN 'poor'      THEN 4
        ELSE 5
    END;


-- ─────────────────────────────────────────────────────────────
-- 6. TOP 20 PRODUCTS — by weighted popularity score
-- ─────────────────────────────────────────────────────────────
SELECT
    p.product_id,
    p.title,
    c.category_name,
    f.rating,
    f.ratings_count,
    f.final_price,
    f.discount,
    f.rating_x_count                                    AS popularity_score
FROM dw.fact_product_listing f
JOIN dw.dim_product  p ON f.product_id   = p.product_id
JOIN dw.dim_category c ON f.category_slug = c.category_slug
ORDER BY f.rating_x_count DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────
-- 7. DISCOUNT EFFECTIVENESS — does discounting correlate with ratings?
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN discount = 0          THEN '0%'
        WHEN discount < 10         THEN '1–9%'
        WHEN discount BETWEEN 10 AND 19 THEN '10–19%'
        WHEN discount BETWEEN 20 AND 29 THEN '20–29%'
        WHEN discount BETWEEN 30 AND 49 THEN '30–49%'
        ELSE '50%+'
    END                                                 AS discount_band,
    COUNT(*)                                            AS product_count,
    ROUND(AVG(rating)::NUMERIC, 2)                      AS avg_rating,
    ROUND(AVG(ratings_count)::NUMERIC, 0)               AS avg_reviews,
    ROUND(AVG(final_price)::NUMERIC, 2)                 AS avg_final_price
FROM dw.fact_product_listing
GROUP BY discount_band
ORDER BY discount_band;


-- ─────────────────────────────────────────────────────────────
-- 8. LISTING QUALITY SCORE — completeness vs performance
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN title_word_count < 5  THEN 'sparse'
        WHEN title_word_count < 10 THEN 'moderate'
        ELSE 'rich'
    END                                                 AS listing_richness,
    COUNT(*)                                            AS products,
    ROUND(AVG(rating)::NUMERIC, 2)                      AS avg_rating,
    ROUND(AVG(ratings_count)::NUMERIC, 0)               AS avg_reviews,
    ROUND(AVG(final_price)::NUMERIC, 2)                 AS avg_price,
    COUNT(*) FILTER (WHERE has_seller)                  AS with_known_seller
FROM dw.fact_product_listing
GROUP BY listing_richness;


-- ─────────────────────────────────────────────────────────────
-- SUGGESTED DASHBOARD STRUCTURE (Power BI / Tableau)
-- ─────────────────────────────────────────────────────────────
--
-- Page 1 — Executive Overview
--   KPI cards : Total SKUs | Avg Rating | Avg Discount | Revenue Potential
--   Bar chart  : Revenue Potential by Category (top 10)
--   Donut      : % Discounted vs Full-Price
--
-- Page 2 — Product Intelligence
--   Scatter    : Rating vs Final Price (coloured by price_bucket)
--   Table      : Top 20 products by popularity_score
--   Histogram  : Price distribution across catalog
--
-- Page 3 — Seller & Category Deep-Dive
--   Bar chart  : Products per Seller (top 10)
--   Heat map   : Category × Price Bucket product count
--   Line/Bar   : Avg Discount % per Category
--
-- Page 4 — Data Quality Monitor
--   Gauge      : % products with known sellers
--   Pie        : Rating tier distribution
--   Table      : Listing richness vs avg rating
-- =============================================================
