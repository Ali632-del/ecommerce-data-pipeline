SELECT f.product_id, p.title, f.category_slug, f.rating, f.final_price, f.price_bucket
FROM fact_product_listing f
JOIN dim_product p ON f.product_id = p.product_id
LIMIT 20;