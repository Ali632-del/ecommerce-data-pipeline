SELECT category_slug, COUNT(*) as products, ROUND(AVG(rating),2) as avg_rating, ROUND(SUM(final_price),0) as revenue
FROM fact_product_listing
GROUP BY category_slug
ORDER BY revenue DESC
LIMIT 10;