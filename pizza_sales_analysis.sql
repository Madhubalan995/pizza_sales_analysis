create database pizza_details_analysis;
use pizza_details_analysis;
drop database pizza_details_analysis;

-- Total revenue

SELECT left(SUM(price),8) AS total_revenue FROM pizza_details;

-- Total number of orders
SELECT COUNT(DISTINCT order_id) AS total_orders FROM pizza_details;

-- Average order value
SELECT left(SUM(price)/COUNT(DISTINCT order_id),6) AS avg_order_value FROM pizza_details;
-- Most popular pizzas by order count
SELECT pizza_name, COUNT(*) AS order_count
FROM pizza_details
GROUP BY pizza_name
ORDER BY order_count DESC
LIMIT 10;

-- Revenue by pizza category
SELECT pizza_category, SUM(price) AS category_revenue
FROM pizza_details
GROUP BY pizza_category
ORDER BY category_revenue DESC;
-- Top spending customers
SELECT 
    person_name, 
    SUM(price) AS total_spent,
    COUNT(DISTINCT order_id) AS order_count
FROM pizza_details
GROUP BY person_name
ORDER BY total_spent DESC
LIMIT 20;

-- Customers who ordered the most unique pizza types
SELECT 
    person_name,
    COUNT(DISTINCT pizza_name) AS unique_pizzas_ordered
FROM pizza_details
GROUP BY person_name
ORDER BY unique_pizzas_ordered DESC
LIMIT 10;
-- Revenue by pizza size
SELECT 
    pizza_size, 
    SUM(price) AS size_revenue,
    COUNT(*) AS order_count,
    SUM(price)/COUNT(*) AS avg_price_per_size
FROM pizza_details
GROUP BY pizza_size
ORDER BY size_revenue DESC;
-- Most expensive pizzas
SELECT 
    pizza_name,
    pizza_size,
    price
FROM pizza_details
ORDER BY price DESC
LIMIT 10;

-- Price distribution by category
SELECT 
    pizza_category,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    COUNT(*) AS count
FROM pizza_details
GROUP BY pizza_category;
-- RFM (Recency, Frequency, Monetary) Analysis
WITH customer_stats AS (
    SELECT 
        person_name,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(price) AS monetary
    FROM pizza_details
    GROUP BY person_name
)
SELECT 
    person_name,
    last_order_date,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY last_order_date DESC) AS recency_score,
    NTILE(5) OVER (ORDER BY frequency) AS frequency_score,
    NTILE(5) OVER (ORDER BY monetary) AS monetary_score
FROM customer_stats
ORDER BY monetary DESC;
-- Most commonly ordered pizza pairs in the same order
WITH order_pizzas AS (
    SELECT 
        order_id,
        GROUP_CONCAT(pizza_name, '|') AS pizza_names
    FROM pizza_details
    GROUP BY order_id
    HAVING COUNT(*) > 1
),
pizza_pairs AS (
    SELECT 
        a.pizza_name AS pizza1,
        b.pizza_name AS pizza2,
        COUNT(*) AS pair_count
    FROM pizza_details a
    JOIN pizza_details b ON a.order_id = b.order_id AND a.pizza_id < b.pizza_id
    GROUP BY pizza1, pizza2
)
SELECT 
    pizza1,
    pizza2,
    pair_count
FROM pizza_pairs
ORDER BY pair_count DESC
LIMIT 10;
-- Price elasticity analysis by pizza type
SELECT 
    pizza_name,
    COUNT(*) AS order_count,
    AVG(price) AS avg_price,
    SUM(price) AS total_revenue,
    SUM(price)/COUNT(*) AS revenue_per_order
FROM pizza_details
GROUP BY pizza_name
ORDER BY revenue_per_order DESC;

SELECT 
    DATE(order_date, 'start of month') AS month,
    SUM(price) AS monthly_revenue
FROM pizza_details
GROUP BY month
ORDER BY month;

-- Monthly revenue trends
SELECT 
    STRFTIME('%Y-%m', order_date) AS month,
    SUM(price) AS monthly_revenue
FROM pizza_details
GROUP BY month
ORDER BY month;

-- Daily sales patterns
SELECT 
    STRFTIME('%H', order_time) AS hour_of_day,
    COUNT(*) AS order_count
FROM pizza_details
GROUP BY hour_of_day
ORDER BY hour_of_day;
-- sp--
DELIMITER $$
CREATE PROCEDURE top_pizzas(
    IN category_name VARCHAR(100),
    IN size_filter VARCHAR(10)
)
BEGIN
    SELECT 
        pizza_name,
        pizza_category,
        pizza_size,
        COUNT(*) AS order_count,
        SUM(price) AS total_revenue,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(DISTINCT order_id) AS unique_orders
    FROM pizza_details
    WHERE 
        pizza_category = category_name AND
        (pizza_size = size_filter OR size_filter IS NULL)
    GROUP BY pizza_name, pizza_category, pizza_size
    ORDER BY order_count DESC, total_revenue DESC
    LIMIT 5;
END$$
DELIMITER ;
CALL top_5_pizzas('Supreme', 'L');
-- fun--
DELIMITER $$
CREATE FUNCTION get_top_customers_by_category(
    category_name VARCHAR(100),
    size_filter VARCHAR(10)
) 
RETURNS TEXT
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result TEXT DEFAULT '';
    DECLARE done INT DEFAULT FALSE;
    DECLARE customer_name VARCHAR(100);
    DECLARE customer_orders INT;
    DECLARE customer_revenue DECIMAL(10,2);
    DECLARE counter INT DEFAULT 0;
    DECLARE customer_cursor CURSOR FOR
        SELECT 
            person_name,
            COUNT(*) AS order_count,
            SUM(price) AS total_spent
        FROM pizza_details
        WHERE 
            pizza_category = category_name AND
            (pizza_size = size_filter OR size_filter IS NULL)
        GROUP BY person_name
        ORDER BY order_count DESC, total_spent DESC
        LIMIT 5;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN customer_cursor;
    customer_loop: LOOP
        FETCH customer_cursor INTO customer_name, customer_orders, customer_revenue;
        IF done THEN
            LEAVE customer_loop;
        END IF;
        SET counter = counter + 1;
        SET result = CONCAT(result, 
                           counter, '. ', customer_name, 
                           ' - Orders: ', customer_orders, 
                           ', Spent: $', ROUND(customer_revenue, 2),
                           IF(counter < 5, '\n', ''));
    END LOOP;
    CLOSE customer_cursor;
    RETURN IFNULL(result, 'No customers found for this category/size combination');
END$$
DELIMITER ;
-- Get top 5 customers for Supreme pizzas in Large size
SELECT get_top_customers_by_category('Supreme', 'L') AS top_customers;

-- Get top 5 customers for all Veggie pizzas (any size)
SELECT get_top_customers_by_category('Veggie', NULL) AS top_customers;
