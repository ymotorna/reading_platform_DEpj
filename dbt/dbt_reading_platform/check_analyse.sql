
-- check
SELECT count(*) FROM dim_users;
SELECT count(*) FROM dim_authors;
SELECT count(*) FROM dim_books;

SELECT count(*) FROM fct_reading_sessions;
SELECT count(*) FROM fct_reviews;
SELECT count(*) FROM fct_transactions;


-- analyse

-- user engagement
select user_id,
       country_code,
       total_pages_read,
       books_completed,
       round(lifetime_payments, 2) as lifetime_oayments,
       revenue_rank
from user_stats
where is_active=true
order by revenue_rank asc
limit 10;

-- genre perfomance
select g.genre_name,
       count(bp.book_id) as num_books,
       count(distinct bp.author_id) as num_authors,
       sum(bp.total_readers) as total_readers,
       sum(bp.total_Sessions) as total_sessions,
       round(avg(bp.genre_avg_rating), 2) as genre_avg_rateing
from genres g
left join book_performance bp
    on g.genre_id = bp.genre_id
where is_available=true
group by genre_name
order by genre_avg_rateing desc;


-- revenue
select date_trunc('month', billing_period_start) as month,
    plan_type,
    round(sum(payment_amount), 2) as total_revenue,
    round(sum(payment_amount) / sum(sum(payment_amount)) over () * 100, 2) as pct_of_total_revenue,
    mode() within group (order by payment_method) as top_payment_method
from fct_transactions
where payment_status='success'
group by 1,2
order by 1,2 asc;