/** Ratings Distribution **/
select
  overall,
  count(*)
from reviews
GROUP BY overall
ORDER BY overall;


/** Ratings Distribution by ASIN **/
SELECT
  p.asin,
  coalesce(one_star_votes,0) as one_star_votes,
  coalesce(two_star_votes,0) as two_star_votes,
  coalesce(three_star_votes,0) as three_star_votes,
  coalesce(four_star_votes,0) as four_star_votes,
  coalesce(five_star_votes,0) as five_star_votes
FROM products p
LEFT JOIN (
    SELECT
      asin,
      count(*) as one_star_votes
    FROM reviews
    WHERE overall = 1
    GROUP BY asin) r1
  on
    p.asin = r1.asin
LEFT JOIN (
    SELECT
      asin,
      count(*) as two_star_votes
    FROM reviews
    WHERE overall = 2
    GROUP BY asin) r2
  on
    p.asin = r2.asin
LEFT JOIN (
    SELECT
      asin,
      count(*) as three_star_votes
    FROM reviews
    WHERE overall = 3
    GROUP BY asin) r3
  on
    p.asin = r3.asin
LEFT JOIN (
    SELECT
      asin,
      count(*) as four_star_votes
    FROM reviews
    WHERE overall = 4
    GROUP BY asin) r4
  on
    p.asin = r4.asin
LEFT JOIN (
    SELECT
      asin,
      count(*) as five_star_votes
    FROM reviews
    WHERE overall = 5
    GROUP BY asin) r5
  on
    p.asin = r5.asin;

/** Product Stats **/
SELECT
    orderlines.productid,
    products.asin,
    count(*) as num_orders,
    min(shipdate) as first_order,
    max(shipdate) as last_order,
    DATE_PART('day', max(shipdate)::TIMESTAMP - min(shipdate)::TIMESTAMP) as days_on_sale,
    min(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_min,
    max(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_max,
    avg(regexp_replace(unitprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as unitprice_avg,
    min(numunits) as numunits_min,
    max(numunits) as numunits_max,
    avg(numunits) as numunits_avg,
    sum(numunits) as numunits_sum,
    min(regexp_replace(totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_min,
    max(regexp_replace(totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_max,
    avg(regexp_replace(totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_avg,
    sum(regexp_replace(totalprice :: TEXT, '[$,]', '', 'g') :: NUMERIC) as totalprice_sum
FROM orderlines
JOIN products
  ON orderlines.productid = products.productid
WHERE numunits > 0
GROUP BY orderlines.productid, products.asin
ORDER BY count(*) DESC;


