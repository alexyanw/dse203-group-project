SELECT
    productid,
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
WHERE numunits > 0
GROUP BY productid
ORDER BY count(*) DESC;
