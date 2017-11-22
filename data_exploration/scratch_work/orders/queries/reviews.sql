/** Helpful Ratio Distribution **/

SELECT
  round(
      cast(substring(helpful, position('[' in helpful)+1, position(',' in helpful) - 1 - position('[' in helpful)) as NUMERIC)
      / cast(substring(helpful, position(',' in helpful)+2, position(']' in helpful) - 2 - position(',' in helpful)) as NUMERIC)
      , 2
  ) as helpfulRatio,
  count(*)
FROM reviews
WHERE outofhelpful > 0
GROUP BY helpfulRatio
ORDER BY helpfulRatio