--fail=SQL window functions are not currently supported
SELECT * FROM (
    SELECT ROW_NUMBER()  OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_number, auction FROM (
      SELECT       bid.auction as auction,
    hop(INTERVAL '10' minute, INTERVAL '20' minute ) as window,
    count(*) as count
  FROM (SELECT bid from nexmark where bid is not null)
  GROUP BY 1, 2)) where row_number = 1