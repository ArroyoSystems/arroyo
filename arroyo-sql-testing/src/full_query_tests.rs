use arroyo_sql_macro::full_pipeline_codegen;

full_pipeline_codegen! {"select_star", "SELECT * FROM nexmark"}

full_pipeline_codegen! {"query_5_join",
"WITH bids as (SELECT bid.auction as auction, bid.datetime as datetime
    FROM (select bid from  nexmark) where bid is not null)
    SELECT AuctionBids.auction as auction, AuctionBids.num as count
    FROM (
      SELECT
        B1.auction,
        HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) as window,
        count(*) AS num

      FROM bids B1
      GROUP BY
        1,2
    ) AS AuctionBids
    JOIN (
      SELECT 
        max(num) AS maxn,
        window
      FROM (
        SELECT
          count(*) AS num,
          HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS window
        FROM bids B2
        GROUP BY
          B2.auction,2
        ) AS CountBids
      GROUP BY 2
    ) AS MaxBids
    ON
       AuctionBids.num = MaxBids.maxn
       and AuctionBids.window = MaxBids.window;"}
