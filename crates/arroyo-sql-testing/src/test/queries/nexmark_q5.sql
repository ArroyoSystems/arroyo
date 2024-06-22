CREATE TABLE bids (
  datetime TIMESTAMP,
  auction BIGINT
) WITH (
  connector = 'single_file',
  path = '$input_dir/nexmark_bids.json',
  format = 'json',
  type = 'source',
  event_time_field = 'datetime'
);
CREATE TABLE top_auctions (
  auction BIGINT,
  count INT
) WITH (
  connector = 'single_file',
  path = '$output_path',
  format = 'json',
  type = 'sink'
);

INSERT INTO top_auctions
SELECT AuctionBids.auction, AuctionBids.num
 FROM (
   SELECT
     auction,
     count(*) AS num,
     hop(interval '2 second', interval '10 seconds') as window
    FROM bids
    GROUP BY auction, window
 ) AS AuctionBids
 JOIN (
   SELECT
     max(CountBids.num) AS maxn,
     CountBids.window
   FROM (
     SELECT
       count(*) AS num,
       hop(interval '2 second', interval '10 seconds') as window
     FROM bids
     GROUP BY auction, window
     ) AS CountBids
   GROUP BY CountBids.window
 ) AS MaxBids
 ON AuctionBids.window = MaxBids.window AND AuctionBids.num >= MaxBids.maxn;
