CREATE TABLE coinbase (
  type TEXT,
  price TEXT
) WITH (
  connector = 'websocket',
  endpoint = 'wss://ws-feed.exchange.coinbase.com',
  subscription_message = '{
      "type": "subscribe",
      "product_ids": [
        "BTC-USD"
      ],
      "channels": ["ticker"]
    }',
      format = 'json'
);

SELECT avg(CAST(price as FLOAT)) from coinbase
WHERE type = 'ticker'
GROUP BY hop(interval '5' second, interval '1 minute');