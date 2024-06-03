CREATE TABLE coinbase (
    type TEXT,
    product_id TEXT,
    price FLOAT
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

create view stats as (
SELECT
 tumble('5 seconds') as window,
 product_id,
 first_value(price) AS open,
 max(price) AS high,
 min(price) AS low,
 last_value(price) AS close
FROM
 coinbase
GROUP BY
 1, 2);

select window.start, product_id, open, high, low, close
from stats
where product_id is not null;
