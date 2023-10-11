export interface ExampleQuery {
  name: string;
  shortDescription: string;
  longDescription: string;
  url?: string;
  query: string;
}

export const exampleQueries: ExampleQuery[] = [
  {
    name: 'Mastodon Trends',
    shortDescription: 'Real-time analytics on the Mastodon firehose',
    longDescription:
      'This query reads from the Mastodon firehouse via a Server-Sent Events source ' +
      'and computes the top 5 trending tags over a 15 minute window.',
    url: 'https://doc.arroyo.dev/tutorial/mastodon',
    query:
      'CREATE TABLE mastodon (\n' +
      '    value TEXT\n' +
      ') WITH (\n' +
      "    connector = 'sse',\n" +
      "    format = 'raw_string',\n" +
      "    endpoint = 'https://mstdn.party/api/v1/streaming/public',\n" +
      "    events = 'update'\n" +
      ');\n' +
      '\n' +
      'CREATE VIEW tags AS (\n' +
      '    SELECT tag FROM (\n' +
      "        SELECT extract_json_string(value, '$.tags[*].name') AS tag\n" +
      '     FROM mastodon)\n' +
      '    WHERE tag is not null\n' +
      ');\n' +
      '\n' +
      'SELECT * FROM (\n' +
      '    SELECT *, ROW_NUMBER() OVER (\n' +
      '        PARTITION BY window\n' +
      '        ORDER BY count DESC) as row_num\n' +
      '    FROM (SELECT count(*) as count,\n' +
      '        tag,\n' +
      "        hop(interval '5 seconds', interval '15 minutes') as window\n" +
      '            FROM tags\n' +
      '            group by tag, window)) WHERE row_num <= 5;\n',
  },
  {
    name: 'Bitcoin Exchange Rate',
    shortDescription: 'Use a Websocket and sliding window',
    longDescription:
      'Fetch the current Bitcoin exchange rate from Coinbase via a Websocket connection. ' +
      'Use a sliding window to emit the average price over the last minute.',
    query:
      'CREATE TABLE coinbase (\n' +
      '    type TEXT,\n' +
      '    price TEXT\n' +
      ') WITH (\n' +
      "    connector = 'websocket',\n" +
      "    endpoint = 'wss://ws-feed.exchange.coinbase.com',\n" +
      "    subscription_message = '{\n" +
      '      "type": "subscribe",\n' +
      '      "product_ids": [\n' +
      '        "BTC-USD"\n' +
      '      ],\n' +
      '      "channels": ["ticker"]\n' +
      "    }',\n" +
      "    format = 'json'\n" +
      ');\n' +
      ' \n' +
      'SELECT avg(CAST(price as FLOAT)) from coinbase\n' +
      "WHERE type = 'ticker'\n" +
      "GROUP BY hop(interval '5' second, interval '1 minute');",
  },
];
