--fail=Error during planning: Time window function hop are not allowed in this context. Are you missing a GROUP BY clause?
CREATE TABLE impulse WITH (
      connector = 'impulse',
      event_rate = '10',
      message_count = '20'
    );


    SELECT
     hop(interval '1 second', interval '10 second' ) as window,
count(*) as count,
min(counter) as min,
max(counter) as max
from impulse 