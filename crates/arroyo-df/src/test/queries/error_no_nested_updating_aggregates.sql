--fail=Error during planning: can't currently nest updating aggregates
CREATE TABLE impulse with (
    connector = 'impulse',
    event_rate = '10'
  );
  SELECT sum(count) , max(counter) FROM(
  SELECT count(*) as count, subtask_index, max(counter) as counter FROM impulse group by 2);