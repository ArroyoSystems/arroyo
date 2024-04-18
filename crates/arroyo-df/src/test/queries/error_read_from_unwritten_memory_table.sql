--fail=Error during planning: Can't query from memory table memory without first inserting into it.
    CREATE TABLE memory (
      event_type TEXT,
      location TEXT,
      driver_id BIGINT,
    );

    SELECT * FROM memory;