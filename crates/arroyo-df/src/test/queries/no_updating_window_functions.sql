--fail=SQL window functions are not currently supported

-- when window functions are supported, this error should be reported:
--fail=window function must be partitioned by a window as the first argument
SELECT *, row_number() OVER (partition by bid.auction order by bid.datetime desc) as row_num
     FROM nexmark where bid is not null