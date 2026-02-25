-- Add your Flink SQL below
select
    SUBSTRING(name, 1, 1) AS `first_char`,
    count(*) as `count_with_first_letter`
from `users`
group by SUBSTRING(name, 1, 1)