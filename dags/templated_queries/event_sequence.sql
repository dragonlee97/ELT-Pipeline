INSERT INTO {{ params.event_schema }}.event_sequence
WITH EVENT_USER AS (SELECT
ID, USER_ID, CREATED_AT, RANK() OVER(PARTITION BY USER_ID ORDER BY CREATED_AT)
FROM (
    SELECT * FROM{{ params.event_schema }}.{{ params.table_names["app"]["target"] }}
    UNION
    SELECT * FROM{{ params.event_schema }}.{{ params.table_names["web"]["target"] }}
)
WHERE USER_ID IS NOT NULL
)
SELECT e1.ID as EVENT_ID, e2.ID as PREVIOUS_EVENT_ID, e3.ID as NEXT_EVENT_ID, e1.USER_ID
FROM EVENT_USER e1 LEFT JOIN EVENT_USER e2 ON e1.USER_ID = e2.USER_ID AND e2.rank+1 = e1.rank
LEFT JOIN EVENT_USER e3 ON e1.USER_ID = e3.USER_ID AND e3.rank-1 = e1.rank;