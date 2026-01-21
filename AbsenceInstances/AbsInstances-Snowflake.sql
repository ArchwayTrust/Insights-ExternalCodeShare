-- Absence Instances Query for Snowflake (untested)
-- Identifies continuous absence periods for students

WITH all_records AS (
    -- Get all attendance records ordered by date
    SELECT
        a.ATTENDANCE_ROLL_CALL_RECORD_UNIQUE_ID
        ,a.STUDENT_UNIQUE_ID
        ,a.APPLICATION_ID
        ,a.DATE
        ,a.ATTENDANCE_ROLL_CALL_ID
        ,a.IS_PRESENT
        ,a.IS_POSSIBLE_ATTENDANCE
    FROM ARBOR_BI_CONNECTOR_PRODUCTION.ARBOR_MIS_ENGLAND_MODELLED.ROLL_CALL_ATTENDANCE AS a
    WHERE a.DATE < '2025-12-31' 
        AND a.DATE >= '2025-09-01'
),
with_groups AS (
    -- Create absence group IDs - increment whenever student is present
    -- This groups continuous absences together even if there are non-possible days in between
    SELECT
        *
        ,SUM(CASE WHEN IS_PRESENT = TRUE THEN 1 ELSE 0 END) 
            OVER (PARTITION BY STUDENT_UNIQUE_ID, APPLICATION_ID 
                  ORDER BY DATE, ATTENDANCE_ROLL_CALL_ID 
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ABSENCE_GROUP_ID
    FROM all_records
),
absence_records AS (
    -- Filter to only actual absence records (where attendance was possible but student was absent)
    SELECT
        *
    FROM with_groups
    WHERE IS_POSSIBLE_ATTENDANCE = TRUE
        AND IS_PRESENT = FALSE
),
absence_instances AS (
    -- Aggregate each continuous absence period
    SELECT
        STUDENT_UNIQUE_ID
        ,APPLICATION_ID
        ,MIN(DATE) AS ABSENCE_START_DATE
        ,MAX(DATE) AS ABSENCE_END_DATE
        ,COUNT(*)/2 AS POSSIBLE_ATTENDANCE_DAYS_MISSED
    FROM absence_records
    GROUP BY STUDENT_UNIQUE_ID, APPLICATION_ID, ABSENCE_GROUP_ID
)
SELECT
    STUDENT_UNIQUE_ID
    ,APPLICATION_ID
    ,ABSENCE_START_DATE
    ,ABSENCE_END_DATE
    ,POSSIBLE_ATTENDANCE_DAYS_MISSED
FROM absence_instances
ORDER BY STUDENT_UNIQUE_ID, APPLICATION_ID, ABSENCE_START_DATE
