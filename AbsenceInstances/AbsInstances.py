base_attendance_table = "Lakehouse.LH_Base.dbo.edu_arbor_roll_call_attendance"
period_start_date = "2025-09-01"
period_end_date = "2025-12-31"

df = spark.sql(f"""
    WITH all_records AS (
        -- Get all attendance records ordered by date
        SELECT
            a.attendance_roll_call_record_unique_id
            ,a.student_unique_id
            ,a.application_id
            ,a.`date`
            ,a.attendance_roll_call_id
            ,a.is_present
            ,a.is_possible_attendance
        FROM {base_attendance_table} AS a
        WHERE a.date < '{period_start_date}' 
            AND a.date >= '{period_end_date}'
    ),
    with_groups AS (
        -- Create absence group IDs - increment whenever student is present
        -- This groups continuous absences together even if there are non-possible days in between
        SELECT
            *
            ,SUM(CASE WHEN is_present = true THEN 1 ELSE 0 END) 
                OVER (PARTITION BY student_unique_id, application_id 
                      ORDER BY `date`, attendance_roll_call_id 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS absence_group_id
        FROM all_records
    ),
    absence_records AS (
        -- Filter to only actual absence records (where attendance was possible but student was absent)
        SELECT
            *
        FROM with_groups
        WHERE is_possible_attendance = true
            AND is_present = false
    ),
    absence_instances AS (
        -- Aggregate each continuous absence period
        SELECT
            student_unique_id
            ,application_id
            ,MIN(`date`) AS absence_start_date
            ,MAX(`date`) AS absence_end_date
            ,COUNT(*) AS possible_attendance_days_missed
        FROM absence_records
        GROUP BY student_unique_id, application_id, absence_group_id
    )
    SELECT
        student_unique_id
        ,application_id
        ,absence_start_date
        ,absence_end_date
        ,possible_attendance_days_missed
    FROM absence_instances
    ORDER BY student_unique_id, application_id, absence_start_date
""")