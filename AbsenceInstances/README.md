# Absence Instances - Window Function Explanation

## Overview
This script identifies continuous absence periods (instances) for students by grouping consecutive absences together, even when non-attendance days (weekends, holidays) occur in between.
- [Absence Instances - Fabric Lakehouse](AbsInstances.py)
- [Absence Instances - Direct Snowflake](AbsInstances-Snowflake.sql)
## How the Window Function Works

### The Key Window Function
```sql
SUM(CASE WHEN is_present = true THEN 1 ELSE 0 END) 
    OVER (PARTITION BY student_unique_id, application_id 
          ORDER BY `date`, attendance_roll_call_id 
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS absence_group_id
```

### Breaking Down the Components

#### 1. **PARTITION BY student_unique_id, application_id**
- Separates data into independent windows for each student/application combination
- Each student's attendance is analyzed independently
- Prevents one student's attendance from affecting another's groupings

#### 2. **ORDER BY `date`, attendance_roll_call_id**
- Sorts records chronologically within each partition
- Secondary sort by `attendance_roll_call_id` handles multiple roll calls per day
- Ensures the running sum is calculated in the correct temporal sequence

#### 3. **ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW**
- Creates a running/cumulative sum from the first row to the current row
- Each row sees all previous rows in its partition
- This is what makes it a "running total"

#### 4. **The SUM and CASE Logic**
- `CASE WHEN is_present = true THEN 1 ELSE 0 END` creates a counter that increments only when the student is present
- The SUM creates a cumulative count of "present" days
- **Result**: The `absence_group_id` stays the same for consecutive absences, then increments when the student is present

## Example Walkthrough

Given these attendance records for one student:

| Date       | is_present | is_possible_attendance | absence_group_id |
|------------|------------|------------------------|------------------|
| 2025-09-01 | false      | true                   | 0                |
| 2025-09-02 | false      | true                   | 0                |
| 2025-09-03 | true       | true                   | 1                |
| 2025-09-04 | false      | true                   | 1                |
| 2025-09-05 | false      | false (weekend)        | 1                |
| 2025-09-06 | false      | false (weekend)        | 1                |
| 2025-09-07 | false      | true                   | 1                |
| 2025-09-08 | true       | true                   | 2                |

### Result: Two Absence Instances

**Instance 1 (absence_group_id = 0):**
- Start: 2025-09-01
- End: 2025-09-02
- Days missed: 2

**Instance 2 (absence_group_id = 1):**
- Start: 2025-09-04
- End: 2025-09-07
- Days missed: 3 (excludes weekend where `is_possible_attendance = false`)

## Why This Approach Works

1. **Continuous Grouping**: The window function creates an ID that remains constant during absence periods and only changes when attendance occurs
2. **Handles Non-Attendance Days**: Weekend and holiday records don't break the absence group because they don't increment the counter
3. **Efficient**: Uses a single pass over the data rather than complex self-joins or iterative logic
4. **Scalable**: Window functions are optimized in modern SQL engines for large datasets

## Query Flow

1. **all_records CTE**: Retrieves all attendance data for the specified date range
2. **with_groups CTE**: Applies the window function to create absence group IDs
3. **absence_records CTE**: Filters to only records where attendance was possible but student was absent
4. **absence_instances CTE**: Groups by the absence_group_id to aggregate each absence period
5. **Final SELECT**: Returns the consolidated absence instances with start date, end date, and total days missed
