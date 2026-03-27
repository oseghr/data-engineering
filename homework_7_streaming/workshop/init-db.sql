-- ============================================================
-- Auto-runs when PostgreSQL container starts for the first time
-- Creates all result tables needed for Q4, Q5, Q6
-- ============================================================

-- Q4: Tumbling window — trip counts per pickup location
CREATE TABLE IF NOT EXISTS trip_counts_by_location (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    "PULocationID" INT,
    num_trips    BIGINT
);

-- Q5: Session window — trip streaks per pickup location
CREATE TABLE IF NOT EXISTS session_trips (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    "PULocationID" INT,
    num_trips    BIGINT
);

-- Q6: Hourly tumbling window — total tips per hour
CREATE TABLE IF NOT EXISTS hourly_tips (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    total_tip    DOUBLE PRECISION
);
