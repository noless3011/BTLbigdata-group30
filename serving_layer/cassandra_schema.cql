-- Cassandra Schema for University Learning Analytics Serving Layer
-- Keyspace Definition

CREATE KEYSPACE IF NOT EXISTS university_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE university_analytics;

-- ========== SUMMARY METRICS ==========

-- Real-time active users (from speed layer)
CREATE TABLE IF NOT EXISTS active_users (
    window_start timestamp,
    window_end timestamp,
    active_users int,
    source text,
    PRIMARY KEY (window_start, window_end)
) WITH CLUSTERING ORDER BY (window_end DESC);

-- Course popularity (from speed layer)
CREATE TABLE IF NOT EXISTS course_popularity (
    course_id text,
    window_start timestamp,
    window_end timestamp,
    interactions int,
    views int,
    PRIMARY KEY (course_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- Video engagement (from speed layer)
CREATE TABLE IF NOT EXISTS video_engagement (
    video_id text,
    window_start timestamp,
    window_end timestamp,
    views int,
    unique_viewers int,
    total_watch_seconds int,
    PRIMARY KEY (video_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

-- ========== BATCH LAYER VIEWS ==========

-- Course Overview
CREATE TABLE IF NOT EXISTS course_overview (
    course_id text PRIMARY KEY,
    total_enrolled_students int,
    total_videos_watched int,
    total_materials_downloaded int,
    total_assignments_submitted int,
    total_quizzes_completed int,
    avg_assignment_score double,
    avg_quiz_score double,
    total_interactions int,
    last_updated timestamp
);

-- Student Overview
CREATE TABLE IF NOT EXISTS student_overview (
    student_id text PRIMARY KEY,
    total_courses_enrolled int,
    total_videos_watched int,
    total_assignments_submitted int,
    total_quizzes_completed int,
    total_logins int,
    total_materials_downloaded int,
    avg_assignment_score double,
    avg_quiz_score double,
    total_watch_time_minutes double,
    last_login timestamp,
    last_updated timestamp
);

-- Student-Course Enrollment (denormalized for fast lookups)
CREATE TABLE IF NOT EXISTS student_course_enrollment (
    student_id text,
    course_id text,
    enrollment_date timestamp,
    videos_watched int,
    assignments_submitted int,
    quizzes_completed int,
    materials_downloaded int,
    avg_score double,
    last_activity timestamp,
    PRIMARY KEY (student_id, course_id)
);

-- Course-Student Enrollment (reverse lookup)
CREATE TABLE IF NOT EXISTS course_student_enrollment (
    course_id text,
    student_id text,
    enrollment_date timestamp,
    videos_watched int,
    assignments_submitted int,
    quizzes_completed int,
    materials_downloaded int,
    avg_score double,
    last_activity timestamp,
    PRIMARY KEY (course_id, student_id)
);

-- Student-Course Detailed Performance
CREATE TABLE IF NOT EXISTS student_course_detailed (
    student_id text,
    course_id text,
    videos_watched int,
    watch_time_minutes double,
    materials_downloaded int,
    assignments_submitted int,
    quizzes_completed int,
    avg_assignment_score double,
    avg_quiz_score double,
    total_logins int,
    last_activity timestamp,
    PRIMARY KEY (student_id, course_id)
);

-- ========== AUTHENTICATION ANALYTICS ==========

-- Daily Active Users (DAU)
CREATE TABLE IF NOT EXISTS auth_daily_active_users (
    date timestamp,
    daily_active_users int,
    total_sessions int,
    avg_session_duration_minutes double,
    PRIMARY KEY (date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Hourly Login Patterns
CREATE TABLE IF NOT EXISTS auth_hourly_patterns (
    hour_of_day int,
    day_of_week int,
    avg_logins int,
    peak_concurrent_users int,
    PRIMARY KEY (hour_of_day, day_of_week)
);

-- User Session Metrics
CREATE TABLE IF NOT EXISTS user_session_metrics (
    user_id text,
    total_sessions int,
    avg_session_duration_minutes double,
    total_login_time_minutes double,
    last_login timestamp,
    PRIMARY KEY (user_id)
);

-- ========== ASSESSMENT ANALYTICS ==========

-- Student Assessment Submissions
CREATE TABLE IF NOT EXISTS student_submissions (
    student_id text,
    course_id text,
    assignment_count int,
    quiz_count int,
    avg_assignment_score double,
    avg_quiz_score double,
    total_submissions int,
    PRIMARY KEY (student_id, course_id)
);

-- Quiz Performance by Course
CREATE TABLE IF NOT EXISTS quiz_performance (
    course_id text,
    quiz_id text,
    total_attempts int,
    avg_score double,
    pass_rate double,
    PRIMARY KEY (course_id, quiz_id)
);

-- Assignment Performance
CREATE TABLE IF NOT EXISTS assignment_performance (
    course_id text,
    assignment_id text,
    total_submissions int,
    avg_score double,
    on_time_submissions int,
    late_submissions int,
    PRIMARY KEY (course_id, assignment_id)
);

-- ========== VIDEO ANALYTICS ==========

-- Video Watch Statistics
CREATE TABLE IF NOT EXISTS video_watch_stats (
    video_id text PRIMARY KEY,
    total_views int,
    unique_viewers int,
    total_watch_time_minutes double,
    avg_watch_time_minutes double,
    completion_rate double
);

-- Student Video Engagement
CREATE TABLE IF NOT EXISTS student_video_engagement (
    student_id text,
    course_id text,
    total_videos_watched int,
    total_watch_time_minutes double,
    avg_watch_time_minutes double,
    PRIMARY KEY (student_id, course_id)
);

-- Course Video Metrics
CREATE TABLE IF NOT EXISTS course_video_metrics (
    course_id text PRIMARY KEY,
    total_videos int,
    total_views int,
    total_watch_time_minutes double,
    avg_completion_rate double
);

-- ========== COURSE INTERACTION ANALYTICS ==========

-- Material Access Patterns
CREATE TABLE IF NOT EXISTS material_access_patterns (
    material_id text,
    course_id text,
    total_downloads int,
    unique_downloaders int,
    avg_time_to_access_hours double,
    PRIMARY KEY (material_id, course_id)
);

-- Material Popularity
CREATE TABLE IF NOT EXISTS material_popularity (
    course_id text,
    material_id text,
    material_type text,
    download_count int,
    unique_students int,
    PRIMARY KEY (course_id, download_count, material_id)
) WITH CLUSTERING ORDER BY (download_count DESC, material_id ASC);

-- Course Enrollment Stats
CREATE TABLE IF NOT EXISTS course_enrollment_stats (
    course_id text PRIMARY KEY,
    total_enrollments int,
    active_students int,
    avg_materials_per_student double,
    total_material_downloads int
);

-- ========== PROFILE & NOTIFICATION ANALYTICS ==========

-- Profile Update Frequency
CREATE TABLE IF NOT EXISTS profile_update_frequency (
    user_id text PRIMARY KEY,
    total_updates int,
    last_update timestamp,
    update_frequency_days double
);

-- Notification Delivery Stats
CREATE TABLE IF NOT EXISTS notification_delivery_stats (
    notification_type text,
    total_sent int,
    total_delivered int,
    total_read int,
    total_clicked int,
    delivery_rate double,
    read_rate double,
    click_through_rate double,
    PRIMARY KEY (notification_type)
);

-- User Notification Engagement
CREATE TABLE IF NOT EXISTS user_notification_engagement (
    user_id text PRIMARY KEY,
    total_received int,
    total_read int,
    total_clicked int,
    read_rate double,
    click_through_rate double,
    avg_time_to_read_minutes double
);

-- ========== AGGREGATED METRICS (for dashboard) ==========

-- System Summary (single row table for quick dashboard load)
CREATE TABLE IF NOT EXISTS system_summary (
    summary_id text PRIMARY KEY,
    current_active_users int,
    total_active_courses int,
    total_students int,
    total_courses int,
    batch_last_run timestamp,
    speed_layer_status text,
    last_updated timestamp
);

-- Recent Activity Summary
CREATE TABLE IF NOT EXISTS recent_activity (
    time_period text PRIMARY KEY,
    videos_watched int,
    materials_downloaded int,
    assignments_submitted int,
    quizzes_completed int,
    active_users int,
    window_start timestamp,
    window_end timestamp
);

-- Student Engagement Distribution
CREATE TABLE IF NOT EXISTS engagement_distribution (
    category text PRIMARY KEY,
    student_count int,
    percentage double,
    last_updated timestamp
);

-- ========== INDEXES FOR COMMON QUERIES ==========

-- Index for querying courses by activity
CREATE INDEX IF NOT EXISTS idx_course_last_updated
ON course_overview (last_updated);

-- Index for querying students by last login
CREATE INDEX IF NOT EXISTS idx_student_last_login
ON student_overview (last_login);

-- Index for material popularity by type
CREATE INDEX IF NOT EXISTS idx_material_type
ON material_popularity (material_type);
