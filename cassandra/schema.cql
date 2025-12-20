-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS lms_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE lms_analytics;

-- ==========================================
-- AUTH TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS auth_daily_active_users (
    date text,
    daily_active_users bigint,
    total_sessions bigint,
    PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS auth_hourly_login_patterns (
    date text,
    hour int,
    login_count bigint,
    PRIMARY KEY ((date), hour)
);

CREATE TABLE IF NOT EXISTS auth_user_session_metrics (
    user_id text PRIMARY KEY,
    total_sessions bigint,
    avg_session_duration_minutes double,
    min_session_duration_minutes double,
    max_session_duration_minutes double,
    total_time_online_minutes double
);

CREATE TABLE IF NOT EXISTS auth_user_activity_summary (
    user_id text,
    role text,
    total_logins bigint,
    total_logouts bigint,
    first_activity timestamp,
    last_activity timestamp,
    unique_sessions bigint,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS auth_registration_analytics (
    date text,
    registration_source text,
    role text,
    new_signups bigint,
    PRIMARY KEY ((date), registration_source, role)
);


-- ==========================================
-- ASSESSMENT TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS assessment_student_submissions (
    user_id text,
    course_id text,
    total_assignments_submitted bigint,
    total_submissions bigint,
    first_submission timestamp,
    last_submission timestamp,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS assessment_engagement_timeline (
    user_id text,
    course_id text,
    assignments_completed bigint,
    avg_hours_to_submit double,
    min_hours_to_submit double,
    max_hours_to_submit double,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS assessment_quiz_performance (
    user_id text,
    course_id text,
    total_quizzes_completed bigint,
    avg_correct_answers double,
    total_correct_answers bigint,
    min_correct_answers bigint,
    max_correct_answers bigint,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS assessment_grading_stats (
    student_id text,
    course_id text,
    total_assignments_graded bigint,
    avg_assignment_score double,
    min_assignment_score bigint,
    max_assignment_score bigint,
    total_assignment_score bigint,
    PRIMARY KEY ((student_id), course_id)
);

CREATE TABLE IF NOT EXISTS assessment_teacher_workload (
    user_id text,
    course_id text,
    total_works_viewed bigint,
    total_assignments_graded bigint,
    avg_score_given double,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS assessment_submission_distribution (
    date text,
    course_id text,
    assignment_type text,
    submissions_count bigint,
    unique_students bigint,
    PRIMARY KEY ((date), course_id, assignment_type)
);

CREATE TABLE IF NOT EXISTS assessment_student_overall_performance (
    user_id text,
    course_id text,
    quizzes_completed bigint,
    avg_quiz_correct double,
    assignments_graded bigint,
    avg_assignment_score double,
    PRIMARY KEY ((user_id), course_id)
);

-- ==========================================
-- VIDEO TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS video_total_watch_time (
    user_id text,
    video_id text,
    course_id text,
    total_watch_seconds bigint,
    total_watch_sessions bigint,
    avg_watch_duration_seconds double,
    longest_session_seconds bigint,
    shortest_session_seconds bigint,
    total_watch_minutes double,
    total_watch_hours double,
    PRIMARY KEY ((user_id), course_id, video_id)
);

CREATE TABLE IF NOT EXISTS video_student_engagement (
    user_id text,
    course_id text,
    unique_videos_watched bigint,
    total_watch_seconds bigint,
    total_video_events bigint,
    avg_watch_duration_seconds double,
    total_watch_hours double,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS video_popularity (
    video_id text,
    course_id text,
    unique_viewers bigint,
    total_views_seconds bigint,
    total_view_events bigint,
    avg_view_duration_seconds double,
    total_views_hours double,
    PRIMARY KEY ((video_id), course_id)
);

CREATE TABLE IF NOT EXISTS video_daily_engagement (
    date text,
    course_id text,
    active_video_watchers bigint,
    unique_videos_watched bigint,
    total_watch_seconds bigint,
    total_watch_events bigint,
    total_watch_hours double,
    PRIMARY KEY ((date), course_id)
);

CREATE TABLE IF NOT EXISTS video_course_metrics (
    course_id text PRIMARY KEY,
    total_students_watching bigint,
    total_videos_in_course bigint,
    total_course_watch_seconds bigint,
    total_course_watch_hours double
);

CREATE TABLE IF NOT EXISTS video_student_course_summary (
    user_id text,
    course_id text,
    videos_watched_count bigint,
    total_watch_seconds bigint,
    total_sessions bigint,
    total_watch_hours double,
    total_videos_in_course bigint,
    video_coverage_percentage decimal,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS video_drop_off_indicators (
    video_id text,
    course_id text,
    total_views bigint,
    avg_watch_duration double,
    min_watch_duration bigint,
    unique_viewers bigint,
    potential_drop_off text,
    PRIMARY KEY ((video_id), course_id)
);


-- ==========================================
-- COURSE TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS course_enrollment_stats (
    date text,
    course_id text,
    new_enrollments bigint,
    unique_new_students bigint,
    PRIMARY KEY ((date), course_id)
);

CREATE TABLE IF NOT EXISTS course_material_access (
    user_id text,
    course_id text,
    total_material_accesses bigint,
    unique_materials_accessed bigint,
    first_access timestamp,
    last_access timestamp,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS course_material_popularity (
    course_id text,
    material_id text,
    material_type text,
    unique_accessors bigint,
    total_accesses bigint,
    PRIMARY KEY ((course_id), material_id, material_type)
);

CREATE TABLE IF NOT EXISTS course_download_analytics (
    user_id text,
    course_id text,
    total_downloads bigint,
    unique_resources_downloaded bigint,
    PRIMARY KEY ((user_id), course_id)
);

CREATE TABLE IF NOT EXISTS course_resource_download_stats (
    course_id text,
    resource_id text,
    unique_downloaders bigint,
    total_download_count bigint,
    PRIMARY KEY ((course_id), resource_id)
);

CREATE TABLE IF NOT EXISTS course_activity_summary (
    user_id text PRIMARY KEY,
    courses_enrolled bigint,
    total_material_accesses bigint,
    courses_with_material_access bigint,
    total_downloads bigint
);

CREATE TABLE IF NOT EXISTS course_daily_engagement (
    date text,
    course_id text,
    event_type text,
    unique_users bigint,
    event_count bigint,
    PRIMARY KEY ((date), course_id, event_type)
);

CREATE TABLE IF NOT EXISTS course_overall_metrics (
    course_id text PRIMARY KEY,
    total_enrollments bigint,
    total_material_accesses bigint,
    total_downloads bigint,
    unique_students_interacting bigint
);


-- ==========================================
-- PROFILE & NOTIFICATION TABLES
-- ==========================================
CREATE TABLE IF NOT EXISTS profile_update_frequency (
    user_id text PRIMARY KEY,
    total_profile_updates bigint,
    first_update timestamp,
    last_update timestamp
);

CREATE TABLE IF NOT EXISTS profile_field_changes (
    field_name text PRIMARY KEY,
    total_updates bigint,
    unique_users_updating bigint
);

CREATE TABLE IF NOT EXISTS profile_avatar_changes (
    user_id text PRIMARY KEY,
    avatar_changes_count bigint,
    first_change timestamp,
    last_change timestamp
);

CREATE TABLE IF NOT EXISTS profile_daily_activity (
    date text,
    event_type text,
    unique_users bigint,
    event_count bigint,
    PRIMARY KEY ((date), event_type)
);

CREATE TABLE IF NOT EXISTS notification_delivery_stats (
    notification_type text PRIMARY KEY,
    total_sent bigint,
    unique_recipients bigint
);

CREATE TABLE IF NOT EXISTS notification_engagement (
    user_id text,
    notification_type text,
    notifications_clicked bigint,
    PRIMARY KEY ((user_id), notification_type)
);

CREATE TABLE IF NOT EXISTS notification_click_through_rate (
    notification_type text PRIMARY KEY,
    total_sent bigint,
    total_clicked bigint,
    click_through_rate decimal
);

CREATE TABLE IF NOT EXISTS notification_user_preferences (
    user_id text,
    notification_type text,
    notifications_received bigint,
    notifications_clicked bigint,
    engagement_rate decimal,
    PRIMARY KEY ((user_id), notification_type)
);

CREATE TABLE IF NOT EXISTS notification_daily_activity (
    date text,
    event_type text,
    notification_type text,
    unique_users bigint,
    event_count bigint,
    PRIMARY KEY ((date), event_type, notification_type)
);

CREATE TABLE IF NOT EXISTS notification_user_summary (
    user_id text PRIMARY KEY,
    total_received bigint,
    total_clicked bigint,
    overall_engagement_rate decimal
);
