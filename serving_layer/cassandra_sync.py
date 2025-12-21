"""
Cassandra Sync Service
Syncs data from MinIO (Parquet files) to Cassandra for fast serving layer queries
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import s3fs
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = "university_analytics"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bucket-0"

SYNC_INTERVAL = int(os.environ.get("SYNC_INTERVAL", "300"))  # 5 minutes default


class CassandraSync:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.fs = None
        self.init_cassandra()
        self.init_minio()

    def init_cassandra(self):
        """Initialize Cassandra connection"""
        try:
            logger.info(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            self.cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            self.session = self.cluster.connect()

            # Wait for Cassandra to be ready
            max_retries = 30
            for i in range(max_retries):
                try:
                    self.session.execute("SELECT now() FROM system.local")
                    logger.info("Cassandra connection established")
                    break
                except Exception as e:
                    if i < max_retries - 1:
                        logger.warning(
                            f"Waiting for Cassandra... ({i + 1}/{max_retries})"
                        )
                        time.sleep(10)
                    else:
                        raise

            # Create keyspace if not exists
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            self.session.set_keyspace(CASSANDRA_KEYSPACE)
            logger.info(f"Using keyspace: {CASSANDRA_KEYSPACE}")

        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def init_minio(self):
        """Initialize MinIO S3 filesystem"""
        try:
            self.fs = s3fs.S3FileSystem(
                client_kwargs={"endpoint_url": MINIO_ENDPOINT},
                key=MINIO_ACCESS_KEY,
                secret=MINIO_SECRET_KEY,
                use_listings_cache=False,
            )
            logger.info("MinIO S3 filesystem initialized")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO: {e}")
            raise

    def read_parquet(self, path):
        """Read Parquet file from MinIO"""
        full_path = f"{BUCKET_NAME}/{path}"
        try:
            if self.fs.exists(full_path):
                table = pq.read_table(full_path, filesystem=self.fs)
                df = table.to_pandas()
                logger.info(f"Read {len(df)} rows from {path}")
                return df
            else:
                logger.warning(f"Path does not exist: {full_path}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading {full_path}: {e}")
            return pd.DataFrame()

    def sync_speed_layer_views(self):
        """Sync speed layer views to Cassandra"""
        logger.info("Syncing speed layer views...")

        # Active Users
        try:
            df = self.read_parquet("speed_views/active_users")
            if not df.empty:
                insert_stmt = self.session.prepare("""
                    INSERT INTO active_users (window_start, window_end, active_users, source)
                    VALUES (?, ?, ?, ?)
                """)
                for _, row in df.iterrows():
                    self.session.execute(
                        insert_stmt,
                        (
                            pd.to_datetime(row["start"]),
                            pd.to_datetime(row["end"]),
                            int(row["active_users"]),
                            "speed",
                        ),
                    )
                logger.info(f"Synced {len(df)} active_users records")
        except Exception as e:
            logger.error(f"Error syncing active_users: {e}")

        # Course Popularity
        try:
            df = self.read_parquet("speed_views/course_popularity")
            if not df.empty:
                insert_stmt = self.session.prepare("""
                    INSERT INTO course_popularity (course_id, window_start, window_end, interactions, views)
                    VALUES (?, ?, ?, ?, ?)
                """)
                for _, row in df.iterrows():
                    self.session.execute(
                        insert_stmt,
                        (
                            str(row["course_id"]),
                            pd.to_datetime(row["start"]),
                            pd.to_datetime(row["end"]),
                            int(row["interactions"]),
                            int(row.get("views", 0)),
                        ),
                    )
                logger.info(f"Synced {len(df)} course_popularity records")
        except Exception as e:
            logger.error(f"Error syncing course_popularity: {e}")

        # Video Engagement
        try:
            df = self.read_parquet("speed_views/video_engagement")
            if not df.empty:
                insert_stmt = self.session.prepare("""
                    INSERT INTO video_engagement (video_id, window_start, window_end, views, unique_viewers, total_watch_seconds)
                    VALUES (?, ?, ?, ?, ?, ?)
                """)
                for _, row in df.iterrows():
                    self.session.execute(
                        insert_stmt,
                        (
                            str(row["video_id"]),
                            pd.to_datetime(row["start"]),
                            pd.to_datetime(row["end"]),
                            int(row["views"]),
                            int(row.get("unique_viewers", 0)),
                            int(row.get("total_watch_seconds", 0)),
                        ),
                    )
                logger.info(f"Synced {len(df)} video_engagement records")
        except Exception as e:
            logger.error(f"Error syncing video_engagement: {e}")

    def sync_batch_layer_views(self):
        """Sync batch layer views to Cassandra"""
        logger.info("Syncing batch layer views...")

        # Course Overview
        try:
            df = self.read_parquet("batch_views/course_overview")
            if not df.empty:
                # Truncate table before inserting (full refresh)
                self.session.execute("TRUNCATE course_overview")

                insert_stmt = self.session.prepare("""
                    INSERT INTO course_overview (
                        course_id, total_enrolled_students, total_videos_watched,
                        total_materials_downloaded, total_assignments_submitted,
                        total_quizzes_completed, avg_assignment_score, avg_quiz_score,
                        total_interactions, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)

                for _, row in df.iterrows():
                    self.session.execute(
                        insert_stmt,
                        (
                            str(row["course_id"]),
                            int(row.get("total_enrolled_students", 0)),
                            int(row.get("total_videos_watched", 0)),
                            int(row.get("total_materials_downloaded", 0)),
                            int(row.get("total_assignments_submitted", 0)),
                            int(row.get("total_quizzes_completed", 0)),
                            float(row.get("avg_assignment_score", 0.0)),
                            float(row.get("avg_quiz_score", 0.0)),
                            int(row.get("total_interactions", 0)),
                            datetime.now(timezone.utc),
                        ),
                    )
                logger.info(f"Synced {len(df)} course_overview records")
        except Exception as e:
            logger.error(f"Error syncing course_overview: {e}")

        # Student Overview
        try:
            df = self.read_parquet("batch_views/student_overview")
            if not df.empty:
                self.session.execute("TRUNCATE student_overview")

                insert_stmt = self.session.prepare("""
                    INSERT INTO student_overview (
                        student_id, total_courses_enrolled, total_videos_watched,
                        total_assignments_submitted, total_quizzes_completed,
                        total_logins, total_materials_downloaded, avg_assignment_score,
                        avg_quiz_score, total_watch_time_minutes, last_login, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)

                for _, row in df.iterrows():
                    last_login = row.get("last_login")
                    if pd.notna(last_login):
                        last_login = pd.to_datetime(last_login)
                    else:
                        last_login = None

                    self.session.execute(
                        insert_stmt,
                        (
                            str(row["student_id"]),
                            int(row.get("total_courses_enrolled", 0)),
                            int(row.get("total_videos_watched", 0)),
                            int(row.get("total_assignments_submitted", 0)),
                            int(row.get("total_quizzes_completed", 0)),
                            int(row.get("total_logins", 0)),
                            int(row.get("total_materials_downloaded", 0)),
                            float(row.get("avg_assignment_score", 0.0)),
                            float(row.get("avg_quiz_score", 0.0)),
                            float(row.get("total_watch_time_minutes", 0.0)),
                            last_login,
                            datetime.now(timezone.utc),
                        ),
                    )
                logger.info(f"Synced {len(df)} student_overview records")
        except Exception as e:
            logger.error(f"Error syncing student_overview: {e}")

        # Student-Course Enrollment (dual tables for bi-directional lookup)
        try:
            df = self.read_parquet("batch_views/student_course_enrollment")
            if not df.empty:
                self.session.execute("TRUNCATE student_course_enrollment")
                self.session.execute("TRUNCATE course_student_enrollment")

                insert_student_stmt = self.session.prepare("""
                    INSERT INTO student_course_enrollment (
                        student_id, course_id, enrollment_date, videos_watched,
                        assignments_submitted, quizzes_completed, materials_downloaded,
                        avg_score, last_activity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)

                insert_course_stmt = self.session.prepare("""
                    INSERT INTO course_student_enrollment (
                        course_id, student_id, enrollment_date, videos_watched,
                        assignments_submitted, quizzes_completed, materials_downloaded,
                        avg_score, last_activity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)

                for _, row in df.iterrows():
                    enrollment_date = row.get("enrollment_date")
                    if pd.notna(enrollment_date):
                        enrollment_date = pd.to_datetime(enrollment_date)
                    else:
                        enrollment_date = None

                    last_activity = row.get("last_activity")
                    if pd.notna(last_activity):
                        last_activity = pd.to_datetime(last_activity)
                    else:
                        last_activity = None

                    params = (
                        str(row["student_id"]),
                        str(row["course_id"]),
                        enrollment_date,
                        int(row.get("videos_watched", 0)),
                        int(row.get("assignments_submitted", 0)),
                        int(row.get("quizzes_completed", 0)),
                        int(row.get("materials_downloaded", 0)),
                        float(row.get("avg_score", 0.0)),
                        last_activity,
                    )

                    # Insert into both tables
                    self.session.execute(insert_student_stmt, params)
                    self.session.execute(
                        insert_course_stmt,
                        (
                            params[1],
                            params[0],
                            params[2],
                            params[3],
                            params[4],
                            params[5],
                            params[6],
                            params[7],
                            params[8],
                        ),
                    )

                logger.info(f"Synced {len(df)} enrollment records to both tables")
        except Exception as e:
            logger.error(f"Error syncing enrollment: {e}")

        # Student-Course Detailed
        try:
            df = self.read_parquet("batch_views/student_course_detailed")
            if not df.empty:
                self.session.execute("TRUNCATE student_course_detailed")

                insert_stmt = self.session.prepare("""
                    INSERT INTO student_course_detailed (
                        student_id, course_id, videos_watched, watch_time_minutes,
                        materials_downloaded, assignments_submitted, quizzes_completed,
                        avg_assignment_score, avg_quiz_score, total_logins, last_activity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)

                for _, row in df.iterrows():
                    last_activity = row.get("last_activity")
                    if pd.notna(last_activity):
                        last_activity = pd.to_datetime(last_activity)
                    else:
                        last_activity = None

                    self.session.execute(
                        insert_stmt,
                        (
                            str(row["student_id"]),
                            str(row["course_id"]),
                            int(row.get("videos_watched", 0)),
                            float(row.get("watch_time_minutes", 0.0)),
                            int(row.get("materials_downloaded", 0)),
                            int(row.get("assignments_submitted", 0)),
                            int(row.get("quizzes_completed", 0)),
                            float(row.get("avg_assignment_score", 0.0)),
                            float(row.get("avg_quiz_score", 0.0)),
                            int(row.get("total_logins", 0)),
                            last_activity,
                        ),
                    )
                logger.info(f"Synced {len(df)} student_course_detailed records")
        except Exception as e:
            logger.error(f"Error syncing student_course_detailed: {e}")

        # Daily Active Users
        try:
            df = self.read_parquet("batch_views/auth_daily_active_users")
            if not df.empty:
                self.session.execute("TRUNCATE auth_daily_active_users")

                insert_stmt = self.session.prepare("""
                    INSERT INTO auth_daily_active_users (
                        date, daily_active_users, total_sessions, avg_session_duration_minutes
                    ) VALUES (?, ?, ?, ?)
                """)

                for _, row in df.iterrows():
                    self.session.execute(
                        insert_stmt,
                        (
                            pd.to_datetime(row["date"]),
                            int(row.get("daily_active_users", 0)),
                            int(row.get("total_sessions", 0)),
                            float(row.get("avg_session_duration_minutes", 0.0)),
                        ),
                    )
                logger.info(f"Synced {len(df)} auth_daily_active_users records")
        except Exception as e:
            logger.error(f"Error syncing auth_daily_active_users: {e}")

    def update_system_summary(self):
        """Update system summary metrics for dashboard"""
        try:
            # Get current active users from speed layer
            df_speed = self.read_parquet("speed_views/active_users")
            current_active_users = 0
            if not df_speed.empty:
                latest = df_speed.sort_values("start", ascending=False).iloc[0]
                current_active_users = int(latest["active_users"])

            # Get total courses from batch layer
            df_courses = self.read_parquet("batch_views/course_overview")
            total_courses = len(df_courses) if not df_courses.empty else 0

            # Get total students from batch layer
            df_students = self.read_parquet("batch_views/student_overview")
            total_students = len(df_students) if not df_students.empty else 0

            # Get active courses (courses with activity in last 24h)
            df_course_pop = self.read_parquet("speed_views/course_popularity")
            total_active_courses = 0
            if not df_course_pop.empty:
                total_active_courses = df_course_pop["course_id"].nunique()

            # Update summary table
            self.session.execute(
                """
                INSERT INTO system_summary (
                    summary_id, current_active_users, total_active_courses,
                    total_students, total_courses, batch_last_run,
                    speed_layer_status, last_updated
                ) VALUES ('main', ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    current_active_users,
                    total_active_courses,
                    total_students,
                    total_courses,
                    datetime.now(timezone.utc),
                    "healthy",
                    datetime.now(timezone.utc),
                ),
            )

            logger.info("Updated system summary")
        except Exception as e:
            logger.error(f"Error updating system summary: {e}")

    def run_sync(self):
        """Run a complete sync cycle"""
        logger.info("=" * 60)
        logger.info("Starting sync cycle")
        start_time = time.time()

        try:
            self.sync_speed_layer_views()
            self.sync_batch_layer_views()
            self.update_system_summary()

            elapsed = time.time() - start_time
            logger.info(f"Sync completed in {elapsed:.2f} seconds")
        except Exception as e:
            logger.error(f"Error during sync: {e}")

        logger.info("=" * 60)

    def run_continuous(self):
        """Run continuous sync loop"""
        logger.info(f"Starting continuous sync with {SYNC_INTERVAL}s interval")

        while True:
            try:
                self.run_sync()
                logger.info(f"Waiting {SYNC_INTERVAL} seconds until next sync...")
                time.sleep(SYNC_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(30)  # Wait before retry

        self.cleanup()

    def cleanup(self):
        """Clean up connections"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("Cassandra connection closed")


def main():
    """Main entry point"""
    sync_service = CassandraSync()

    # Check if running in one-shot mode
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        sync_service.run_sync()
        sync_service.cleanup()
    else:
        sync_service.run_continuous()


if __name__ == "__main__":
    main()
