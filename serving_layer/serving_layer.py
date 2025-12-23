import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytz
import s3fs
from fastapi import FastAPI, HTTPException

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")

app = FastAPI(title="University Learning Analytics API - Batch Layer (MinIO)")

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bucket-0"

logger.info(
    f"MinIO Batch API Starting - Endpoint: {MINIO_ENDPOINT}, Bucket: {BUCKET_NAME}"
)

# S3 Filesystem
fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    use_listings_cache=False,
)


def read_parquet(path):
    """Helper to read parquet path from MinIO"""
    full_path = f"{BUCKET_NAME}/{path}"
    logger.info(f"Reading parquet from: {full_path}")
    try:
        import pyarrow.parquet as pq

        if fs.exists(full_path):
            table = pq.read_table(f"{full_path}", filesystem=fs)
            df = table.to_pandas()
            logger.info(f"Successfully read {len(df)} rows from {full_path}")
            return df
        else:
            logger.warning(f"Path does not exist: {full_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {full_path}: {e}", exc_info=True)
        return pd.DataFrame()


@app.get("/")
def root():
    logger.info("Root endpoint called")
    return {"status": "ok", "service": "serving_layer_batch", "backend": "minio"}


@app.get("/health")
def health_check():
    logger.info("Health check endpoint called")
    return {"status": "ok", "backend": "minio", "layer": "batch"}


# ========== BATCH LAYER SUMMARY METRICS ==========


@app.get("/analytics/batch/summary")
def get_batch_summary():
    """
    Get batch layer summary metrics
    Returns: total courses, total students, batch job status
    """
    logger.info("GET /analytics/batch/summary - Fetching batch summary metrics")
    summary = {
        "total_students": 0,
        "total_courses": 0,
        "batch_last_run": None,
    }

    # Total students from batch layer
    df_students = read_parquet("batch_views/student_overview")
    if not df_students.empty:
        summary["total_students"] = len(df_students)

    # Total courses from batch layer
    df_courses = read_parquet("batch_views/course_overview")
    if not df_courses.empty:
        summary["total_courses"] = len(df_courses)
        if "last_updated" in df_courses.columns:
            summary["batch_last_run"] = df_courses["last_updated"].max().isoformat()

    return summary


@app.get("/analytics/batch/student_engagement_distribution")
def get_student_engagement_distribution():
    """
    Get distribution of student engagement levels from batch data
    Categories: Highly Active, Moderately Active, Low Activity, Inactive
    """
    logger.info("GET /analytics/batch/student_engagement_distribution")

    distribution = {
        "highly_active": 0,  # >50 interactions total
        "moderately_active": 0,  # 10-50 interactions
        "low_activity": 0,  # 1-9 interactions
        "inactive": 0,  # 0 interactions
    }

    # Get student activity from batch overview
    df_students = read_parquet("batch_views/student_overview")
    if df_students.empty:
        return distribution

    # Calculate total interactions per student
    if "total_videos_watched" in df_students.columns:
        df_students["total_interactions"] = (
            df_students.get("total_videos_watched", 0)
            + df_students.get("total_assignments_submitted", 0)
            + df_students.get("total_logins", 0)
        )

        distribution["highly_active"] = len(
            df_students[df_students["total_interactions"] > 50]
        )
        distribution["moderately_active"] = len(
            df_students[
                (df_students["total_interactions"] >= 10)
                & (df_students["total_interactions"] <= 50)
            ]
        )
        distribution["low_activity"] = len(
            df_students[
                (df_students["total_interactions"] >= 1)
                & (df_students["total_interactions"] < 10)
            ]
        )
        distribution["inactive"] = len(
            df_students[df_students["total_interactions"] == 0]
        )

    return distribution


@app.get("/analytics/batch/dau")
def get_historical_dau(hours: int = 6):
    """
    Get historical Daily Active Users from batch layer
    """
    logger.info(f"GET /analytics/batch/dau - hours={hours}")

    df_batch = read_parquet("batch_views/auth_daily_active_users")
    now_vn = datetime.now(vn_tz).replace(tzinfo=None)

    api_response = []

    if not df_batch.empty:
        cutoff_time = now_vn - timedelta(hours=hours)
        df_batch["date"] = pd.to_datetime(df_batch["date"])
        df_batch = df_batch[df_batch["date"] >= cutoff_time]

        for _, row in df_batch.iterrows():
            api_response.append(
                {
                    "date": row["date"].strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(row["date"], "strftime")
                    else str(row["date"]),
                    "users": int(row["daily_active_users"]),
                    "source": "batch",
                }
            )

    api_response.sort(key=lambda x: x["date"])
    return api_response


@app.get("/analytics/batch/recent_activity")
def get_batch_activity_stats():
    """
    Get cumulative activity stats from batch layer
    Returns: total videos watched, materials downloaded (cumulative)
    """
    logger.info("GET /analytics/batch/recent_activity")

    activity = {
        "total_videos_watched": 0,
        "total_materials_downloaded": 0,
    }

    df_course = read_parquet("batch_views/course_overview")
    if not df_course.empty:
        if "total_videos_watched" in df_course.columns:
            activity["total_videos_watched"] = int(
                df_course["total_videos_watched"].sum()
            )
        if "total_materials_downloaded" in df_course.columns:
            activity["total_materials_downloaded"] = int(
                df_course["total_materials_downloaded"].sum()
            )

    return activity


# ========== COURSE ENDPOINTS ==========


@app.get("/analytics/courses")
def get_all_courses():
    """List all courses with summary stats from batch layer"""
    logger.info("GET /analytics/courses")
    df = read_parquet("batch_views/course_overview")
    if df.empty:
        return []
    return df.to_dict("records")


@app.get("/analytics/course/{course_id}")
def get_course_details(course_id: str):
    """Get detailed stats for a specific course from batch layer"""
    logger.info(f"GET /analytics/course/{course_id}")
    df = read_parquet("batch_views/course_overview")
    if df.empty:
        return {}
    course = df[df["course_id"] == course_id]
    if course.empty:
        raise HTTPException(status_code=404, detail="Course not found")
    return course.to_dict("records")[0]


@app.get("/analytics/course/{course_id}/students")
def get_course_students(course_id: str):
    """Get all students enrolled in a course with their performance from batch layer"""
    logger.info(f"GET /analytics/course/{course_id}/students")
    df = read_parquet("batch_views/student_course_enrollment")
    if df.empty:
        return []
    students = df[df["course_id"] == course_id]
    return students.to_dict("records")


#  ========== STUDENT ENDPOINTS ==========


@app.get("/analytics/students")
def get_all_students():
    """List all students with summary stats from batch layer"""
    logger.info("GET /analytics/students")
    df = read_parquet("batch_views/student_overview")
    if df.empty:
        return []
    return df.to_dict("records")


@app.get("/analytics/student/{student_id}")
def get_student_details(student_id: str):
    """Get detailed stats for a specific student from batch layer"""
    logger.info(f"GET /analytics/student/{student_id}")
    df = read_parquet("batch_views/student_overview")
    if df.empty:
        return {}
    student = df[df["student_id"] == student_id]
    if student.empty:
        raise HTTPException(status_code=404, detail="Student not found")
    return student.to_dict("records")[0]


@app.get("/analytics/student/{student_id}/courses")
def get_student_courses(student_id: str):
    """Get all courses a student is enrolled in with performance from batch layer"""
    logger.info(f"GET /analytics/student/{student_id}/courses")
    df = read_parquet("batch_views/student_course_enrollment")
    if df.empty:
        return []
    courses = df[df["student_id"] == student_id]
    return courses.to_dict("records")


@app.get("/analytics/student/{student_id}/course/{course_id}")
def get_student_course_performance(student_id: str, course_id: str):
    """Get detailed performance of a student in a specific course from batch layer"""
    logger.info(f"GET /analytics/student/{student_id}/course/{course_id}")
    df = read_parquet("batch_views/student_course_detailed")
    if df.empty:
        return {}
    perf = df[(df["student_id"] == student_id) & (df["course_id"] == course_id)]
    if perf.empty:
        raise HTTPException(status_code=404, detail="Student-course record not found")
    return perf.to_dict("records")[0]


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting MinIO Batch API server on 0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
