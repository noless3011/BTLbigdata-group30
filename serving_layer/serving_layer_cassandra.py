import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytz
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from fastapi import FastAPI, HTTPException

vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")

app = FastAPI(title="University Learning Analytics API - Cassandra Backend")

# Configuration
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = "university_analytics"

# Global Cassandra connection
cluster = None
session = None


def get_cassandra_session():
    """Get or create Cassandra session"""
    global cluster, session
    if session is None:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
    return session


@app.on_event("startup")
async def startup_event():
    """Initialize Cassandra connection on startup"""
    get_cassandra_session()


@app.on_event("shutdown")
async def shutdown_event():
    """Close Cassandra connection on shutdown"""
    global cluster, session
    if session:
        session.shutdown()
    if cluster:
        cluster.shutdown()


def query_to_dict_list(rows) -> List[Dict[str, Any]]:
    """Convert Cassandra rows to list of dictionaries"""
    if not rows:
        return []

    result = []
    for row in rows:
        row_dict = {}
        for key, value in row._asdict().items():
            # Convert datetime to ISO format string
            if isinstance(value, datetime):
                row_dict[key] = value.isoformat()
            else:
                row_dict[key] = value
        result.append(row_dict)
    return result


@app.get("/")
def health_check():
    return {
        "status": "ok",
        "service": "serving_layer_cassandra",
        "backend": "cassandra",
    }


@app.get("/analytics/summary")
def get_summary_metrics():
    """
    Get all key metrics for dashboard overview in one call
    Returns: current active users, total courses, total students, system health
    """
    sess = get_cassandra_session()

    summary = {
        "current_active_users": 0,
        "total_active_courses": 0,
        "total_students": 0,
        "batch_last_run": None,
        "speed_layer_status": "unknown",
    }

    try:
        # Get system summary (pre-aggregated)
        rows = sess.execute("SELECT * FROM system_summary WHERE summary_id = 'main'")
        row = rows.one()

        if row:
            summary["current_active_users"] = row.current_active_users or 0
            summary["total_active_courses"] = row.total_active_courses or 0
            summary["total_students"] = row.total_students or 0
            summary["speed_layer_status"] = row.speed_layer_status or "unknown"
            if row.batch_last_run:
                summary["batch_last_run"] = row.batch_last_run.isoformat()
    except Exception as e:
        # Fallback to individual queries if system_summary doesn't exist
        try:
            # Get current active users from speed layer
            rows = sess.execute("""
                SELECT SUM(views) as total_views FROM video_engagement
                    LIMIT 1000
            """)
            row = rows.one()
            if row:
                summary["current_active_users"] = row.active_users
        except:
            pass

        try:
            # Get total students
            rows = sess.execute("SELECT COUNT(*) as count FROM student_overview")
            row = rows.one()
            if row:
                summary["total_students"] = row.count
        except:
            pass

        try:
            # Get active courses count
            rows = sess.execute(
                "SELECT COUNT(DISTINCT course_id) as count FROM course_popularity"
            )
            row = rows.one()
            if row:
                summary["total_active_courses"] = row.count
        except:
            pass

    return summary


@app.get("/analytics/recent_activity")
def get_recent_activity(hours: int = 1):
    """
    Get recent activity stats for content consumption
    Returns: videos watched, materials downloaded in last N hours
    """
    sess = get_cassandra_session()

    activity = {
        "videos_watched": 0,
        "materials_downloaded": 0,
        "time_period_hours": hours,
    }

    try:
        # Check if we have pre-aggregated recent activity
        rows = sess.execute(f"""
            SELECT * FROM recent_activity
            WHERE time_period = '{hours}h'
        """)
        row = rows.one()

        if row:
            activity["videos_watched"] = row.videos_watched or 0
            activity["materials_downloaded"] = row.materials_downloaded or 0
        else:
            # Fallback: aggregate from speed layer
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            # Videos from speed layer
            rows = sess.execute(f"""
                SELECT SUM(views) as total_views FROM video_engagement
                WHERE window_start >= '{cutoff.isoformat()}'
            """)
            row = rows.one()
            if row and row.total_views:
                activity["videos_watched"] = int(row.total_views)

            # Materials from course overview (cumulative, not time-filtered)
            rows = sess.execute("""
                SELECT SUM(total_materials_downloaded) as total
                FROM course_overview
            """)
            row = rows.one()
            if row and row.total:
                activity["materials_downloaded"] = int(row.total)

    except Exception as e:
        print(f"Error getting recent activity: {e}")

    return activity


@app.get("/analytics/student_engagement_distribution")
def get_student_engagement_distribution():
    """
    Get distribution of student engagement levels
    Categories: Highly Active, Moderately Active, Low Activity, Inactive
    """
    sess = get_cassandra_session()

    distribution = {
        "highly_active": 0,
        "moderately_active": 0,
        "low_activity": 0,
        "inactive": 0,
    }

    try:
        # Check if we have pre-aggregated distribution
        rows = sess.execute("SELECT * FROM engagement_distribution")

        for row in rows:
            category = row.category.lower().replace(" ", "_")
            if category in distribution:
                distribution[category] = row.student_count or 0

        # If no pre-aggregated data, calculate from student_overview
        if sum(distribution.values()) == 0:
            # This is a simplified calculation
            # In production, you'd want to calculate based on recent activity
            rows = sess.execute("""
                SELECT total_videos_watched, total_assignments_submitted, total_logins
                FROM student_overview
            """)

            for row in rows:
                total_interactions = (
                    (row.total_videos_watched or 0)
                    + (row.total_assignments_submitted or 0)
                    + (row.total_logins or 0)
                )

                if total_interactions > 50:
                    distribution["highly_active"] += 1
                elif total_interactions >= 10:
                    distribution["moderately_active"] += 1
                elif total_interactions >= 1:
                    distribution["low_activity"] += 1
                else:
                    distribution["inactive"] += 1

    except Exception as e:
        print(f"Error getting engagement distribution: {e}")

    return distribution


@app.get("/analytics/dau")
def get_daily_active_users(hours: int = 6):
    """
    Get DAU by merging Batch Layer (Historical) and Speed Layer (Real-time)
    """
    sess = get_cassandra_session()
    api_response = []

    try:
        # Get batch data (historical)
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        rows = sess.execute(f"""
            SELECT date, daily_active_users
            FROM auth_daily_active_users
            WHERE date >= '{cutoff_time.isoformat()}'
        """)

        for row in rows:
            api_response.append(
                {
                    "date": row.date.strftime("%Y-%m-%d %H:%M:%S"),
                    "users": row.daily_active_users or 0,
                    "source": "batch",
                }
            )

        # Get speed data (real-time)
        rows = sess.execute(f"""
            SELECT window_start, active_users
            FROM active_users
            WHERE window_start >= '{cutoff_time.isoformat()}'
        """)

        for row in rows:
            # Convert UTC to Vietnam time for display
            vn_time = row.window_start.replace(tzinfo=timezone.utc).astimezone(vn_tz)
            api_response.append(
                {
                    "date": vn_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "users": row.active_users or 0,
                    "source": "speed",
                }
            )

        # Sort by date
        api_response.sort(key=lambda x: x["date"])

    except Exception as e:
        print(f"Error getting DAU: {e}")

    return api_response


@app.get("/analytics/course_popularity")
def get_course_popularity(limit: int = 10):
    """
    Get Top N Popular Courses (default top 10)
    """
    sess = get_cassandra_session()
    response = []

    try:
        # Aggregate interactions per course from speed layer
        rows = sess.execute(f"""
            SELECT course_id, SUM(interactions) as total_interactions
            FROM course_popularity
            GROUP BY course_id
        """)

        # Convert to list and sort
        courses = []
        for row in rows:
            courses.append(
                {
                    "course_id": row.course_id,
                    "interactions": int(row.total_interactions or 0),
                }
            )

        # Sort by interactions descending and take top N
        courses.sort(key=lambda x: x["interactions"], reverse=True)
        response = courses[:limit]

    except Exception as e:
        print(f"Error getting course popularity: {e}")

    return response


@app.get("/analytics/realtime/video")
def get_realtime_video_stats():
    """
    Get latest video engagement
    """
    sess = get_cassandra_session()
    response = []

    try:
        # Get latest window data for each video
        rows = sess.execute("""
            SELECT video_id, window_end, views
            FROM video_engagement
            LIMIT 100
        """)

        for row in rows:
            response.append(
                {
                    "video_id": row.video_id,
                    "views": row.views or 0,
                    "window_end": row.window_end.isoformat(),
                }
            )

    except Exception as e:
        print(f"Error getting video stats: {e}")

    return response


# ========== COURSE ENDPOINTS ==========


@app.get("/analytics/courses")
def get_all_courses():
    """List all courses with summary stats"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute("SELECT * FROM course_overview")
        return query_to_dict_list(rows)
    except Exception as e:
        print(f"Error getting courses: {e}")
        return []


@app.get("/analytics/course/{course_id}")
def get_course_details(course_id: str):
    """Get detailed stats for a specific course"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute(
            "SELECT * FROM course_overview WHERE course_id = %s", [course_id]
        )
        row = rows.one()

        if not row:
            raise HTTPException(status_code=404, detail="Course not found")

        result = query_to_dict_list([row])
        return result[0] if result else {}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting course details: {e}")
        return {}


@app.get("/analytics/course/{course_id}/students")
def get_course_students(course_id: str):
    """Get all students enrolled in a course with their performance"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute(
            "SELECT * FROM course_student_enrollment WHERE course_id = %s", [course_id]
        )
        return query_to_dict_list(rows)
    except Exception as e:
        print(f"Error getting course students: {e}")
        return []


#  ========== STUDENT ENDPOINTS ==========


@app.get("/analytics/students")
def get_all_students():
    """List all students with summary stats"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute("SELECT * FROM student_overview LIMIT 1000")
        return query_to_dict_list(rows)
    except Exception as e:
        print(f"Error getting students: {e}")
        return []


@app.get("/analytics/student/{student_id}")
def get_student_details(student_id: str):
    """Get detailed stats for a specific student"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute(
            "SELECT * FROM student_overview WHERE student_id = %s", [student_id]
        )
        row = rows.one()

        if not row:
            raise HTTPException(status_code=404, detail="Student not found")

        result = query_to_dict_list([row])
        return result[0] if result else {}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting student details: {e}")
        return {}


@app.get("/analytics/student/{student_id}/courses")
def get_student_courses(student_id: str):
    """Get all courses a student is enrolled in with performance"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute(
            "SELECT * FROM student_course_enrollment WHERE student_id = %s",
            [student_id],
        )
        return query_to_dict_list(rows)
    except Exception as e:
        print(f"Error getting student courses: {e}")
        return []


@app.get("/analytics/student/{student_id}/course/{course_id}")
def get_student_course_performance(student_id: str, course_id: str):
    """Get detailed performance of a student in a specific course"""
    sess = get_cassandra_session()

    try:
        rows = sess.execute(
            """
            SELECT * FROM student_course_detailed
            WHERE student_id = %s AND course_id = %s
            """,
            [student_id, course_id],
        )
        row = rows.one()

        if not row:
            raise HTTPException(
                status_code=404, detail="Student-course record not found"
            )

        result = query_to_dict_list([row])
        return result[0] if result else {}

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting student course performance: {e}")
        return {}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
