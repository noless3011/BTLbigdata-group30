import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytz
from cassandra.cluster import Cluster
from fastapi import FastAPI, HTTPException

vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")

app = FastAPI(title="University Learning Analytics API - Speed Layer (Cassandra)")

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
def root():
    return {
        "status": "ok",
        "service": "serving_layer_speed",
        "backend": "cassandra",
    }


@app.get("/health")
def health_check():
    """Health check endpoint with database connectivity check"""
    if session:
        return {
            "status": "ok",
            "backend": "cassandra",
            "layer": "speed",
            "db": "connected",
        }
    return {"status": "ok", "backend": "cassandra", "layer": "speed", "db": "unknown"}


# ========== SPEED LAYER SUMMARY METRICS ==========


@app.get("/analytics/speed/summary")
def get_speed_summary():
    """
    Get speed layer summary metrics
    Returns: current active users, active courses with recent activity, speed layer status
    """
    sess = get_cassandra_session()

    summary = {
        "current_active_users": 0,
        "total_active_courses": 0,
        "speed_layer_status": "unknown",
        "last_update": None,
    }

    now_utc = datetime.now(timezone.utc)

    try:
        # Get current active users from speed layer (most recent window)
        rows = sess.execute("""
            SELECT window_start, active_users
            FROM active_users
            LIMIT 100
        """)

        latest_window = None
        latest_users = 0

        for row in rows:
            if latest_window is None or row.window_start > latest_window:
                latest_window = row.window_start
                latest_users = row.active_users or 0

        if latest_window:
            summary["current_active_users"] = latest_users
            summary["last_update"] = latest_window.isoformat()

            # Check if speed layer is healthy (data within last 5 minutes)
            time_diff = (
                now_utc.replace(tzinfo=None) - latest_window.replace(tzinfo=None)
            ).total_seconds()
            if time_diff < 300:  # 5 minutes
                summary["speed_layer_status"] = "healthy"
            else:
                summary["speed_layer_status"] = "stale"

        # Get active courses count (courses with activity in last hour)
        cutoff = now_utc - timedelta(hours=1)

        # FIX: Removed 'DISTINCT' below. Python logic handles the unique set.
        rows = sess.execute(f"""
            SELECT course_id
            FROM course_popularity
            WHERE window_start >= '{cutoff.isoformat()}'
            ALLOW FILTERING
        """)

        active_courses = set()
        for row in rows:
            active_courses.add(row.course_id)

        summary["total_active_courses"] = len(active_courses)

    except Exception as e:
        print(f"Error getting speed summary: {e}")

    return summary


@app.get("/analytics/speed/recent_activity")
def get_recent_activity(hours: int = 1):
    """
    Get recent activity stats from speed layer
    Returns: videos watched, course interactions in last N hours
    """
    sess = get_cassandra_session()

    activity = {
        "videos_watched": 0,
        "course_interactions": 0,
        "time_period_hours": hours,
    }

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Videos from speed layer
        rows = sess.execute(f"""
            SELECT views
            FROM video_engagement
            WHERE window_start >= '{cutoff.isoformat()}'
            ALLOW FILTERING
        """)

        total_views = 0
        for row in rows:
            total_views += row.views or 0

        activity["videos_watched"] = total_views

        # Course interactions from speed layer
        rows = sess.execute(f"""
            SELECT interactions
            FROM course_popularity
            WHERE window_start >= '{cutoff.isoformat()}'
            ALLOW FILTERING
        """)

        total_interactions = 0
        for row in rows:
            total_interactions += row.interactions or 0

        activity["course_interactions"] = total_interactions

    except Exception as e:
        print(f"Error getting recent activity: {e}")

    return activity


@app.get("/analytics/speed/active_users")
def get_realtime_active_users(hours: int = 6):
    """
    Get real-time active users from speed layer
    Returns time series of active users
    """
    sess = get_cassandra_session()
    api_response = []

    try:
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

        rows = sess.execute(f"""
            SELECT window_start, window_end, active_users
            FROM active_users
            WHERE window_start >= '{cutoff_time.isoformat()}'
            ALLOW FILTERING
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
        print(f"Error getting active users: {e}")

    return api_response


@app.get("/analytics/speed/course_popularity")
def get_course_popularity(limit: int = 10, hours: int = 24):
    """
    Get Top N Popular Courses from speed layer (default top 10)
    Based on recent activity in the last N hours
    """
    sess = get_cassandra_session()
    response = []

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Get all course popularity data from the time window
        rows = sess.execute(f"""
            SELECT course_id, interactions
            FROM course_popularity
            WHERE window_start >= '{cutoff.isoformat()}'
            ALLOW FILTERING
        """)

        # Aggregate interactions per course
        course_totals = {}
        for row in rows:
            course_id = row.course_id
            interactions = row.interactions or 0

            if course_id in course_totals:
                course_totals[course_id] += interactions
            else:
                course_totals[course_id] = interactions

        # Convert to list and sort
        courses = [
            {"course_id": course_id, "interactions": total}
            for course_id, total in course_totals.items()
        ]

        # Sort by interactions descending and take top N
        courses.sort(key=lambda x: x["interactions"], reverse=True)
        response = courses[:limit]

    except Exception as e:
        print(f"Error getting course popularity: {e}")

    return response


@app.get("/analytics/speed/video")
def get_realtime_video_stats(limit: int = 20, hours: int = 1):
    """
    Get latest video engagement from speed layer
    """
    sess = get_cassandra_session()
    response = []

    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Get recent video engagement data
        # FIX: Swapped order of LIMIT and ALLOW FILTERING
        rows = sess.execute(f"""
            SELECT video_id, window_start, window_end, views
            FROM video_engagement
            WHERE window_start >= '{cutoff.isoformat()}'
            LIMIT {limit * 10}
            ALLOW FILTERING
        """)

        # Aggregate views per video
        video_totals = {}
        video_latest = {}

        for row in rows:
            video_id = row.video_id
            views = row.views or 0

            if video_id in video_totals:
                video_totals[video_id] += views
            else:
                video_totals[video_id] = views

            # Track latest window_end for each video
            if video_id not in video_latest or row.window_end > video_latest[video_id]:
                video_latest[video_id] = row.window_end

        # Convert to list
        videos = [
            {
                "video_id": video_id,
                "views": total,
                "window_end": video_latest[video_id].isoformat()
                if video_id in video_latest
                else None,
            }
            for video_id, total in video_totals.items()
        ]

        # Sort by views descending and take top N
        videos.sort(key=lambda x: x["views"], reverse=True)
        response = videos[:limit]

    except Exception as e:
        print(f"Error getting video stats: {e}")

    return response


@app.get("/analytics/speed/active_users/latest")
def get_latest_active_users():
    """
    Get the most recent active users count (for dashboard display)
    """
    sess = get_cassandra_session()

    try:
        rows = sess.execute("""
            SELECT window_start, active_users
            FROM active_users
            LIMIT 100
        """)

        latest_window = None
        latest_users = 0

        for row in rows:
            if latest_window is None or row.window_start > latest_window:
                latest_window = row.window_start
                latest_users = row.active_users or 0

        if latest_window:
            return {
                "active_users": latest_users,
                "timestamp": latest_window.isoformat(),
            }

    except Exception as e:
        print(f"Error getting latest active users: {e}")

    return {"active_users": 0, "timestamp": None}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
