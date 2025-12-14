"""
Serving Layer - Unified Query Interface
Merges batch views (historical) with speed views (real-time) to provide complete, up-to-date results
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import os
import sys

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from view_merger import ViewMerger
    from config import serving_config
except ImportError:
    # Fallback for module import
    from serving_layer.view_merger import ViewMerger
    from serving_layer.config import serving_config

app = FastAPI(
    title="Learning Analytics API",
    description="Unified query interface for batch and real-time analytics",
    version="1.0.0"
)

# Initialize view merger
view_merger = ViewMerger()

# Response models
class EngagementMetrics(BaseModel):
    student_id: str
    total_watch_time: float
    login_count: int
    submission_count: int
    course_count: int
    last_activity: str

class VideoAnalytics(BaseModel):
    video_id: str
    total_views: int
    total_watch_time: float
    avg_watch_duration: float
    completion_rate: float
    unique_viewers: int

class CourseStatistics(BaseModel):
    course_id: str
    total_enrollments: int
    active_students: int
    material_access_count: int
    avg_engagement_score: float

class DashboardOverview(BaseModel):
    total_students: int
    total_courses: int
    daily_active_users: int
    total_events_today: int
    system_health: str

# ============= API ENDPOINTS =============

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "service": "Learning Analytics Serving Layer",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "student_engagement": "/api/v1/student/{student_id}/engagement",
            "video_analytics": "/api/v1/video/{video_id}/analytics",
            "course_statistics": "/api/v1/course/{course_id}/statistics",
            "dashboard": "/api/v1/dashboard/overview",
            "assessment": "/api/v1/assessment/*",
            "auth": "/api/v1/auth/*",
            "profile": "/api/v1/profile/*",
            "teacher": "/api/v1/teacher/{teacher_id}/dashboard"
        }
    }

@app.get("/api/v1/student/{student_id}/engagement")
async def get_student_engagement(
    student_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get comprehensive engagement metrics for a student
    Merges batch views (historical) with speed views (real-time)
    """
    try:
        result = view_merger.get_student_engagement(
            student_id=student_id,
            start_date=start_date,
            end_date=end_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching student engagement: {str(e)}")

@app.get("/api/v1/video/{video_id}/analytics")
async def get_video_analytics(
    video_id: str,
    course_id: Optional[str] = Query(None, description="Filter by course ID")
):
    """
    Get analytics for a specific video
    Includes watch time, completion rates, popularity metrics
    """
    try:
        result = view_merger.get_video_analytics(
            video_id=video_id,
            course_id=course_id
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching video analytics: {str(e)}")

@app.get("/api/v1/course/{course_id}/statistics")
async def get_course_statistics(
    course_id: str,
    include_students: bool = Query(False, description="Include student-level details")
):
    """
    Get comprehensive statistics for a course
    Includes enrollment, engagement, material access patterns
    """
    try:
        result = view_merger.get_course_statistics(
            course_id=course_id,
            include_students=include_students
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching course statistics: {str(e)}")

@app.get("/api/v1/dashboard/overview")
async def get_dashboard_overview(
    date: Optional[str] = Query(None, description="Date for metrics (YYYY-MM-DD), defaults to today")
):
    """
    Get system-wide dashboard overview
    Aggregates metrics across all students, courses, and activities
    """
    try:
        result = view_merger.get_dashboard_overview(date=date)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dashboard overview: {str(e)}")

@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint"""
    mongo_status = False
    cassandra_status = False
    
    if view_merger.mongo_handler:
        mongo_status = view_merger.mongo_handler.is_connected()
    
    if view_merger.cassandra_handler:
        cassandra_status = view_merger.cassandra_handler.is_connected()
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "databases": {
            "mongodb": {
                "connected": mongo_status,
                "available": view_merger.mongo_handler is not None
            },
            "cassandra": {
                "connected": cassandra_status,
                "available": view_merger.cassandra_handler is not None
            }
        },
        "views": {
            "batch_views_available": view_merger.check_batch_views_available(),
            "speed_views_available": view_merger.check_speed_views_available()
        }
    }

@app.get("/api/v1/views/list")
async def list_available_views():
    """List all available batch and speed views"""
    return {
        "batch_views": view_merger.list_batch_views(),
        "speed_views": view_merger.list_speed_views()
    }

# ============= ASSESSMENT APIs =============

@app.get("/api/v1/assessment/student/{student_id}/submissions")
async def get_student_submissions(
    student_id: str,
    course_id: Optional[str] = Query(None, description="Filter by course ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)")
):
    """
    Get submission history and performance for a student
    Includes: assignments submitted, quiz scores, edit/delete actions
    """
    try:
        result = view_merger.get_student_assessment_data(
            student_id=student_id,
            course_id=course_id,
            start_date=start_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching student submissions: {str(e)}")

@app.get("/api/v1/assessment/quiz/{quiz_id}/performance")
async def get_quiz_performance(
    quiz_id: str,
    course_id: Optional[str] = Query(None, description="Filter by course ID")
):
    """
    Get quiz performance analytics
    Includes: average scores, completion rates, student performance distribution
    """
    try:
        result = view_merger.get_quiz_performance(
            quiz_id=quiz_id,
            course_id=course_id
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching quiz performance: {str(e)}")

@app.get("/api/v1/assessment/teacher/{teacher_id}/workload")
async def get_teacher_grading_workload(
    teacher_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get teacher grading workload analytics
    Includes: assignments to grade, grading history, student work reviews
    """
    try:
        result = view_merger.get_teacher_grading_workload(
            teacher_id=teacher_id,
            start_date=start_date,
            end_date=end_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching teacher workload: {str(e)}")

@app.get("/api/v1/assessment/assignment/{assignment_id}/analytics")
async def get_assignment_analytics(
    assignment_id: str,
    include_timeline: bool = Query(True, description="Include view/submit/edit timeline")
):
    """
    Get comprehensive analytics for an assignment
    Includes: submission rates, view patterns, edit/delete actions, grading status
    """
    try:
        result = view_merger.get_assignment_analytics(
            assignment_id=assignment_id,
            include_timeline=include_timeline
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching assignment analytics: {str(e)}")

# ============= AUTH APIs =============

@app.get("/api/v1/auth/user/{user_id}/activity")
async def get_user_auth_activity(
    user_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get authentication activity for a user
    Includes: login/logout times, session durations, signup date
    """
    try:
        result = view_merger.get_user_auth_activity(
            user_id=user_id,
            start_date=start_date,
            end_date=end_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching auth activity: {str(e)}")

@app.get("/api/v1/auth/sessions/active")
async def get_active_sessions(
    role: Optional[str] = Query(None, description="Filter by role: student or teacher")
):
    """
    Get currently active user sessions (real-time)
    Shows who is online right now
    """
    try:
        result = view_merger.get_active_sessions(role=role)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching active sessions: {str(e)}")

@app.get("/api/v1/auth/login-patterns")
async def get_login_patterns(
    date: Optional[str] = Query(None, description="Date for patterns (YYYY-MM-DD), defaults to today"),
    hourly: bool = Query(False, description="Return hourly breakdown")
):
    """
    Get login patterns and peak usage times
    Useful for understanding system load and user behavior
    """
    try:
        result = view_merger.get_login_patterns(date=date, hourly=hourly)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching login patterns: {str(e)}")

# ============= COURSE INTERACTION APIs =============

@app.get("/api/v1/course/{course_id}/materials/downloads")
async def get_material_downloads(
    course_id: str,
    material_id: Optional[str] = Query(None, description="Filter by specific material"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)")
):
    """
    Get material download analytics for a course
    Includes: download counts, popular materials, download patterns
    """
    try:
        result = view_merger.get_material_downloads(
            course_id=course_id,
            material_id=material_id,
            start_date=start_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching material downloads: {str(e)}")

@app.get("/api/v1/course/{course_id}/interactions")
async def get_course_interactions(
    course_id: str,
    student_id: Optional[str] = Query(None, description="Filter by student ID"),
    item_type: Optional[str] = Query(None, description="Filter by item type: lesson, module, quiz, assignment, discussion")
):
    """
    Get detailed interaction tracking for a course
    Includes: item clicks, navigation patterns, engagement by section
    """
    try:
        result = view_merger.get_course_interactions(
            course_id=course_id,
            student_id=student_id,
            item_type=item_type
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching course interactions: {str(e)}")

@app.get("/api/v1/course/lifecycle")
async def get_course_lifecycle(
    course_id: Optional[str] = Query(None, description="Filter by course ID"),
    event_type: Optional[str] = Query(None, description="Filter by event: CREATED, DELETED, UPDATED, COMPLETED, VIEW")
):
    """
    Get course lifecycle events
    Includes: course creation, updates, deletions, completions, views
    """
    try:
        result = view_merger.get_course_lifecycle(
            course_id=course_id,
            event_type=event_type
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching course lifecycle: {str(e)}")

# ============= PROFILE APIs =============

@app.get("/api/v1/profile/user/{user_id}/activity")
async def get_profile_activity(
    user_id: str,
    include_updates: bool = Query(True, description="Include profile update history")
):
    """
    Get profile activity for a user
    Includes: profile creation, update frequency, field changes, avatar updates
    """
    try:
        result = view_merger.get_profile_activity(
            user_id=user_id,
            include_updates=include_updates
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching profile activity: {str(e)}")

@app.get("/api/v1/profile/updates/summary")
async def get_profile_updates_summary(
    role: Optional[str] = Query(None, description="Filter by role: student or teacher"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)")
):
    """
    Get summary of profile updates across the system
    Includes: update frequency, most updated fields, update patterns
    """
    try:
        result = view_merger.get_profile_updates_summary(
            role=role,
            start_date=start_date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching profile updates summary: {str(e)}")

# ============= TEACHER APIs =============

@app.get("/api/v1/teacher/{teacher_id}/dashboard")
async def get_teacher_dashboard(
    teacher_id: str,
    date: Optional[str] = Query(None, description="Date for metrics (YYYY-MM-DD), defaults to today")
):
    """
    Get comprehensive dashboard for a teacher
    Includes: courses taught, grading workload, student engagement, course analytics
    """
    try:
        result = view_merger.get_teacher_dashboard(
            teacher_id=teacher_id,
            date=date
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching teacher dashboard: {str(e)}")

# ============= MAIN =============

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "serving_layer.serving_layer:app",
        host=serving_config["api"]["host"],
        port=serving_config["api"]["port"],
        reload=True
    )

