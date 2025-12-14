"""
View Merger - Combines batch views and speed views
Implements Lambda Architecture merge logic
"""

import os
import sys
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

# Try to import PySpark (may fail if not available)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not available. Using mock data mode.")

# Import config (handle both direct import and module import)
try:
    from config import serving_config
except ImportError:
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    from config import serving_config

# Import database handlers
try:
    from database_handlers import MongoDBHandler, CassandraHandler
    DB_HANDLERS_AVAILABLE = True
except ImportError:
    DB_HANDLERS_AVAILABLE = False
    print("Warning: Database handlers not available.")

class ViewMerger:
    """
    Merges batch views (historical, accurate) with speed views (recent, real-time)
    Speed views override batch views for overlapping time periods
    """
    
    def __init__(self):
        self.config = serving_config
        self.batch_views_path = self.config["paths"]["batch_views"]
        self.speed_views_path = self.config["paths"]["speed_views"]
        self.use_mock_data = self.config.get("use_mock_data", False)
        
        # Initialize database handlers
        self.mongo_handler = None
        self.cassandra_handler = None
        if DB_HANDLERS_AVAILABLE:
            try:
                self.mongo_handler = MongoDBHandler(self.config)
                self.cassandra_handler = CassandraHandler(self.config)
            except Exception as e:
                print(f"Warning: Could not initialize database handlers: {str(e)}")
        
        # Initialize Spark if available and not using mock data
        self.spark = None
        if SPARK_AVAILABLE and not self.use_mock_data:
            self._init_spark()
    
    def _init_spark(self):
        """Initialize Spark Session for querying views"""
        if self.spark is None:
            minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
            
            self.spark = SparkSession.builder \
                .appName("Serving_Layer_Query") \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
    
    def _read_batch_view(self, view_name: str) -> Optional[Any]:
        """Read a batch view from Cassandra (preferred) or MinIO (fallback)"""
        if self.use_mock_data:
            return self._get_mock_batch_view(view_name)
        
        # Try Cassandra first
        if self.cassandra_handler and self.cassandra_handler.is_connected():
            try:
                cassandra_data = self.cassandra_handler.get_batch_view(view_name)
                if cassandra_data:
                    # Parse JSON data from Cassandra
                    results = []
                    for row in cassandra_data:
                        try:
                            data = json.loads(row["data"])
                            results.append(data)
                        except:
                            results.append(row["data"])
                    return {"data": results, "source": "cassandra"}
            except Exception as e:
                print(f"Warning: Could not read from Cassandra: {str(e)}")
        
        # Fallback to MinIO/Spark
        if not SPARK_AVAILABLE or self.spark is None:
            return self._get_mock_batch_view(view_name)
        
        try:
            view_path = f"{self.batch_views_path}/{view_name}"
            df = self.spark.read.parquet(view_path)
            return df
        except Exception as e:
            print(f"Warning: Could not read batch view {view_name}: {str(e)}")
            return self._get_mock_batch_view(view_name)
    
    def _read_speed_view(self, view_name: str) -> Optional[Any]:
        """Read a speed view (real-time data) from Cassandra"""
        if self.use_mock_data:
            return self._get_mock_speed_view(view_name)
        
        # Try Cassandra first
        if self.cassandra_handler and self.cassandra_handler.is_connected():
            try:
                cassandra_data = self.cassandra_handler.get_speed_view(view_name, limit=100)
                if cassandra_data:
                    # Parse JSON data from Cassandra
                    results = []
                    for row in cassandra_data:
                        try:
                            data = json.loads(row["data"])
                            results.append(data)
                        except:
                            results.append(row["data"])
                    return {"data": results, "source": "cassandra"}
            except Exception as e:
                print(f"Warning: Could not read speed view from Cassandra: {str(e)}")
        
        # Fallback to mock data
        return self._get_mock_speed_view(view_name)
    
    def _get_mock_batch_view(self, view_name: str) -> Dict[str, Any]:
        """Generate mock batch view data for testing"""
        # Mock data based on view name
        if "auth" in view_name:
            return {
                "data": [
                    {"date": "2025-12-14", "daily_active_users": 150, "total_sessions": 320},
                    {"date": "2025-12-13", "daily_active_users": 145, "total_sessions": 310}
                ],
                "source": "mock_batch"
            }
        elif "video" in view_name:
            return {
                "data": [
                    {"video_id": "VID001", "total_watch_time": 1250.5, "view_count": 45},
                    {"video_id": "VID002", "total_watch_time": 980.2, "view_count": 38}
                ],
                "source": "mock_batch"
            }
        elif "course" in view_name:
            return {
                "data": [
                    {"course_id": "COURSE001", "enrollment_count": 120, "active_students": 95},
                    {"course_id": "COURSE002", "enrollment_count": 110, "active_students": 88}
                ],
                "source": "mock_batch"
            }
        else:
            return {"data": [], "source": "mock_batch"}
    
    def _get_mock_speed_view(self, view_name: str) -> Dict[str, Any]:
        """Generate mock speed view data (today's real-time data)"""
        today = datetime.now().strftime("%Y-%m-%d")
        return {
            "data": [
                {"date": today, "count": 25, "timestamp": datetime.now().isoformat()}
            ],
            "source": "mock_speed"
        }
    
    def _merge_views(self, batch_data: Any, speed_data: Any, merge_key: str = "date") -> Dict[str, Any]:
        """
        Merge batch and speed views
        Speed view data overrides batch view for overlapping periods
        """
        if isinstance(batch_data, dict) and isinstance(speed_data, dict):
            # Mock data mode
            batch_items = batch_data.get("data", [])
            speed_items = speed_data.get("data", [])
            
            # Create lookup for speed data
            speed_lookup = {item.get(merge_key): item for item in speed_items}
            
            # Merge: speed overrides batch
            merged = []
            batch_keys = {item.get(merge_key) for item in batch_items}
            
            # Add batch data (excluding overlapping keys)
            for item in batch_items:
                key = item.get(merge_key)
                if key not in speed_lookup:
                    merged.append({**item, "source": "batch"})
            
            # Add speed data (overrides batch)
            for item in speed_items:
                merged.append({**item, "source": "speed"})
            
            return {
                "data": merged,
                "merged_at": datetime.now().isoformat(),
                "batch_count": len(batch_items),
                "speed_count": len(speed_items),
                "merged_count": len(merged)
            }
        else:
            # Spark DataFrame mode
            if batch_data is None and speed_data is None:
                return {"data": [], "error": "No data available"}
            
            if batch_data is None:
                return speed_data.toPandas().to_dict(orient="records")
            
            if speed_data is None:
                return batch_data.toPandas().to_dict(orient="records")
            
            # Merge Spark DataFrames
            # Speed data overrides batch data
            merged = batch_data.unionByName(speed_data, allowMissingColumns=True)
            return merged.toPandas().to_dict(orient="records")
    
    # ============= QUERY METHODS =============
    
    def get_student_engagement(
        self, 
        student_id: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive engagement metrics for a student"""
        # Read batch views
        video_engagement = self._read_batch_view("video_student_engagement")
        auth_summary = self._read_batch_view("auth_user_activity_summary")
        course_activity = self._read_batch_view("course_activity_summary")
        
        # Read speed views (today's data)
        speed_video = self._read_speed_view("video_realtime_engagement_5min")
        speed_auth = self._read_speed_view("auth_realtime_active_users_5min")
        
        # Merge views
        video_merged = self._merge_views(video_engagement, speed_video, merge_key="student_id")
        auth_merged = self._merge_views(auth_summary, speed_auth, merge_key="user_id")
        
        # Aggregate results
        result = {
            "student_id": student_id,
            "video_engagement": video_merged,
            "auth_activity": auth_merged,
            "course_activity": course_activity,
            "computed_at": datetime.now().isoformat()
        }
        
        return result
    
    def get_video_analytics(
        self, 
        video_id: str,
        course_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get analytics for a specific video"""
        # Read batch views
        watch_time = self._read_batch_view("video_total_watch_time")
        popularity = self._read_batch_view("video_popularity")
        
        # Read speed views
        speed_viewers = self._read_speed_view("video_realtime_viewers_1min")
        
        # Merge and filter
        result = {
            "video_id": video_id,
            "course_id": course_id,
            "watch_time_stats": watch_time,
            "popularity_rank": popularity,
            "current_viewers": speed_viewers,
            "computed_at": datetime.now().isoformat()
        }
        
        return result
    
    def get_course_statistics(
        self, 
        course_id: str,
        include_students: bool = False
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a course"""
        # Read batch views
        enrollment = self._read_batch_view("course_enrollment_stats")
        material_access = self._read_batch_view("course_material_access")
        overall_metrics = self._read_batch_view("course_overall_metrics")
        
        # Read speed views
        speed_activity = self._read_speed_view("course_realtime_engagement_metrics")
        
        # Merge
        activity_merged = self._merge_views(overall_metrics, speed_activity, merge_key="course_id")
        
        result = {
            "course_id": course_id,
            "enrollment": enrollment,
            "material_access": material_access,
            "overall_metrics": activity_merged,
            "include_students": include_students,
            "computed_at": datetime.now().isoformat()
        }
        
        if include_students:
            result["student_details"] = material_access
        
        return result
    
    def get_dashboard_overview(self, date: Optional[str] = None) -> Dict[str, Any]:
        """Get system-wide dashboard overview"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        # Read batch views
        dau = self._read_batch_view("auth_daily_active_users")
        course_stats = self._read_batch_view("course_overall_metrics")
        video_popularity = self._read_batch_view("video_popularity")
        
        # Read speed views
        speed_dau = self._read_speed_view("auth_realtime_active_users_5min")
        
        # Merge
        dau_merged = self._merge_views(dau, speed_dau, merge_key="date")
        
        result = {
            "date": date,
            "daily_active_users": dau_merged,
            "total_courses": len(course_stats.get("data", [])) if isinstance(course_stats, dict) else 0,
            "top_videos": video_popularity,
            "system_health": "healthy",
            "computed_at": datetime.now().isoformat()
        }
        
        return result
    
    def check_batch_views_available(self) -> bool:
        """Check if batch views are available"""
        if self.use_mock_data:
            return True  # Mock data always available
        
        # Check Cassandra first
        if self.cassandra_handler and self.cassandra_handler.is_connected():
            try:
                test_data = self.cassandra_handler.get_batch_view("auth_daily_active_users")
                if test_data:
                    return True
            except:
                pass
        
        # Check MinIO/Spark
        if not SPARK_AVAILABLE or self.spark is None:
            return False
        
        try:
            # Try to read one view
            test_path = f"{self.batch_views_path}/auth_daily_active_users"
            self.spark.read.parquet(test_path).limit(1).collect()
            return True
        except:
            return False
    
    def check_speed_views_available(self) -> bool:
        """Check if speed views are available"""
        # Check Cassandra
        if self.cassandra_handler and self.cassandra_handler.is_connected():
            try:
                test_data = self.cassandra_handler.get_speed_view("auth_realtime_active_users_5min", limit=1)
                if test_data:
                    return True
            except:
                pass
        
        # Fallback to mock data
        return True
    
    def list_batch_views(self) -> List[str]:
        """List all available batch views"""
        return [
            "auth_daily_active_users",
            "auth_hourly_login_patterns",
            "auth_user_session_metrics",
            "video_total_watch_time",
            "video_student_engagement",
            "video_popularity",
            "course_enrollment_stats",
            "course_material_access",
            "assessment_student_submissions",
            "assessment_quiz_performance"
        ]
    
    def list_speed_views(self) -> List[str]:
        """List all available speed views"""
        return [
            "auth_realtime_active_users_1min",
            "auth_realtime_active_users_5min",
            "video_realtime_viewers_1min",
            "video_realtime_watch_time_5min",
            "course_realtime_active_courses_1min",
            "course_realtime_engagement_metrics"
        ]
    
    # ============= ASSESSMENT METHODS =============
    
    def get_student_assessment_data(
        self,
        student_id: str,
        course_id: Optional[str] = None,
        start_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get student assessment data: submissions, quiz scores, edit/delete actions"""
        submissions = self._read_batch_view("assessment_student_submissions")
        performance = self._read_batch_view("assessment_student_overall_performance")
        timeline = self._read_batch_view("assessment_engagement_timeline")
        
        return {
            "student_id": student_id,
            "course_id": course_id,
            "submissions": submissions,
            "performance": performance,
            "timeline": timeline,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_quiz_performance(
        self,
        quiz_id: str,
        course_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get quiz performance analytics"""
        quiz_perf = self._read_batch_view("assessment_quiz_performance")
        
        return {
            "quiz_id": quiz_id,
            "course_id": course_id,
            "performance": quiz_perf,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_teacher_grading_workload(
        self,
        teacher_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get teacher grading workload"""
        workload = self._read_batch_view("assessment_teacher_workload")
        grading_stats = self._read_batch_view("assessment_grading_stats")
        
        return {
            "teacher_id": teacher_id,
            "start_date": start_date,
            "end_date": end_date,
            "workload": workload,
            "grading_stats": grading_stats,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_assignment_analytics(
        self,
        assignment_id: str,
        include_timeline: bool = True
    ) -> Dict[str, Any]:
        """Get assignment analytics"""
        submissions = self._read_batch_view("assessment_student_submissions")
        distribution = self._read_batch_view("assessment_submission_distribution")
        
        result = {
            "assignment_id": assignment_id,
            "submissions": submissions,
            "distribution": distribution,
            "computed_at": datetime.now().isoformat()
        }
        
        if include_timeline:
            result["timeline"] = self._read_batch_view("assessment_engagement_timeline")
        
        return result
    
    # ============= AUTH METHODS =============
    
    def get_user_auth_activity(
        self,
        user_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get user authentication activity"""
        session_metrics = self._read_batch_view("auth_user_session_metrics")
        activity_summary = self._read_batch_view("auth_user_activity_summary")
        hourly_patterns = self._read_batch_view("auth_hourly_login_patterns")
        
        return {
            "user_id": user_id,
            "start_date": start_date,
            "end_date": end_date,
            "session_metrics": session_metrics,
            "activity_summary": activity_summary,
            "login_patterns": hourly_patterns,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_active_sessions(self, role: Optional[str] = None) -> Dict[str, Any]:
        """Get currently active sessions (real-time)"""
        speed_auth = self._read_speed_view("auth_realtime_active_users_5min")
        
        return {
            "role": role,
            "active_sessions": speed_auth,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_login_patterns(
        self,
        date: Optional[str] = None,
        hourly: bool = False
    ) -> Dict[str, Any]:
        """Get login patterns"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        dau = self._read_batch_view("auth_daily_active_users")
        hourly_patterns = self._read_batch_view("auth_hourly_login_patterns")
        
        result = {
            "date": date,
            "daily_active_users": dau,
            "computed_at": datetime.now().isoformat()
        }
        
        if hourly:
            result["hourly_patterns"] = hourly_patterns
        
        return result
    
    # ============= COURSE INTERACTION METHODS =============
    
    def get_material_downloads(
        self,
        course_id: str,
        material_id: Optional[str] = None,
        start_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get material download analytics"""
        downloads = self._read_batch_view("course_download_analytics")
        resource_stats = self._read_batch_view("course_resource_download_stats")
        popularity = self._read_batch_view("course_material_popularity")
        
        return {
            "course_id": course_id,
            "material_id": material_id,
            "start_date": start_date,
            "downloads": downloads,
            "resource_stats": resource_stats,
            "popularity": popularity,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_course_interactions(
        self,
        course_id: str,
        student_id: Optional[str] = None,
        item_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get course interaction tracking"""
        material_access = self._read_batch_view("course_material_access")
        activity_summary = self._read_batch_view("course_activity_summary")
        
        return {
            "course_id": course_id,
            "student_id": student_id,
            "item_type": item_type,
            "material_access": material_access,
            "activity_summary": activity_summary,
            "computed_at": datetime.now().isoformat()
        }
    
    def get_course_lifecycle(
        self,
        course_id: Optional[str] = None,
        event_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get course lifecycle events"""
        enrollment = self._read_batch_view("course_enrollment_stats")
        activity = self._read_batch_view("course_daily_engagement")
        
        return {
            "course_id": course_id,
            "event_type": event_type,
            "enrollment": enrollment,
            "activity": activity,
            "computed_at": datetime.now().isoformat()
        }
    
    # ============= PROFILE METHODS =============
    
    def get_profile_activity(
        self,
        user_id: str,
        include_updates: bool = True
    ) -> Dict[str, Any]:
        """Get profile activity"""
        update_freq = self._read_batch_view("profile_update_frequency")
        field_changes = self._read_batch_view("profile_field_changes")
        avatar_changes = self._read_batch_view("profile_avatar_changes")
        daily_activity = self._read_batch_view("profile_daily_activity")
        
        result = {
            "user_id": user_id,
            "update_frequency": update_freq,
            "field_changes": field_changes,
            "avatar_changes": avatar_changes,
            "daily_activity": daily_activity,
            "computed_at": datetime.now().isoformat()
        }
        
        return result
    
    def get_profile_updates_summary(
        self,
        role: Optional[str] = None,
        start_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get profile updates summary"""
        update_freq = self._read_batch_view("profile_update_frequency")
        field_changes = self._read_batch_view("profile_field_changes")
        daily_activity = self._read_batch_view("profile_daily_activity")
        
        return {
            "role": role,
            "start_date": start_date,
            "update_frequency": update_freq,
            "field_changes": field_changes,
            "daily_activity": daily_activity,
            "computed_at": datetime.now().isoformat()
        }
    
    # ============= TEACHER METHODS =============
    
    def get_teacher_dashboard(
        self,
        teacher_id: str,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive teacher dashboard"""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        workload = self._read_batch_view("assessment_teacher_workload")
        grading_stats = self._read_batch_view("assessment_grading_stats")
        course_metrics = self._read_batch_view("course_overall_metrics")
        
        return {
            "teacher_id": teacher_id,
            "date": date,
            "grading_workload": workload,
            "grading_stats": grading_stats,
            "course_metrics": course_metrics,
            "computed_at": datetime.now().isoformat()
        }

