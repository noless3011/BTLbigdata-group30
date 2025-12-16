from fastapi import FastAPI, HTTPException
import pandas as pd
import os
import s3fs
from datetime import datetime

app = FastAPI(title="University Learning Analytics API")

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bucket-0"

# S3 Filesystem
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': MINIO_ENDPOINT},
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    use_listings_cache=False
)

def read_parquet(path):
    """Helper to read parquet path from MinIO"""
    full_path = f"{BUCKET_NAME}/{path}"
    try:
        # Use pyarrow directly to avoid s3fs path resolution issues
        import pyarrow.parquet as pq
        
        # Check if exists (s3fs glob or exists)
        if fs.exists(full_path):
            # Read using pyarrow, then convert to pandas
            table = pq.read_table(f"{full_path}", filesystem=fs)
            return table.to_pandas()
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {full_path}: {e}")
        return pd.DataFrame()

@app.get("/")
def health_check():
    return {"status": "ok", "service": "serving_layer"}

@app.get("/analytics/dau")
def get_daily_active_users():
    """
    Get DAU by merging Batch Layer (Historical) and Speed Layer (Real-time)
    """
    # 1. Read Batch Data (Historical)
    # Batch path: batch_views/auth_daily_active_users
    # Schema: date, daily_active_users, total_sessions
    df_batch = read_parquet("batch_views/auth_daily_active_users")
    
    # 2. Read Speed Data (Real-time)
    # Speed path: speed_views/active_users
    # Schema: start, end, active_users
    df_speed = read_parquet("speed_views/active_users")
    
    # Process Speed Data to match Batch Schema
    # Aggregating real-time windows to "Today's" count is an approximation.
    # In a real Lambda architecture, we might dedup user_ids across batch & speed.
    # Here we simplify: Batch covers up to T-1. Speed covers T.
    
    api_response = []
    
    # Format Batch Data
    if not df_batch.empty:
        for _, row in df_batch.iterrows():
            api_response.append({
                "date": row['date'],
                "users": int(row['daily_active_users']),
                "source": "batch"
            })
            
    # Format Speed Data
    # Speed data is windowed (1 min). We want to show the trend or the max?
    # For a DAU chart, usually we show history days + today's current value.
    if not df_speed.empty:
        # Normalize timestamp to date
        # Assuming speed data is very recent.
        # Let's sum unique users per window? No, users might overlap.
        # Max of active users in a window gives a "Peak Concurrent" proxy, 
        # but for DAU we ideally want unique set.
        # Since we stored counts, we can't perfectly merge uniques.
        # We will return the raw speed windows for the frontend to visualize as "Real-time Trend"
        # Or aggregate to "Today (est)"
        
        # Let's return the speed data series separately or appended with a timestamp
        for _, row in df_speed.iterrows():
            api_response.append({
                "date": row['start'].strftime("%Y-%m-%d %H:%M:%S"), # detailed time for speed
                "users": int(row['active_users']),
                "source": "speed"
            })
            
    # Sort
    api_response.sort(key=lambda x: x['date'])
    return api_response

@app.get("/analytics/course_popularity")
def get_course_popularity():
    """
    Get Course Popularity (Top courses)
    """
    # Batch: Course interaction stats (if available) - let's assume raw counts
    # We didn't explicitly implement a "course_popularity" batch view in the prompt plan, 
    # but README mentions "Material popularity".
    # We will rely on Speed Layer for now as it's the focus.
    
    df_speed = read_parquet("speed_views/course_popularity")
    
    response = []
    if not df_speed.empty:
        # Aggregating speed views per course
        grouped = df_speed.groupby("course_id")['interactions'].sum().reset_index()
        top_courses = grouped.sort_values("interactions", ascending=False).head(10)
        
        for _, row in top_courses.iterrows():
            response.append({
                "course_id": row['course_id'],
                "interactions": int(row['interactions'])
            })
            
    return response

@app.get("/analytics/realtime/video")
def get_realtime_video_stats():
    """
    Get latest video engagement
    """
    df_speed = read_parquet("speed_views/video_engagement")
    response = []
    if not df_speed.empty:
        # Get latest window
        latest_window = df_speed['end'].max()
        recent_df = df_speed[df_speed['end'] == latest_window]
        
        for _, row in recent_df.iterrows():
            response.append({
                "video_id": row['video_id'],
                "views": int(row['views']),
                "window_end": row['end'].isoformat()
            })
            
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
