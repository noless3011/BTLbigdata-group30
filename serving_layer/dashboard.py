import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px
import os

# Configuration
API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="University Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎓 University Learning Analytics - Real-time Dashboard")
st.markdown("Metrics from **Lambda Architecture** merging Batch (Historical) & Speed (Real-time) layers.")

# Sidebar
st.sidebar.header("Configuration")
auto_refresh = st.sidebar.checkbox("Auto Refresh (5s)", value=True)

if auto_refresh:
    time.sleep(5)
    st.rerun()

# Fetch Data Functions
def get_dau():
    try:
        response = requests.get(f"{API_URL}/analytics/dau")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching DAU: {e}")
    return []

def get_course_popularity():
    try:
        response = requests.get(f"{API_URL}/analytics/course_popularity")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching Course Popularity: {e}")
    return []

def get_video_stats():
    try:
        response = requests.get(f"{API_URL}/analytics/realtime/video")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        st.error(f"Error fetching Video Stats: {e}")
    return []

# Layout
col1, col2 = st.columns(2)

# 1. DAU / Activity Trend
with col1:
    st.subheader("👥 User Activity Trend (Batch + Speed)")
    dau_data = get_dau()
    if dau_data:
        df_dau = pd.DataFrame(dau_data)
        # Create a combined chart
        fig = px.line(df_dau, x="date", y="users", color="source", title="Active Users over Time", markers=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available yet.")

# 2. Course Popularity
with col2:
    st.subheader("📚 Top Popular Courses (Real-time)")
    course_data = get_course_popularity()
    if course_data:
        df_course = pd.DataFrame(course_data)
        fig = px.bar(df_course, x="course_id", y="interactions", title="Interactions per Course", color="interactions")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available yet.")

# 3. Video Engagement
st.subheader("🎥 Real-time Video Engagement (Last Window)")
video_data = get_video_stats()
if video_data:
    df_video = pd.DataFrame(video_data)
    if not df_video.empty:
        # Sort by views
        df_video = df_video.sort_values("views", ascending=False).head(15)
        fig = px.bar(df_video, x="video_id", y="views", title="Views per Video (Latest)", color="views")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No video activity in the last window.")
else:
    st.info("No video data available yet.")


# Raw Data Section
with st.expander("View Raw Data"):
    st.write("DAU Data:", dau_data)
    st.write("Course Data:", course_data)
