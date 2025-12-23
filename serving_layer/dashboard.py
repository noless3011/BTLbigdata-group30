import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# Configuration for both APIs
API_URL_BATCH = os.environ.get("API_URL_BATCH", "http://localhost:8000")  # MinIO
API_URL_SPEED = os.environ.get("API_URL_SPEED", "http://localhost:8001")  # Cassandra
print(f"API_URL_BATCH (MinIO): {API_URL_BATCH}")
print(f"API_URL_SPEED (Cassandra): {API_URL_SPEED}")

st.set_page_config(
    page_title="University Analytics Dashboard",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state
if "page" not in st.session_state:
    st.session_state["page"] = "Dashboard"
if "selected_course" not in st.session_state:
    st.session_state["selected_course"] = None
if "selected_student" not in st.session_state:
    st.session_state["selected_student"] = None

# Sidebar Navigation
st.sidebar.title("🎓 Navigation")
st.sidebar.markdown("---")

# Data Architecture Info
with st.sidebar.expander("ℹ️ Data Architecture", expanded=False):
    st.markdown("""
    **Lambda Architecture:**
    - **Batch Layer (MinIO)**: Historical data
    - **Speed Layer (Cassandra)**: Real-time data

    The dashboard combines both layers for complete analytics.
    """)

page = st.sidebar.radio(
    "Select View",
    ["Dashboard", "Courses", "Students"],
    index=["Dashboard", "Courses", "Students"].index(st.session_state["page"]),
)
st.session_state["page"] = page

auto_refresh = st.sidebar.checkbox("Auto Refresh (5s)", value=False)

st.sidebar.markdown("---")
st.sidebar.caption("🔵 Batch Layer: MinIO")
st.sidebar.caption("🟢 Speed Layer: Cassandra")

# ========== HELPER FUNCTIONS ==========


def get_batch_summary():
    """Get summary from batch layer (MinIO)"""
    try:
        url = f"{API_URL_BATCH}/analytics/batch/summary"
        print(f"[BATCH] Fetching summary from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Summary status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] Summary response: {data}")
            return data
        else:
            print(f"[BATCH] Summary failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Summary error: {e}")
        st.error(f"Error fetching batch summary: {e}")
    return {}


def get_speed_summary():
    """Get summary from speed layer (Cassandra)"""
    try:
        url = f"{API_URL_SPEED}/analytics/speed/summary"
        print(f"[SPEED] Fetching summary from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[SPEED] Summary status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[SPEED] Summary response: {data}")
            return data
        else:
            print(f"[SPEED] Summary failed with status {response.status_code}")
    except Exception as e:
        print(f"[SPEED] Summary error: {e}")
        st.error(f"Error fetching speed summary: {e}")
    return {}


def get_speed_recent_activity(hours=1):
    """Get recent activity from speed layer"""
    try:
        url = f"{API_URL_SPEED}/analytics/speed/recent_activity?hours={hours}"
        print(f"[SPEED] Fetching recent activity from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[SPEED] Recent activity status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[SPEED] Recent activity response: {data}")
            return data
        else:
            print(f"[SPEED] Recent activity failed with status {response.status_code}")
    except Exception as e:
        print(f"[SPEED] Recent activity error: {e}")
        st.error(f"Error fetching speed activity: {e}")
    return {}


def get_batch_recent_activity():
    """Get cumulative activity from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/batch/recent_activity"
        print(f"[BATCH] Fetching recent activity from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Recent activity status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] Recent activity response: {data}")
            return data
        else:
            print(f"[BATCH] Recent activity failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Recent activity error: {e}")
        st.error(f"Error fetching batch activity: {e}")
    return {}


def get_batch_student_engagement():
    """Get student engagement distribution from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/batch/student_engagement_distribution"
        print(f"[BATCH] Fetching student engagement from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Student engagement status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] Student engagement response: {data}")
            return data
        else:
            print(
                f"[BATCH] Student engagement failed with status {response.status_code}"
            )
    except Exception as e:
        print(f"[BATCH] Student engagement error: {e}")
        st.error(f"Error fetching engagement: {e}")
    return {}


def get_batch_dau(hours=6):
    """Get historical DAU from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/batch/dau?hours={hours}"
        print(f"[BATCH] Fetching DAU from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] DAU status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] DAU response: {data}")
            return data
        else:
            print(f"[BATCH] DAU failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] DAU error: {e}")
        st.error(f"Error fetching batch DAU: {e}")
    return []


def get_speed_active_users(hours=6):
    """Get real-time active users from speed layer"""
    try:
        url = f"{API_URL_SPEED}/analytics/speed/active_users?hours={hours}"
        print(f"[SPEED] Fetching active users from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[SPEED] Active users status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[SPEED] Active users response: {data}")
            return data
        else:
            print(f"[SPEED] Active users failed with status {response.status_code}")
    except Exception as e:
        print(f"[SPEED] Active users error: {e}")
        st.error(f"Error fetching speed active users: {e}")
    return []


def get_speed_course_popularity(limit=10, hours=24):
    """Get top courses from speed layer"""
    try:
        url = f"{API_URL_SPEED}/analytics/speed/course_popularity?limit={limit}&hours={hours}"
        print(f"[SPEED] Fetching course popularity from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[SPEED] Course popularity status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[SPEED] Course popularity response: {data}")
            print(
                f"[SPEED] Course popularity data length: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(
                f"[SPEED] Course popularity failed with status {response.status_code}"
            )
            print(f"[SPEED] Response text: {response.text}")
    except Exception as e:
        print(f"[SPEED] Course popularity error: {e}")
        st.error(f"Error fetching course popularity: {e}")
    return []


def get_speed_video_stats(limit=20, hours=1):
    """Get video stats from speed layer"""
    try:
        url = f"{API_URL_SPEED}/analytics/speed/video?limit={limit}&hours={hours}"
        print(f"[SPEED] Fetching video stats from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[SPEED] Video stats status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[SPEED] Video stats response: {data}")
            print(
                f"[SPEED] Video stats data length: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(f"[SPEED] Video stats failed with status {response.status_code}")
            print(f"[SPEED] Response text: {response.text}")
    except Exception as e:
        print(f"[SPEED] Video stats error: {e}")
        st.error(f"Error fetching video stats: {e}")
    return []


def get_all_courses():
    """Get all courses from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/courses"
        print(f"[BATCH] Fetching all courses from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] All courses status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(
                f"[BATCH] All courses count: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(f"[BATCH] All courses failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] All courses error: {e}")
        st.error(f"Error fetching courses: {e}")
    return []


def get_course_details(course_id):
    """Get course details from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/course/{course_id}"
        print(f"[BATCH] Fetching course details from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Course details status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] Course details response: {data}")
            return data
        else:
            print(f"[BATCH] Course details failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Course details error: {e}")
        st.error(f"Error fetching course details: {e}")
    return {}


def get_course_students(course_id):
    """Get course students from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/course/{course_id}/students"
        print(f"[BATCH] Fetching course students from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Course students status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(
                f"[BATCH] Course students count: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(f"[BATCH] Course students failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Course students error: {e}")
        st.error(f"Error fetching course students: {e}")
    return []


def get_all_students():
    """Get all students from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/students"
        print(f"[BATCH] Fetching all students from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] All students status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(
                f"[BATCH] All students count: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(f"[BATCH] All students failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] All students error: {e}")
        st.error(f"Error fetching students: {e}")
    return []


def get_student_details(student_id):
    """Get student details from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/student/{student_id}"
        print(f"[BATCH] Fetching student details from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Student details status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"[BATCH] Student details response: {data}")
            return data
        else:
            print(f"[BATCH] Student details failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Student details error: {e}")
        st.error(f"Error fetching student details: {e}")
    return {}


def get_student_courses(student_id):
    """Get student courses from batch layer"""
    try:
        url = f"{API_URL_BATCH}/analytics/student/{student_id}/courses"
        print(f"[BATCH] Fetching student courses from: {url}")
        response = requests.get(url, timeout=5)
        print(f"[BATCH] Student courses status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(
                f"[BATCH] Student courses count: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data
        else:
            print(f"[BATCH] Student courses failed with status {response.status_code}")
    except Exception as e:
        print(f"[BATCH] Student courses error: {e}")
        st.error(f"Error fetching student courses: {e}")
    return []


# ========== PAGE: OVERVIEW DASHBOARD ==========


def show_overview_dashboard():
    st.title("🎓 University Learning Analytics Dashboard")
    st.markdown(
        "**Lambda Architecture:** Combining Batch Layer (MinIO) + Speed Layer (Cassandra)"
    )

    # Fetch data from both layers
    batch_summary = get_batch_summary()
    speed_summary = get_speed_summary()

    # ===== ROW 1: KEY METRICS =====
    st.subheader("📈 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        current_users = speed_summary.get("current_active_users", 0)
        st.metric("👥 Current Active Users", current_users)
        st.caption("🟢 Live (Speed Layer)")

    with col2:
        active_courses = speed_summary.get("total_active_courses", 0)
        st.metric("📚 Active Courses (1h)", active_courses)
        st.caption("🟢 Real-time")

    with col3:
        total_students = batch_summary.get("total_students", 0)
        st.metric("🎓 Total Students", total_students)
        st.caption("🔵 Batch Layer")

    with col4:
        speed_status = speed_summary.get("speed_layer_status", "unknown")
        status_emoji = (
            "🟢"
            if speed_status == "healthy"
            else "🟡"
            if speed_status == "stale"
            else "🔴"
        )
        st.metric("🔌 Speed Layer Status", f"{status_emoji} {speed_status.title()}")
        last_update = speed_summary.get("last_update")
        if last_update:
            st.caption(f"Last update: {last_update[:19]}")

    st.markdown("---")

    # ===== ROW 2: ACTIVITY OVERVIEW =====
    st.subheader("📉 Activity Overview")
    col1, col2 = st.columns(2)

    # Real-time User Activity (Speed Layer)
    with col1:
        st.markdown("**👥 Real-time User Activity (Last 6 Hours)**")
        st.caption("🟢 Speed Layer Data")

        speed_users = get_speed_active_users(hours=6)
        if speed_users:
            df_speed = pd.DataFrame(speed_users)
            df_speed = df_speed.sort_values("date").reset_index(drop=True)
            df_speed["date"] = pd.to_datetime(df_speed["date"])

            fig = px.line(
                df_speed,
                x="date",
                y="users",
                title="Active Users (Real-time Windows)",
                markers=True,
            )
            fig.update_traces(line_color="#10b981", marker_color="#10b981")
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)

            # Show stats
            peak_users = df_speed["users"].max()
            avg_users = df_speed["users"].mean()
            st.caption(f"🔺 Peak: {peak_users} users | 📊 Avg: {avg_users:.1f} users")
        else:
            st.info("No real-time user data available")

    # Top Engaged Courses (Speed Layer)
    with col2:
        st.markdown("**📚 Top 10 Engaged Courses (Last 24h)**")
        st.caption("🟢 Speed Layer Data")

        course_data = get_speed_course_popularity(limit=10, hours=24)
        if course_data:
            df_course = pd.DataFrame(course_data).reset_index(drop=True)
            fig = px.bar(
                df_course,
                x="course_id",
                y="interactions",
                title="Course Interactions",
                color="interactions",
                color_continuous_scale="Greens",
            )
            fig.update_layout(showlegend=False, height=300)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            total_interactions = df_course["interactions"].sum()
            st.caption(f"📈 Total interactions: {total_interactions}")
        else:
            st.info("No course popularity data available")

    st.markdown("---")

    # ===== ROW 3: ENGAGEMENT INSIGHTS =====
    st.subheader("🎯 Engagement Insights")
    col1, col2 = st.columns(2)

    # Recent Activity Comparison (Batch vs Speed)
    with col1:
        st.markdown("**🎥 Recent Activity Comparison**")

        speed_activity = get_speed_recent_activity(hours=1)
        batch_activity = get_batch_recent_activity()

        if speed_activity or batch_activity:
            activity_data = []

            # Speed layer data (last hour)
            if speed_activity:
                activity_data.append(
                    {
                        "Metric": "Videos (1h)",
                        "Count": speed_activity.get("videos_watched", 0),
                        "Layer": "Speed",
                    }
                )
                activity_data.append(
                    {
                        "Metric": "Interactions (1h)",
                        "Count": speed_activity.get("course_interactions", 0),
                        "Layer": "Speed",
                    }
                )

            # Batch layer data (cumulative)
            if batch_activity:
                activity_data.append(
                    {
                        "Metric": "Total Videos",
                        "Count": batch_activity.get("total_videos_watched", 0),
                        "Layer": "Batch",
                    }
                )
                activity_data.append(
                    {
                        "Metric": "Total Materials",
                        "Count": batch_activity.get("total_materials_downloaded", 0),
                        "Layer": "Batch",
                    }
                )

            df_activity = pd.DataFrame(activity_data)
            fig = px.bar(
                df_activity,
                x="Metric",
                y="Count",
                color="Layer",
                title="Activity Metrics",
                barmode="group",
                color_discrete_map={"Speed": "#10b981", "Batch": "#3b82f6"},
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            st.caption("🟢 Speed: Real-time (last 1h) | 🔵 Batch: Cumulative")
        else:
            st.info("No activity data available")

    # Student Engagement Distribution (Batch Layer)
    with col2:
        st.markdown("**👥 Student Engagement Distribution**")
        st.caption("🔵 Batch Layer Data")

        distribution = get_batch_student_engagement()

        if distribution and sum(distribution.values()) > 0:
            dist_data = pd.DataFrame(
                [
                    {
                        "Category": "Highly Active",
                        "Students": distribution.get("highly_active", 0),
                    },
                    {
                        "Category": "Moderately Active",
                        "Students": distribution.get("moderately_active", 0),
                    },
                    {
                        "Category": "Low Activity",
                        "Students": distribution.get("low_activity", 0),
                    },
                    {
                        "Category": "Inactive",
                        "Students": distribution.get("inactive", 0),
                    },
                ]
            )

            fig = px.pie(
                dist_data,
                values="Students",
                names="Category",
                title="Student Activity Levels",
                color_discrete_sequence=["#10b981", "#3b82f6", "#f59e0b", "#ef4444"],
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            total_students = sum(distribution.values())
            active_pct = (
                (
                    distribution.get("highly_active", 0)
                    + distribution.get("moderately_active", 0)
                )
                / total_students
                * 100
                if total_students > 0
                else 0
            )
            st.caption(f"👥 Total: {total_students} | ✅ Active: {active_pct:.1f}%")
        else:
            st.info("No student engagement data available")

    st.markdown("---")

    # ===== ROW 4: VIDEO ENGAGEMENT =====
    st.subheader("🎬 Real-time Video Engagement")
    st.caption("🟢 Speed Layer Data (Last Hour)")

    video_stats = get_speed_video_stats(limit=10, hours=1)
    if video_stats:
        df_video = pd.DataFrame(video_stats).head(10)

        col1, col2 = st.columns([2, 1])

        with col1:
            fig = px.bar(
                df_video,
                x="video_id",
                y="views",
                title="Top 10 Videos by Views",
                color="views",
                color_continuous_scale="Greens",
            )
            fig.update_layout(showlegend=False, height=300)
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Top Videos:**")
            for idx, row in df_video.iterrows():
                st.markdown(f"**{idx + 1}.** `{row['video_id']}`")
                st.caption(f"   👁️ {row['views']} views")
                if idx >= 4:  # Show top 5
                    break
    else:
        st.info("No video engagement data available")


# ========== PAGE: COURSES VIEW ==========


def show_courses_view():
    st.title("📚 Course Analytics")
    st.caption("🔵 Data from Batch Layer (MinIO)")

    # Fetch courses from batch layer
    courses = get_all_courses()

    if not courses:
        st.warning("No course data available")
        return

    # Display courses table
    st.subheader(f"Total Courses: {len(courses)}")

    df_courses = pd.DataFrame(courses)

    # Display key columns if available
    display_cols = ["course_id"]
    if "total_enrolled_students" in df_courses.columns:
        display_cols.append("total_enrolled_students")
    if "total_videos_watched" in df_courses.columns:
        display_cols.append("total_videos_watched")
    if "total_assignments_submitted" in df_courses.columns:
        display_cols.append("total_assignments_submitted")

    available_cols = [col for col in display_cols if col in df_courses.columns]
    st.dataframe(df_courses[available_cols], use_container_width=True)

    st.markdown("---")

    # Course Details Section
    st.subheader("🔍 Course Details")

    # Course selector
    course_id = st.selectbox(
        "Select a Course:",
        options=df_courses["course_id"].tolist(),
        key="course_selector",
    )

    if course_id:
        st.session_state["selected_course"] = course_id

        # Get course details
        course_details = get_course_details(course_id)

        if course_details:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    "👥 Enrolled Students",
                    course_details.get("total_enrolled_students", "N/A"),
                )

            with col2:
                st.metric(
                    "🎥 Videos Watched",
                    course_details.get("total_videos_watched", "N/A"),
                )

            with col3:
                st.metric(
                    "📝 Assignments Submitted",
                    course_details.get("total_assignments_submitted", "N/A"),
                )

            st.markdown("---")

            # Get enrolled students
            students = get_course_students(course_id)

            if students:
                st.subheader(f"👥 Enrolled Students ({len(students)})")
                df_students = pd.DataFrame(students)

                # Display student performance
                display_cols = ["student_id"]
                if "videos_watched" in df_students.columns:
                    display_cols.append("videos_watched")
                if "assignments_submitted" in df_students.columns:
                    display_cols.append("assignments_submitted")
                if "avg_score" in df_students.columns:
                    display_cols.append("avg_score")

                available_cols = [
                    col for col in display_cols if col in df_students.columns
                ]
                st.dataframe(df_students[available_cols], use_container_width=True)
            else:
                st.info("No student enrollment data available")


# ========== PAGE: STUDENTS VIEW ==========


def show_students_view():
    st.title("👥 Student Analytics")
    st.caption("🔵 Data from Batch Layer (MinIO)")

    # Fetch students from batch layer
    students = get_all_students()

    if not students:
        st.warning("No student data available")
        return

    # Display students table
    st.subheader(f"Total Students: {len(students)}")

    df_students = pd.DataFrame(students)

    # Display key columns if available
    display_cols = ["student_id"]
    if "total_courses_enrolled" in df_students.columns:
        display_cols.append("total_courses_enrolled")
    if "total_videos_watched" in df_students.columns:
        display_cols.append("total_videos_watched")
    if "total_assignments_submitted" in df_students.columns:
        display_cols.append("total_assignments_submitted")
    if "total_logins" in df_students.columns:
        display_cols.append("total_logins")

    available_cols = [col for col in display_cols if col in df_students.columns]
    st.dataframe(df_students[available_cols], use_container_width=True)

    st.markdown("---")

    # Student Details Section
    st.subheader("🔍 Student Details")

    # Student selector
    student_id = st.selectbox(
        "Select a Student:",
        options=df_students["student_id"].tolist(),
        key="student_selector",
    )

    if student_id:
        st.session_state["selected_student"] = student_id

        # Get student details
        student_details = get_student_details(student_id)

        if student_details:
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "📚 Courses Enrolled",
                    student_details.get("total_courses_enrolled", "N/A"),
                )

            with col2:
                st.metric(
                    "🎥 Videos Watched",
                    student_details.get("total_videos_watched", "N/A"),
                )

            with col3:
                st.metric(
                    "📝 Assignments Submitted",
                    student_details.get("total_assignments_submitted", "N/A"),
                )

            with col4:
                st.metric("🔑 Total Logins", student_details.get("total_logins", "N/A"))

            st.markdown("---")

            # Get student's courses
            courses = get_student_courses(student_id)

            if courses:
                st.subheader(f"📚 Enrolled Courses ({len(courses)})")
                df_courses = pd.DataFrame(courses)

                # Display course performance
                display_cols = ["course_id"]
                if "videos_watched" in df_courses.columns:
                    display_cols.append("videos_watched")
                if "assignments_submitted" in df_courses.columns:
                    display_cols.append("assignments_submitted")
                if "avg_score" in df_courses.columns:
                    display_cols.append("avg_score")
                if "last_accessed" in df_courses.columns:
                    display_cols.append("last_accessed")

                available_cols = [
                    col for col in display_cols if col in df_courses.columns
                ]
                st.dataframe(df_courses[available_cols], use_container_width=True)

                # Visualize student performance across courses
                if "avg_score" in df_courses.columns:
                    fig = px.bar(
                        df_courses,
                        x="course_id",
                        y="avg_score",
                        title="Performance Across Courses",
                        color="avg_score",
                        color_continuous_scale="Blues",
                    )
                    fig.update_layout(height=300)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No course enrollment data available")


# ========== MAIN ROUTER ==========

if page == "Dashboard":
    show_overview_dashboard()
elif page == "Courses":
    show_courses_view()
elif page == "Students":
    show_students_view()

# Auto-refresh logic
if auto_refresh:
    time.sleep(5)
    st.rerun()
