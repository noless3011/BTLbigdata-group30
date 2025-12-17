import streamlit as st
import pandas as pd
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
import os

# Configuration
API_URL = os.environ.get("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="University Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'page' not in st.session_state:
    st.session_state['page'] = 'Dashboard'
if 'selected_course' not in st.session_state:
    st.session_state['selected_course'] = None
if 'selected_student' not in st.session_state:
    st.session_state['selected_student'] = None

# Sidebar Navigation
st.sidebar.title("🎓 Navigation")
page = st.sidebar.radio(
    "Select View",
    ["Dashboard", "Courses", "Students"],
    index=["Dashboard", "Courses", "Students"].index(st.session_state['page'])
)
st.session_state['page'] = page

auto_refresh = st.sidebar.checkbox("Auto Refresh (5s)", value=False)

# ========== HELPER FUNCTIONS ==========

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

# ========== PAGE: OVERVIEW DASHBOARD ==========

def show_overview_dashboard():
    st.title("🎓 University Learning Analytics - Overview")
    st.markdown("Real-time insights from **Lambda Architecture** (Batch + Speed Layers)")
    
    col1, col2 = st.columns(2)
    
    # DAU Trend
    with col1:
        st.subheader("👥 User Activity Trend")
        dau_data = get_dau()
        if dau_data:
            df_dau = pd.DataFrame(dau_data)
            fig = px.line(df_dau, x="date", y="users", color="source", 
                         title="Active Users (Batch + Speed)", markers=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available yet.")
    
    # Course Popularity
    with col2:
        st.subheader("📚 Top Popular Courses")
        course_data = get_course_popularity()
        if course_data:
            df_course = pd.DataFrame(course_data)
            fig = px.bar(df_course, x="course_id", y="interactions", 
                        title="Course Interactions", color="interactions")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available yet.")
    
    # Video Engagement
    st.subheader("🎥 Real-time Video Engagement")
    video_data = get_video_stats()
    if video_data:
        df_video = pd.DataFrame(video_data)
        if not df_video.empty:
            df_video = df_video.sort_values("views", ascending=False).head(15)
            fig = px.bar(df_video, x="video_id", y="views", 
                        title="Top Videos (Current Window)", color="views")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No video data available yet.")

# ========== PAGE: COURSES VIEW ==========

def show_courses_view():
    st.title("📚 Course Analytics")
    
    # Fetch courses
    try:
        response = requests.get(f"{API_URL}/analytics/courses")
        courses = response.json() if response.status_code == 200 else []
    except:
        courses = []
        st.error("Failed to load courses")
    
    if not courses:
        st.warning("No course data available. Run batch job first.")
        return
    
    # Course selection
    course_ids = [c['course_id'] for c in courses]
    
    # Use session state if navigating back
    if st.session_state['selected_course'] and st.session_state['selected_course'] in course_ids:
        default_idx = course_ids.index(st.session_state['selected_course'])
        st.session_state['selected_course'] = None  # Reset
    else:
        default_idx = 0
    
    selected_course = st.selectbox("Select Course", course_ids, index=default_idx)
    
    if selected_course:
        # Show course overview
        course_info = [c for c in courses if c['course_id'] == selected_course][0]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Total Students", course_info.get('total_enrolled_students', 0))
        col2.metric("🎥 Videos Watched", course_info.get('total_videos_watched', 0))
        col3.metric("📥 Materials Downloaded", course_info.get('total_materials_downloaded', 0))
        
        st.markdown("---")
        
        # Show students in course
        st.subheader(f"Students in {selected_course}")
        try:
            response = requests.get(f"{API_URL}/analytics/course/{selected_course}/students")
            students = response.json() if response.status_code == 200 else []
        except:
            students = []
        
        if students:
            df_students = pd.DataFrame(students)
            st.dataframe(df_students, use_container_width=True)
            
            # Student selection
            student_ids = df_students['student_id'].tolist()
            selected_student = st.selectbox("View Student Details", ["Select..."] + student_ids)
            
            if selected_student != "Select...":
                st.markdown(f"### Performance: {selected_student} in {selected_course}")
                
                try:
                    response = requests.get(
                        f"{API_URL}/analytics/student/{selected_student}/course/{selected_course}"
                    )
                    perf = response.json() if response.status_code == 200 else {}
                except:
                    perf = {}
                
                if perf:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🎥 Videos Watched", perf.get('videos_watched', 0))
                    col2.metric("⏱️ Watch Time (min)", round(perf.get('watch_time_minutes', 0), 1))
                    col3.metric("📥 Materials Downloaded", perf.get('materials_downloaded', 0))
                    
                    # Button to navigate to student profile
                    if st.button(f"👤 View Full Profile: {selected_student}"):
                        st.session_state['selected_student'] = selected_student
                        st.session_state['page'] = 'Students'
                        st.rerun()
        else:
            st.info("No students enrolled yet")

# ========== PAGE: STUDENTS VIEW ==========

def show_students_view():
    st.title("👥 Student Analytics")
    
    # Check if navigating from course view
    if st.session_state['selected_student']:
        default_student = st.session_state['selected_student']
        st.session_state['selected_student'] = None
    else:
        default_student = None
    
    # Fetch students
    try:
        response = requests.get(f"{API_URL}/analytics/students")
        students = response.json() if response.status_code == 200 else []
    except:
        students = []
        st.error("Failed to load students")
    
    if not students:
        st.warning("No student data available. Run batch job first.")
        return
    
    # Student selection
    student_ids = [s['student_id'] for s in students]
    
    if default_student and default_student in student_ids:
        default_idx = student_ids.index(default_student)
    else:
        default_idx = 0
    
    selected_student = st.selectbox("Select Student", student_ids, index=default_idx)
    
    if selected_student:
        # Show student overview
        student_info = [s for s in students if s['student_id'] == selected_student][0]
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("📚 Courses Enrolled", student_info.get('total_courses_enrolled', 0))
        col2.metric("🎥 Videos Watched", student_info.get('total_videos_watched', 0))
        col3.metric("📝 Assignments Submitted", student_info.get('total_assignments_submitted', 0))
        col4.metric("🔐 Total Logins", student_info.get('total_logins', 0))
        
        st.markdown("---")
        
        # Show courses enrolled
        st.subheader(f"Courses for {selected_student}")
        try:
            response = requests.get(f"{API_URL}/analytics/student/{selected_student}/courses")
            courses = response.json() if response.status_code == 200 else []
        except:
            courses = []
        
        if courses:
            df_courses = pd.DataFrame(courses)
            st.dataframe(df_courses, use_container_width=True)
            
            # Course selection
            course_ids = df_courses['course_id'].tolist()
            selected_course = st.selectbox("View Course Details", ["Select..."] + course_ids)
            
            if selected_course != "Select...":
                st.markdown(f"### Performance: {selected_student} in {selected_course}")
                
                try:
                    response = requests.get(
                        f"{API_URL}/analytics/student/{selected_student}/course/{selected_course}"
                    )
                    perf = response.json() if response.status_code == 200 else {}
                except:
                    perf = {}
                
                if perf:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🎥 Videos Watched", perf.get('videos_watched', 0))
                    col2.metric("⏱️ Watch Time (min)", round(perf.get('watch_time_minutes', 0), 1))
                    col3.metric("📥 Materials Downloaded", perf.get('materials_downloaded', 0))
                    
                    # Button to navigate to course view
                    if st.button(f"📚 View Full Course Details: {selected_course}"):
                        st.session_state['selected_course'] = selected_course
                        st.session_state['page'] = 'Courses'
                        st.rerun()
        else:
            st.info("Student not enrolled in any courses yet")

# ========== MAIN ROUTING ==========

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
