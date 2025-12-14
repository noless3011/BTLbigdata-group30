# Serving Layer API Reference

## Tổng quan

Serving Layer cung cấp REST API để query dữ liệu từ batch views và speed views, merge lại để trả về kết quả đầy đủ và real-time.

## Base URL

```
http://localhost:8000
```

## API Endpoints

### 1. Student APIs

#### `GET /api/v1/student/{student_id}/engagement`
Lấy metrics tổng hợp về engagement của student.

**Query Parameters:**
- `start_date` (optional): YYYY-MM-DD
- `end_date` (optional): YYYY-MM-DD

**Response:**
- Video engagement
- Auth activity
- Course activity

---

### 2. Assessment APIs

#### `GET /api/v1/assessment/student/{student_id}/submissions`
Lấy lịch sử submission và performance của student.

**Query Parameters:**
- `course_id` (optional): Filter by course
- `start_date` (optional): YYYY-MM-DD

**Covers:**
- ✅ Thời điểm xem bài tập lần đầu tiên (SV)
- ✅ Thời điểm nộp bài tập (SV)
- ✅ Thời điểm chỉnh sửa bài tập (SV)
- ✅ Thời điểm xóa bài tập (SV)
- ✅ Điểm số đạt được (quiz) (SV)

#### `GET /api/v1/assessment/quiz/{quiz_id}/performance`
Lấy analytics về performance của quiz.

**Query Parameters:**
- `course_id` (optional): Filter by course

**Covers:**
- ✅ Điểm số quiz
- ✅ Completion rates
- ✅ Performance distribution

#### `GET /api/v1/assessment/teacher/{teacher_id}/workload`
Lấy grading workload của teacher.

**Query Parameters:**
- `start_date` (optional): YYYY-MM-DD
- `end_date` (optional): YYYY-MM-DD

**Covers:**
- ✅ Thời điểm xem bài làm của sinh viên (GV)
- ✅ Thời điểm và điểm số chấm cho bài làm (GV)
- ✅ Grading history

#### `GET /api/v1/assessment/assignment/{assignment_id}/analytics`
Lấy analytics tổng hợp cho một assignment.

**Query Parameters:**
- `include_timeline` (default: true): Include view/submit/edit timeline

**Covers:**
- ✅ Submission rates
- ✅ View patterns
- ✅ Edit/delete actions
- ✅ Grading status

---

### 3. Auth APIs

#### `GET /api/v1/auth/user/{user_id}/activity`
Lấy authentication activity của user.

**Query Parameters:**
- `start_date` (optional): YYYY-MM-DD
- `end_date` (optional): YYYY-MM-DD

**Covers:**
- ✅ Thời điểm login (SV+GV)
- ✅ Thời điểm logout (SV+GV)
- ✅ Thời điểm signup (SV+GV)
- ✅ Session durations

#### `GET /api/v1/auth/sessions/active`
Lấy danh sách active sessions (real-time).

**Query Parameters:**
- `role` (optional): Filter by role (student/teacher)

**Covers:**
- ✅ Online users realtime
- ✅ Active sessions

#### `GET /api/v1/auth/login-patterns`
Lấy login patterns và peak usage times.

**Query Parameters:**
- `date` (optional): YYYY-MM-DD (default: today)
- `hourly` (default: false): Return hourly breakdown

**Covers:**
- ✅ Hourly login distribution
- ✅ Peak usage analysis

---

### 4. Video APIs

#### `GET /api/v1/video/{video_id}/analytics`
Lấy analytics cho một video.

**Query Parameters:**
- `course_id` (optional): Filter by course

**Covers:**
- ✅ Thời điểm bắt đầu xem video (SV)
- ✅ Thời điểm kết thúc xem video (SV)
- ✅ Watch time
- ✅ Completion rates

---

### 5. Course APIs

#### `GET /api/v1/course/{course_id}/statistics`
Lấy statistics tổng hợp cho course.

**Query Parameters:**
- `include_students` (default: false): Include student-level details

**Covers:**
- ✅ Enrollment stats
- ✅ Engagement metrics
- ✅ Material access patterns

#### `GET /api/v1/course/{course_id}/materials/downloads`
Lấy download analytics cho materials.

**Query Parameters:**
- `material_id` (optional): Filter by material
- `start_date` (optional): YYYY-MM-DD

**Covers:**
- ✅ Thời điểm download tài liệu (SV)
- ✅ Download counts
- ✅ Popular materials

#### `GET /api/v1/course/{course_id}/interactions`
Lấy interaction tracking chi tiết.

**Query Parameters:**
- `student_id` (optional): Filter by student
- `item_type` (optional): Filter by type (lesson, module, quiz, assignment, discussion)

**Covers:**
- ✅ Thời điểm click vào từng mục trong course (SV)
- ✅ Navigation patterns
- ✅ Engagement by section

#### `GET /api/v1/course/lifecycle`
Lấy course lifecycle events.

**Query Parameters:**
- `course_id` (optional): Filter by course
- `event_type` (optional): Filter by event (CREATED, DELETED, UPDATED, COMPLETED, VIEW)

**Covers:**
- ✅ Course created
- ✅ Course deleted
- ✅ Course completed
- ✅ Course view
- ✅ Course updated

---

### 6. Profile APIs

#### `GET /api/v1/profile/user/{user_id}/activity`
Lấy profile activity của user.

**Query Parameters:**
- `include_updates` (default: true): Include update history

**Covers:**
- ✅ Thông tin người dùng khi tạo tài khoản mới (SV+GV)
- ✅ Thông tin người dùng khi sửa đổi (SV+GV)
- ✅ Profile update frequency
- ✅ Field changes
- ✅ Avatar updates

#### `GET /api/v1/profile/updates/summary`
Lấy summary về profile updates trong system.

**Query Parameters:**
- `role` (optional): Filter by role (student/teacher)
- `start_date` (optional): YYYY-MM-DD

**Covers:**
- ✅ Update frequency across system
- ✅ Most updated fields
- ✅ Update patterns

---

### 7. Teacher APIs

#### `GET /api/v1/teacher/{teacher_id}/dashboard`
Lấy dashboard tổng hợp cho teacher.

**Query Parameters:**
- `date` (optional): YYYY-MM-DD (default: today)

**Covers:**
- ✅ Courses taught
- ✅ Grading workload
- ✅ Student engagement
- ✅ Course analytics

---

### 8. Dashboard APIs

#### `GET /api/v1/dashboard/overview`
Lấy system-wide dashboard overview.

**Query Parameters:**
- `date` (optional): YYYY-MM-DD (default: today)

**Covers:**
- ✅ Total students
- ✅ Total courses
- ✅ Daily active users
- ✅ System health

---

### 9. Utility APIs

#### `GET /api/v1/health`
Health check endpoint.

#### `GET /api/v1/views/list`
List all available batch and speed views.

---

## Mapping với Events từ Producers

### ✅ Auth Producer
- Login, Logout, Signup → `/api/v1/auth/user/{user_id}/activity`
- Online realtime → `/api/v1/auth/sessions/active`
- Login patterns → `/api/v1/auth/login-patterns`

### ✅ Assessment Producer
- Xem bài tập lần đầu → `/api/v1/assessment/student/{student_id}/submissions`
- Nộp bài tập → `/api/v1/assessment/student/{student_id}/submissions`
- Chỉnh sửa/xóa bài tập → `/api/v1/assessment/student/{student_id}/submissions`
- Điểm số quiz → `/api/v1/assessment/quiz/{quiz_id}/performance`
- Xem bài làm (GV) → `/api/v1/assessment/teacher/{teacher_id}/workload`
- Chấm điểm (GV) → `/api/v1/assessment/teacher/{teacher_id}/workload`

### ✅ Video Tracking Producer
- Bắt đầu/kết thúc xem video → `/api/v1/video/{video_id}/analytics`

### ✅ Course Interaction Producer
- Course lifecycle → `/api/v1/course/lifecycle`
- Click vào mục → `/api/v1/course/{course_id}/interactions`
- Download tài liệu → `/api/v1/course/{course_id}/materials/downloads`

### ✅ Profile Producer
- Tạo tài khoản mới → `/api/v1/profile/user/{user_id}/activity`
- Sửa đổi thông tin → `/api/v1/profile/user/{user_id}/activity`

---

## Example Requests

```bash
# Student engagement
curl http://localhost:8000/api/v1/student/SV001/engagement

# Assessment submissions
curl http://localhost:8000/api/v1/assessment/student/SV001/submissions?course_id=COURSE001

# Quiz performance
curl http://localhost:8000/api/v1/assessment/quiz/QUIZ001/performance

# Teacher workload
curl http://localhost:8000/api/v1/assessment/teacher/GV001/workload

# Auth activity
curl http://localhost:8000/api/v1/auth/user/SV001/activity?start_date=2025-12-01

# Active sessions
curl http://localhost:8000/api/v1/auth/sessions/active?role=student

# Video analytics
curl http://localhost:8000/api/v1/video/VID001/analytics?course_id=COURSE001

# Course materials downloads
curl http://localhost:8000/api/v1/course/COURSE001/materials/downloads

# Course interactions
curl http://localhost:8000/api/v1/course/COURSE001/interactions?item_type=lesson

# Profile activity
curl http://localhost:8000/api/v1/profile/user/SV001/activity

# Teacher dashboard
curl http://localhost:8000/api/v1/teacher/GV001/dashboard

# Dashboard overview
curl http://localhost:8000/api/v1/dashboard/overview?date=2025-12-15
```

---

## API Documentation

Swagger UI: http://localhost:8000/docs
ReDoc: http://localhost:8000/redoc

