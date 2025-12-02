import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Cấu hình Kafka
KAFKA_TOPIC_AUTH = 'auth_topic'
KAFKA_TOPIC_ASSESSMENT = 'assessment_topic'
KAFKA_TOPIC_VIDEO = 'video_topic'
KAFKA_TOPIC_COURSE = 'course_topic'
KAFKA_TOPIC_PROFILE = 'profile_topic'
BOOTSTRAP_SERVERS = ['localhost:9092']

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

fake = Faker()

# Danh sách ID giả định
STUDENT_IDS = [f"SV{i:03d}" for i in range(1, 21)]
TEACHER_IDS = [f"GV{i:03d}" for i in range(1, 6)]
COURSE_IDS = [f"COURSE{i:03d}" for i in range(1, 11)]

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- PRODUCER FUNCTIONS ---

def generate_auth_event():
    """Auth Producer: Login, Logout, Signup"""
    user_type = random.choice(['student', 'teacher'])
    user_id = random.choice(STUDENT_IDS) if user_type == 'student' else random.choice(TEACHER_IDS)
    action = random.choice(['LOGIN', 'LOGOUT', 'SIGNUP'])
    
    data = {
        "event_category": "AUTH",
        "user_id": user_id,
        "role": user_type,
        "action": action,
        "timestamp": get_timestamp(),
        "ip_address": fake.ipv4()
    }
    return KAFKA_TOPIC_AUTH, data

def generate_assessment_event():
    """Assessment Producer: Submit, Grade, View, Edit, Delete"""
    action = random.choice([
        'VIEW_ASSIGNMENT', 'SUBMIT_ASSIGNMENT', 'EDIT_ASSIGNMENT', 
        'DELETE_ASSIGNMENT', 'GRADE_ASSIGNMENT', 'VIEW_STUDENT_WORK'
    ])
    
    user_id = random.choice(TEACHER_IDS) if action in ['GRADE_ASSIGNMENT', 'VIEW_STUDENT_WORK'] else random.choice(STUDENT_IDS)
    
    data = {
        "event_category": "ASSESSMENT",
        "user_id": user_id,
        "assignment_id": f"ASM{random.randint(1, 50)}",
        "action": action,
        "timestamp": get_timestamp()
    }
    
    # Logic thêm field tùy action
    if action == 'SUBMIT_ASSIGNMENT':
        data['file_url'] = fake.url()
    elif action == 'GRADE_ASSIGNMENT':
        data['score'] = random.randint(0, 100)
        data['graded_student_id'] = random.choice(STUDENT_IDS)
    elif action == 'VIEW_ASSIGNMENT':
        data['is_first_view'] = random.choice([True, False])
        
    return KAFKA_TOPIC_ASSESSMENT, data

def generate_video_tracking_event():
    """Video Tracking Producer"""
    action = random.choice(['START_VIDEO', 'STOP_VIDEO'])
    data = {
        "event_category": "VIDEO",
        "user_id": random.choice(STUDENT_IDS),
        "video_id": f"VID{random.randint(1, 100)}",
        "action": action,
        "timestamp": get_timestamp(),
        "session_id": fake.uuid4()
    }
    
    if action == 'STOP_VIDEO':
        data['duration_watched_seconds'] = random.randint(10, 3600)
        
    return KAFKA_TOPIC_VIDEO, data

def generate_course_interaction_event():
    """Course Interaction Producer"""
    action = random.choice([
        'COURSE_CREATED', 'COURSE_DELETED', 'COURSE_COMPLETED', 
        'COURSE_VIEW', 'COURSE_UPDATED', 'ITEM_CLICK', 'DOWNLOAD_MATERIAL'
    ])
    
    # GV tạo/sửa/xóa, SV học/click/tải
    if action in ['COURSE_CREATED', 'COURSE_DELETED', 'COURSE_UPDATED']:
        user_id = random.choice(TEACHER_IDS)
    else:
        user_id = random.choice(STUDENT_IDS)

    data = {
        "event_category": "COURSE",
        "user_id": user_id,
        "course_id": random.choice(COURSE_IDS),
        "action": action,
        "timestamp": get_timestamp()
    }
    
    if action == 'ITEM_CLICK':
        data['item_id'] = f"ITEM{random.randint(1, 200)}"
    elif action == 'DOWNLOAD_MATERIAL':
        data['material_name'] = fake.file_name(extension='pdf')

    return KAFKA_TOPIC_COURSE, data

def generate_profile_event():
    """Profile Producer"""
    action = random.choice(['CREATE_PROFILE', 'UPDATE_PROFILE'])
    user_id = random.choice(STUDENT_IDS + TEACHER_IDS)
    
    data = {
        "event_category": "PROFILE",
        "user_id": user_id,
        "action": action,
        "timestamp": get_timestamp(),
        "email": fake.email(),
        "full_name": fake.name()
    }
    return KAFKA_TOPIC_PROFILE, data

# --- MAIN LOOP ---

def simulate():
    print("Bắt đầu mô phỏng dữ liệu (Ctrl+C để dừng)...")
    try:
        while True:
            # Random chọn loại event để gửi
            event_generators = [
                generate_auth_event, 
                generate_assessment_event, 
                generate_video_tracking_event,
                generate_course_interaction_event,
                generate_profile_event
            ]
            
            generator = random.choice(event_generators)
            topic, data = generator()
            
            # Gửi tin nhắn vào Kafka
            producer.send(topic, data)
            print(f"Sent to {topic}: {data['action']} by {data['user_id']}")
            
            # Ngủ 1 chút để không spam quá nhanh
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("\nĐã dừng mô phỏng.")
        producer.close()

if __name__ == "__main__":
    simulate()