from postgresql_client import Base
from sqlalchemy import BigInteger, Column, String, SmallInteger, text
from sqlalchemy.dialects.postgresql import BIT


# CONTEXTLEVEL
# CONTEXT_SYSTEM = 10
# _USER = 30
# _COURSECAT = 40
# _COURSE = 50
# _MODULE = 70
# _BLOCK = 80


#EDULEVEL
# Other = 0
# Teaching = 1
# Participating = 2



class Log(Base):
    __tablename__ = "logs"
    user_id = Column(name='userid', type_=BigInteger(), default=None)
    id = Column(name='id', type_=BigInteger(), primary_key=True, autoincrement=True)
    event_name = Column(name='eventname', type_=String(255))
    component = Column(name='component', type_=String(100))
    action = Column(name='action', type_=String(100))
    target = Column(name='target', type_=String(100))
    object_table = Column(name='objecttable', type_=String(50), nullable=True, default=None)
    object_id = Column(name='objectid', type_=BigInteger(), nullable=True, default=None)
    crud = Column(name='crud', type_=String(1))
    edu_level = Column(name='edulevel', type_=SmallInteger(), server_default=None)
    context_id = Column(name='contextid', type_=BigInteger(), server_default=None)
    context_level = Column(name='contextlevel', type_=BigInteger(), default=None)
    context_instance_id = Column(name='contextinstanceid', type_=BigInteger(), server_default=None)
    course_id = Column(name='courseid', type_=BigInteger(), nullable=True, default=None)
    anonymous = Column(name='anonymous', type_=BIT(1), server_default=text("B'0'"))
    time_created = Column(name='timecreated', type_=BigInteger(), default=None)
    origin = Column(name='origin', type_=String(10), nullable=True, default=None)
    ip = Column(name='ip', type_=String(45), nullable=True, default=None)
    related_user_id = Column(name='relateduserid', type_=BigInteger(), nullable=True, default=None)
    real_user_id = Column(name='realduserid', type_=BigInteger(), nullable=True, default=None)


# class User(Base):
#     __tablename__ = "users"
#     id = Column(name='id', type_=BigInteger(19), primary_key=True, autoincrement=True)
#     username = Column(name='username', type_=String(100))
#     password = Column(name='password', type_=String(255))
#     first_name = Column(name='firstname', type_=String(100))
#     last_name = Column(name='lastname', type_=String(100))
#     email = Column(name='email', type_=String(100))
#     phone = Column(name='phone', type_=String(20))
#     institution = Column(name='institution', type_=String(255), nullable=True)
#     department = Column(name='department', type_=String(255), nullable=True)
#     first_access = Column(name='firstaccess', type_=BigInteger(19), default=0)
#     last_access = Column(name='lastaccess', type_=BigInteger(19), default=0)
#     last_login = Column(name='lastlogin', type_=BigInteger(19), default=0)
#     current_login = Column(name='currentlogin', type_=BigInteger(19), default=0)
#     last_ip = Column(name='lastip', type_=String(45))
#     time_created = Column(name='timecreated', type_=BigInteger(19), default=0)
#     time_modified = Column(name='timemodified', type_=BigInteger(19), default=0)

# class Course(Base):
#     __tablename__ = "courses"
#     id = Column(name='id', type_=BigInteger(19), primary_key=True, autoincrement=True)
#     category = Column(ForeignKey("course_categories"), name='categoty', type_=BigInteger(19), default=0)
#     start_date = Column(name='startdate', type_=BigInteger(19), default=0)
#     end_date = Column(name='enddate', type_=BigInteger(19), default=0)
#     time_created = Column(name='timecreated', type_=BigInteger(19), default=0)
#     time_modified = Column(name='timemodified', type_=BigInteger(19), default=0)

# class CourseCategory(Base):
#     __tablename__ = "course_categories"
#     id = Column(name='id', type_=BigInteger(19), primary_key=True, autoincrement=True)
#     name = Column(name='name', type_=String(255))
#     course_count = Column(name='coursecount', type_=BigInteger(19), default=0)
#     time_modified = Column(name='timemodified', type_=BigInteger(19), default=0)

    

    