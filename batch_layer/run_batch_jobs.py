"""
Master Batch Job Runner
Orchestrates all batch processing jobs - can be run manually or via Oozie
"""

import sys
import os
from datetime import datetime

# Set Java options BEFORE importing PySpark
# This fixes the "getSubject is not supported" error on Windows with Java 17+
java_opts = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
# Set JAVA_TOOL_OPTIONS for all Java processes
if "JAVA_TOOL_OPTIONS" not in os.environ:
    os.environ["JAVA_TOOL_OPTIONS"] = java_opts
# Also set PYSPARK_SUBMIT_ARGS
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-java-options '{java_opts}' --conf spark.driver.extraJavaOptions='{java_opts}' --conf spark.executor.extraJavaOptions='{java_opts}' pyspark-shell"
os.environ["HADOOP_USER_NAME"] = "root"
# Disable HADOOP_HOME check for Windows (we use MinIO/S3, not HDFS)
# Create a dummy HADOOP_HOME to avoid winutils.exe error
if "HADOOP_HOME" not in os.environ:
    import tempfile
    hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_home")
    os.makedirs(hadoop_home, exist_ok=True)
    bin_dir = os.path.join(hadoop_home, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    os.environ["HADOOP_HOME"] = hadoop_home
    # Create a minimal winutils.exe stub (empty file - Spark will still try to use it)
    # Note: This may not work perfectly, but it's better than nothing
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    if not os.path.exists(winutils_path):
        # Create empty file as placeholder
        with open(winutils_path, 'wb') as f:
            f.write(b'')
    # Set hadoop.home.dir in Spark config
    os.environ["PYSPARK_SUBMIT_ARGS"] = os.environ.get("PYSPARK_SUBMIT_ARGS", "") + f" --conf spark.hadoop.hadoop.home.dir={hadoop_home}"

from pyspark.sql import SparkSession
# Import all batch jobs
sys.path.append(os.path.join(os.path.dirname(__file__), 'jobs'))

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    # Use localhost:9002 for local execution, minio:9000 for Docker/K8s
    import os
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9002")
    
    return SparkSession.builder \
        .appName("Master_Batch_Runner") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED") \
        .getOrCreate()

def run_batch_job(job_name, input_path, output_path):
    """Run a specific batch job"""
    print(f"\n{'='*80}")
    print(f"Starting Batch Job: {job_name}")
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")
    print(f"Timestamp: {datetime.now()}")
    print(f"{'='*80}\n")
    
    try:
        if job_name == "auth":
            from auth_batch_job import main as auth_main
            auth_main(input_path, output_path)
        elif job_name == "assessment":
            from assessment_batch_job import main as assessment_main
            assessment_main(input_path, output_path)
        elif job_name == "video":
            from video_batch_job import main as video_main
            video_main(input_path, output_path)
        elif job_name == "course":
            from course_batch_job import main as course_main
            course_main(input_path, output_path)
        elif job_name == "profile_notification":
            from profile_notification_batch_job import main as profile_notif_main
            profile_notif_main(input_path, output_path)
        else:
            print(f"ERROR: Unknown job name '{job_name}'")
            return False
        
        print(f"\n{'='*80}")
        print(f"✅ Batch Job '{job_name}' completed successfully!")
        print(f"{'='*80}\n")
        return True
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ Batch Job '{job_name}' FAILED!")
        print(f"Error: {str(e)}")
        print(f"{'='*80}\n")
        return False

def run_all_jobs(input_path, output_path):
    """Run all batch jobs sequentially"""
    jobs = ["auth", "assessment", "video", "course", "profile_notification"]
    results = {}
    
    print(f"\n{'#'*80}")
    print(f"# MASTER BATCH JOB RUNNER")
    print(f"# Running {len(jobs)} batch jobs")
    print(f"# Start Time: {datetime.now()}")
    print(f"{'#'*80}\n")
    
    start_time = datetime.now()
    
    for job in jobs:
        success = run_batch_job(job, input_path, output_path)
        results[job] = "✅ SUCCESS" if success else "❌ FAILED"
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Print summary
    print(f"\n{'#'*80}")
    print(f"# BATCH JOB EXECUTION SUMMARY")
    print(f"{'#'*80}")
    print(f"Start Time: {start_time}")
    print(f"End Time: {end_time}")
    print(f"Total Duration: {duration}")
    print(f"\nJob Results:")
    for job, result in results.items():
        print(f"  {job:25s} : {result}")
    print(f"{'#'*80}\n")
    
    # Check if all succeeded
    all_success = all("SUCCESS" in r for r in results.values())
    if all_success:
        print("🎉 All batch jobs completed successfully!")
    else:
        print("⚠️  Some batch jobs failed. Check logs above.")
        sys.exit(1)

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Run all jobs: python run_batch_jobs.py <input_path> <output_path>")
        print("  Run specific job: python run_batch_jobs.py <job_name> <input_path> <output_path>")
        print("\nAvailable jobs: auth, assessment, video, course, profile_notification")
        print("\nExample:")
        print("  python run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        print("  python run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    # Check if first arg is a job name or path
    if sys.argv[1] in ["auth", "assessment", "video", "course", "profile_notification"]:
        # Run specific job
        if len(sys.argv) != 4:
            print("ERROR: When running a specific job, provide: job_name input_path output_path")
            sys.exit(1)
        job_name = sys.argv[1]
        input_path = sys.argv[2]
        output_path = sys.argv[3]
        run_batch_job(job_name, input_path, output_path)
    else:
        # Run all jobs
        if len(sys.argv) != 3:
            print("ERROR: When running all jobs, provide: input_path output_path")
            sys.exit(1)
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        run_all_jobs(input_path, output_path)

if __name__ == "__main__":
    main()
