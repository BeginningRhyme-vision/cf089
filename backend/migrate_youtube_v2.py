import sys
import os
from sqlalchemy import create_engine, text
from app.config import settings

# Setup path to import app modules if needed, though we use raw SQL here for migration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def migrate():
    print(f"Connecting to database: {settings.db_url}")
    engine = create_engine(settings.db_url)
    
    with engine.connect() as conn:
        # 1. Add columns to youtube_jobs
        print("Adding columns to youtube_jobs...")
        try:
            conn.execute(text("ALTER TABLE youtube_jobs ADD COLUMN total_count INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE youtube_jobs ADD COLUMN pending_count INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE youtube_jobs ADD COLUMN running_count INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE youtube_jobs ADD COLUMN success_count INTEGER DEFAULT 0"))
            conn.execute(text("ALTER TABLE youtube_jobs ADD COLUMN failed_count INTEGER DEFAULT 0"))
            print("Columns added to youtube_jobs.")
        except Exception as e:
            print(f"Skipping add columns (might already exist): {e}")

        # 2. Rename youtube_records to youtube_tasks
        print("Renaming youtube_records to youtube_tasks...")
        try:
            # Check if youtube_records exists
            result = conn.execute(text("SELECT to_regclass('public.youtube_records')"))
            if result.scalar():
                conn.execute(text("ALTER TABLE youtube_records RENAME TO youtube_tasks"))
                print("Table renamed.")
            else:
                print("Table youtube_records not found (maybe already renamed).")
        except Exception as e:
            print(f"Error renaming table: {e}")

        # 3. Add columns to youtube_tasks
        print("Adding columns to youtube_tasks...")
        try:
            conn.execute(text("ALTER TABLE youtube_tasks ADD COLUMN worker_id VARCHAR(255)"))
            conn.execute(text("ALTER TABLE youtube_tasks ADD COLUMN started_at TIMESTAMP WITH TIME ZONE"))
            conn.execute(text("ALTER TABLE youtube_tasks ADD COLUMN completed_at TIMESTAMP WITH TIME ZONE"))
            print("Columns added to youtube_tasks.")
        except Exception as e:
            print(f"Skipping add columns to tasks (might already exist): {e}")

        # 4. Populate counts in youtube_jobs
        print("Populating counts in youtube_jobs...")
        try:
            # We need to calculate counts for each job from the tasks table
            # PostgreSQL syntax for update with join/subquery
            
            # Reset all to 0 first
            conn.execute(text("UPDATE youtube_jobs SET total_count=0, pending_count=0, running_count=0, success_count=0, failed_count=0"))
            
            # Update total_count
            conn.execute(text("""
                UPDATE youtube_jobs 
                SET total_count = sub.cnt
                FROM (SELECT job_id, COUNT(*) as cnt FROM youtube_tasks GROUP BY job_id) as sub
                WHERE youtube_jobs.id = sub.job_id
            """))
            
            # Update pending_count (PENDING)
            conn.execute(text("""
                UPDATE youtube_jobs 
                SET pending_count = sub.cnt
                FROM (SELECT job_id, COUNT(*) as cnt FROM youtube_tasks WHERE status = 'PENDING' GROUP BY job_id) as sub
                WHERE youtube_jobs.id = sub.job_id
            """))
            
            # Update running_count (RUNNING)
            conn.execute(text("""
                UPDATE youtube_jobs 
                SET running_count = sub.cnt
                FROM (SELECT job_id, COUNT(*) as cnt FROM youtube_tasks WHERE status = 'RUNNING' GROUP BY job_id) as sub
                WHERE youtube_jobs.id = sub.job_id
            """))
            
            # Update success_count (COMPLETED)
            conn.execute(text("""
                UPDATE youtube_jobs 
                SET success_count = sub.cnt
                FROM (SELECT job_id, COUNT(*) as cnt FROM youtube_tasks WHERE status = 'COMPLETED' GROUP BY job_id) as sub
                WHERE youtube_jobs.id = sub.job_id
            """))
            
            # Update failed_count (FAILED)
            conn.execute(text("""
                UPDATE youtube_jobs 
                SET failed_count = sub.cnt
                FROM (SELECT job_id, COUNT(*) as cnt FROM youtube_tasks WHERE status = 'FAILED' GROUP BY job_id) as sub
                WHERE youtube_jobs.id = sub.job_id
            """))

            print("Counts populated.")
        except Exception as e:
            print(f"Error populating counts: {e}")

        # 5. Create Index on youtube_tasks(job_id, status)
        print("Creating index on youtube_tasks...")
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_tasks_job_status ON youtube_tasks (job_id, status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_tasks_status ON youtube_tasks (status)"))
            print("Indexes created.")
        except Exception as e:
            print(f"Error creating index: {e}")

        conn.commit()
        print("Migration completed successfully.")

if __name__ == "__main__":
    migrate()
