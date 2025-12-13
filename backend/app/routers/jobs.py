from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from .. import schemas, models, database

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"]
)

# --- Mock Transfer Logic ---
import time

def mock_transfer_process(job_id: int, db_session_factory):
    """
    Simulates a long-running transfer task.
    In a real app, this would be a Celery task or an asyncio background worker
    that updates the DB status as it progresses.
    """
    db = db_session_factory()
    try:
        job = db.query(models.TransferJob).filter(models.TransferJob.job_id == job_id).first()
        if not job:
            return

        job.status = models.JobStatus.RUNNING
        job.start_time = datetime.utcnow()
        db.commit()

        # Simulate work
        time.sleep(5) 
        
        # Check if stopped/paused in between (simplified)
        db.refresh(job)
        if job.status == models.JobStatus.STOPPED:
            return

        job.status = models.JobStatus.COMPLETED
        job.end_time = datetime.utcnow()
        job.duration_seconds = int((job.end_time - job.start_time).total_seconds())
        job.result_message = "Transfer completed successfully."
        job.execution_count += 1
        db.commit()
    except Exception as e:
        job.status = models.JobStatus.FAILED
        job.result_message = str(e)
        db.commit()
    finally:
        db.close()

# ---------------------------

@router.post("/", response_model=schemas.Job)
def create_job(job: schemas.JobCreate, db: Session = Depends(database.get_db)):
    # Validate metadata exists
    meta = db.query(models.TransferMetadata).filter(models.TransferMetadata.id == job.metadata_id).first()
    if not meta:
        raise HTTPException(status_code=404, detail="Metadata not found")

    db_job = models.TransferJob(
        metadata_id=job.metadata_id,
        src_dir=job.src_dir,
        dst_dir=job.dst_dir,
        delete_source=job.delete_source,
        status=models.JobStatus.PENDING
    )
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

@router.get("/", response_model=List[schemas.Job])
def read_jobs(
    skip: int = 0, 
    limit: int = 100, 
    status: Optional[models.JobStatus] = None, 
    db: Session = Depends(database.get_db)
):
    query = db.query(models.TransferJob)
    if status:
        query = query.filter(models.TransferJob.status == status)
    
    jobs = query.order_by(models.TransferJob.created_at.desc()).offset(skip).limit(limit).all()
    return jobs

@router.get("/{job_id}", response_model=schemas.Job)
def read_job(job_id: int, db: Session = Depends(database.get_db)):
    job = db.query(models.TransferJob).filter(models.TransferJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@router.post("/{job_id}/start")
def start_job(
    job_id: int, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(database.get_db)
):
    job = db.query(models.TransferJob).filter(models.TransferJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status == models.JobStatus.RUNNING:
        raise HTTPException(status_code=400, detail="Job is already running")

    # Pass the session factory to the background task so it can create its own thread-safe session
    background_tasks.add_task(mock_transfer_process, job_id, database.SessionLocal)
    
    return {"message": "Job started"}

@router.post("/{job_id}/stop")
def stop_job(job_id: int, db: Session = Depends(database.get_db)):
    job = db.query(models.TransferJob).filter(models.TransferJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status not in [models.JobStatus.RUNNING, models.JobStatus.PAUSED]:
        raise HTTPException(status_code=400, detail="Job is not running or paused")

    job.status = models.JobStatus.STOPPED
    db.commit()
    return {"message": "Job stopped"}

@router.delete("/{job_id}")
def delete_job(job_id: int, db: Session = Depends(database.get_db)):
    job = db.query(models.TransferJob).filter(models.TransferJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    db.delete(job)
    db.commit()
    return {"ok": True}
