from fastapi import APIRouter, Depends, HTTPException, Body, File, UploadFile, Form, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional
from sqlalchemy import func, or_, not_, and_
from datetime import datetime
from .. import schemas, models, database
from ..cache import cache

router = APIRouter(
    prefix="/youtube-jobs",
    tags=["youtube-jobs"]
)

def insert_job_tasks(job_id: int, urls: List[str]):
    """Background task to insert tasks for a job."""
    db = database.SessionLocal()
    try:
        tasks = []
        for url in urls:
            tasks.append(models.YoutubeTask(
                job_id=job_id,
                url=url,
                status=models.JobStatus.PENDING
            ))
        
        # Insert in batches to manage memory and transaction size
        BATCH_SIZE = 5000
        for i in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[i : i + BATCH_SIZE]
            db.add_all(batch)
            db.commit()
            
    except Exception as e:
        print(f"Error inserting tasks for job {job_id}: {e}")
    finally:
        db.close()

@router.post("/tasks/acquire", response_model=List[schemas.YoutubeTask])
def acquire_tasks(
    worker_id: str = Body(..., embed=True), 
    limit: int = Body(10, embed=True), 
    db: Session = Depends(database.get_db)
):
    # Find pending tasks
    # Prioritize by job creation time (FIFO)
    # We join with YoutubeJob to order by job creation, but simplest is just order by task id if they are inserted in order.
    # To prevent race conditions, we use simple update-returning or select-for-update if possible.
    # For now, simplistic approach:
    
    tasks = db.query(models.YoutubeTask).filter(
        models.YoutubeTask.status == models.JobStatus.PENDING
    ).limit(limit).with_for_update(skip_locked=True).all()
    
    if not tasks:
        # Retry with FAILED tasks if needed? Or just return empty. 
        # Usually workers ask for PENDING. Retry logic is separate.
        return []
        
    task_ids = [t.id for t in tasks]
    now = datetime.now()
    
    # Bulk update tasks
    # We can't do bulk update easily with SQLAlchemy ORM + tracking changes for counters
    # So we iterate. Optimization: Group by job_id to update job counters once.
    
    job_counts = {} # job_id -> count
    
    for task in tasks:
        task.status = models.JobStatus.RUNNING
        task.worker_id = worker_id
        task.started_at = now
        
        job_counts[task.job_id] = job_counts.get(task.job_id, 0) + 1
        
    # Commit tasks update
    db.commit()
    
    # Update Job Counters
    for job_id, count in job_counts.items():
        job = db.query(models.YoutubeJob).filter(models.YoutubeJob.id == job_id).first()
        if job:
            job.pending_count = models.YoutubeJob.pending_count - count
            job.running_count = models.YoutubeJob.running_count + count
            db.add(job)
            
            # Invalidate cache for this job
            cache.delete(f"youtube_job:{job_id}")

    db.commit()
    
    # Refresh tasks to return
    for t in tasks:
        db.refresh(t)
        
    cache.invalidate_prefix("youtube_jobs_list")
    
    return tasks

@router.get("/{job_id}/tasks", response_model=List[schemas.YoutubeTask])
def read_job_tasks(job_id: int, db: Session = Depends(database.get_db)):
    # Return tasks that need attention or all? 
    # Original logic: PENDING or FAILED, excluding certain errors.
    # Since worker now acquires via /tasks/acquire, this might be for UI or debugging?
    # Or maybe the worker still uses this for specific job processing?
    # Let's keep it compatible but pointing to tasks table.
    
    tasks = db.query(models.YoutubeTask).filter(
        models.YoutubeTask.job_id == job_id
    ).all()
    return tasks

@router.patch("/tasks/{task_id}", response_model=schemas.YoutubeTask)
def update_task(task_id: int, update: schemas.YoutubeTaskUpdate, db: Session = Depends(database.get_db)):
    task = db.query(models.YoutubeTask).filter(models.YoutubeTask.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    old_status = task.status
    job_id = task.job_id
    
    if update.status:
        task.status = update.status
        if update.status in [models.JobStatus.COMPLETED, models.JobStatus.FAILED]:
            task.completed_at = datetime.now()
            
    if update.title:
        task.title = update.title
    if update.video_id:
        task.video_id = update.video_id
    if update.error_message is not None:
        task.error_message = update.error_message
        
    db.commit()
    db.refresh(task)
    
    # Update Job Counters if status changed
    if update.status and update.status != old_status:
        job = db.query(models.YoutubeJob).filter(models.YoutubeJob.id == job_id).first()
        if job:
            # Decrement old
            if old_status == models.JobStatus.PENDING:
                job.pending_count -= 1
            elif old_status == models.JobStatus.RUNNING:
                job.running_count -= 1
            elif old_status == models.JobStatus.COMPLETED:
                job.success_count -= 1
            elif old_status == models.JobStatus.FAILED:
                job.failed_count -= 1
                
            # Increment new
            if update.status == models.JobStatus.PENDING:
                job.pending_count += 1
            elif update.status == models.JobStatus.RUNNING:
                job.running_count += 1
            elif update.status == models.JobStatus.COMPLETED:
                job.success_count += 1
            elif update.status == models.JobStatus.FAILED:
                job.failed_count += 1
            
            # Recalculate total logic if needed, but total shouldn't change here usually
            # job.total_count is fixed unless we delete tasks
            
            db.commit()
            cache.delete(f"youtube_job:{job_id}")
            cache.invalidate_prefix("youtube_jobs_list")

    return task

@router.patch("/{job_id}/status", response_model=schemas.YoutubeJob)
def update_job_status(job_id: int, update: schemas.YoutubeJobStatusUpdate, db: Session = Depends(database.get_db)):
    job = db.query(models.YoutubeJob).filter(models.YoutubeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    job.status = update.status
    db.commit()
    db.refresh(job)
    
    cache.delete(f"youtube_job:{job.id}")
    cache.invalidate_prefix("youtube_jobs_list")
    
    return job

@router.post("/", response_model=List[schemas.YoutubeJob])
async def create_job(
    r2_prefix: str = Form(...),
    background_tasks: BackgroundTasks,
    urls: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    db: Session = Depends(database.get_db)
):
    unique_urls = set()
    
    # Process text input
    if urls:
        for line in urls.split('\n'):
            stripped = line.strip()
            if stripped:
                unique_urls.add(stripped)
    
    # Process file input
    if file:
        # Read file in chunks to avoid memory issues, but here we need lines.
        # SpooledTemporaryFile in FastAPI is already efficient.
        # We can iterate over the file object directly which yields lines (bytes)
        for line in file.file:
            decoded = line.decode("utf-8", errors="ignore").strip()
            if decoded:
                unique_urls.add(decoded)
    
    unique_urls_list = list(unique_urls)
    
    if not unique_urls_list:
        raise HTTPException(status_code=400, detail="No valid URLs provided")

    MAX_URLS_PER_JOB = 100000
    chunks = [unique_urls_list[i:i + MAX_URLS_PER_JOB] for i in range(0, len(unique_urls_list), MAX_URLS_PER_JOB)]
    
    created_jobs = []

    for i, chunk in enumerate(chunks):
        count = len(chunk)
        
        # Create Job with initialized counters
        db_job = models.YoutubeJob(
            r2_prefix=r2_prefix,
            status=models.JobStatus.PENDING,
            total_count=count,
            pending_count=count,
            running_count=0,
            success_count=0,
            failed_count=0
        )
        db.add(db_job)
        db.commit()
        db.refresh(db_job)
        
        # Offload task creation to background
        background_tasks.add_task(insert_job_tasks, db_job.id, chunk)
        
        created_jobs.append(db_job)
    
    cache.invalidate_prefix("youtube_jobs_list")
    
    return created_jobs

@router.get("/", response_model=List[schemas.YoutubeJob])
def read_jobs(skip: int = 0, limit: int = 10, db: Session = Depends(database.get_db)):
    # No caching needed for list anymore, it's fast
    # But user might want it cached if high traffic? 
    # Let's remove list caching for now to avoid stale counters which are now real-time in DB
    
    jobs = db.query(models.YoutubeJob).order_by(models.YoutubeJob.created_at.desc()).offset(skip).limit(limit).all()
    return jobs

@router.get("/{job_id}", response_model=schemas.YoutubeJob)
def read_job(job_id: int, db: Session = Depends(database.get_db)):
    # Caching single job is fine, but invalidate on task updates
    cache_key = f"youtube_job:{job_id}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    job = db.query(models.YoutubeJob).filter(models.YoutubeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    data = schemas.YoutubeJob.model_validate(job).model_dump(mode='json')
    cache.set(cache_key, data, expire=600)
    
    return job

@router.get("/{job_id}/records", response_model=schemas.YoutubeTaskPage)
def read_job_records(job_id: int, page: int = 1, size: int = 50, db: Session = Depends(database.get_db)):
    # Kept endpoint name /records for frontend compatibility if possible, but returning TaskPage
    skip = (page - 1) * size
    query = db.query(models.YoutubeTask).filter(models.YoutubeTask.job_id == job_id)
    total = query.count()
    tasks = query.offset(skip).limit(size).all()
    
    return schemas.YoutubeTaskPage(
        items=tasks,
        total=total,
        page=page,
        size=size
    )

@router.delete("/pending")
def delete_pending_jobs(db: Session = Depends(database.get_db)):
    pending_jobs = db.query(models.YoutubeJob).filter(models.YoutubeJob.status == models.JobStatus.PENDING).all()
    
    if not pending_jobs:
        return {"message": "No pending jobs to delete"}
    
    ids = [job.id for job in pending_jobs]
    
    # Delete Tasks first
    db.query(models.YoutubeTask).filter(models.YoutubeTask.job_id.in_(ids)).delete(synchronize_session=False)
    
    # Delete Jobs
    db.query(models.YoutubeJob).filter(models.YoutubeJob.id.in_(ids)).delete(synchronize_session=False)
    
    db.commit()
    
    cache.invalidate_prefix("youtube_jobs_list")
    for jid in ids:
        cache.delete(f"youtube_job:{jid}")
    
    return {"message": f"Deleted {len(ids)} pending jobs"}