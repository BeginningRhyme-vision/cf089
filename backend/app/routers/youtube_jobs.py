from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from sqlalchemy import func, or_, not_, and_
from .. import schemas, models, database
from ..cache import cache

router = APIRouter(
    prefix="/youtube-jobs",
    tags=["youtube-jobs"]
)

@router.post("/acquire", response_model=schemas.YoutubeJob)
def acquire_job(db: Session = Depends(database.get_db)):
    # Find a PENDING job first
    job = db.query(models.YoutubeJob).filter(
        models.YoutubeJob.status == models.JobStatus.PENDING
    ).order_by(models.YoutubeJob.created_at.asc()).first() 

    if not job:
         # Fallback to RUNNING job (if restarting or taking over)
         job = db.query(models.YoutubeJob).filter(
             models.YoutubeJob.status == models.JobStatus.RUNNING
         ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="No pending or running jobs found")
    
    # Lock it
    job.status = models.JobStatus.RUNNING
    db.commit()
    db.refresh(job)
    
    cache.delete(f"youtube_job:{job.id}")
    cache.invalidate_prefix("youtube_jobs_list")
    
    return job

@router.get("/{job_id}/tasks", response_model=List[schemas.YoutubeRecord])
def read_job_tasks(job_id: int, db: Session = Depends(database.get_db)):
    records = db.query(models.YoutubeRecord).filter(
        models.YoutubeRecord.job_id == job_id,
        models.YoutubeRecord.status.in_([models.JobStatus.PENDING, models.JobStatus.FAILED]),
        or_(
            models.YoutubeRecord.error_message == None,
            and_(
                not_(models.YoutubeRecord.error_message.contains("Video unavailable")),
                not_(models.YoutubeRecord.error_message.contains("This video is private"))
            )
        )
    ).all()
    return records

@router.patch("/records/{record_id}", response_model=schemas.YoutubeRecord)
def update_record(record_id: int, update: schemas.YoutubeRecordUpdate, db: Session = Depends(database.get_db)):
    record = db.query(models.YoutubeRecord).filter(models.YoutubeRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    
    if update.status:
        record.status = update.status
    if update.title:
        record.title = update.title
    if update.video_id:
        record.video_id = update.video_id
    if update.error_message is not None: # Allow clearing error message if passed as empty string or distinct value
        record.error_message = update.error_message
        
    db.commit()
    db.refresh(record)
    return record

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
def create_job(job_in: schemas.YoutubeJobCreate, db: Session = Depends(database.get_db)):
    # Deduplicate URLs
    unique_urls = list(set(url.strip() for url in job_in.urls if url.strip()))
    
    if not unique_urls:
        raise HTTPException(status_code=400, detail="No valid URLs provided")

    MAX_URLS_PER_JOB = 100000
    chunks = [unique_urls[i:i + MAX_URLS_PER_JOB] for i in range(0, len(unique_urls), MAX_URLS_PER_JOB)]
    
    created_jobs = []

    for i, chunk in enumerate(chunks):
        # Create Job
        db_job = models.YoutubeJob(
            r2_prefix=job_in.r2_prefix,
            status=models.JobStatus.PENDING
        )
        db.add(db_job)
        db.flush() # Get ID

        # Create Records
        records = []
        
        for url in chunk:
            records.append(models.YoutubeRecord(
                job_id=db_job.id,
                url=url,
                status=models.JobStatus.PENDING
            ))
        
        if records:
            db.add_all(records)
        
        db.commit()
        db.refresh(db_job)
        
        # Manually populate counts for response
        db_job.total_count = len(records)
        db_job.pending_count = len(records)
        db_job.success_count = 0
        db_job.failed_count = 0
        
        created_jobs.append(db_job)
    
    cache.invalidate_prefix("youtube_jobs_list")
    
    # Cache the new jobs
    for job in created_jobs:
        data = schemas.YoutubeJob.model_validate(job).model_dump(mode='json')
        cache.set(f"youtube_job:{job.id}", data, expire=600)
    
    return created_jobs

@router.get("/", response_model=List[schemas.YoutubeJob])
def read_jobs(skip: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    cache_key = f"youtube_jobs_list:{skip}:{limit}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    jobs = db.query(models.YoutubeJob).order_by(models.YoutubeJob.created_at.desc()).offset(skip).limit(limit).all()
    
    # Enrich with counts
    for job in jobs:
        counts = db.query(
            models.YoutubeRecord.status, func.count(models.YoutubeRecord.id)
        ).filter(models.YoutubeRecord.job_id == job.id).group_by(models.YoutubeRecord.status).all()
        
        count_map = {status: count for status, count in counts}
        job.total_count = sum(count_map.values())
        job.success_count = count_map.get(models.JobStatus.COMPLETED, 0)
        job.failed_count = count_map.get(models.JobStatus.FAILED, 0)
        # Pending + Running + Paused = Pending for simplicity or just explicit Pending
        job.pending_count = count_map.get(models.JobStatus.PENDING, 0) + count_map.get(models.JobStatus.RUNNING, 0)

    data = [schemas.YoutubeJob.model_validate(j).model_dump(mode='json') for j in jobs]
    cache.set(cache_key, data, expire=600)

    return jobs

@router.get("/{job_id}", response_model=schemas.YoutubeJob)
def read_job(job_id: int, db: Session = Depends(database.get_db)):
    cache_key = f"youtube_job:{job_id}"
    cached = cache.get(cache_key)
    if cached:
        return cached

    job = db.query(models.YoutubeJob).filter(models.YoutubeJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    counts = db.query(
        models.YoutubeRecord.status, func.count(models.YoutubeRecord.id)
    ).filter(models.YoutubeRecord.job_id == job.id).group_by(models.YoutubeRecord.status).all()
    
    count_map = {status: count for status, count in counts}
    job.total_count = sum(count_map.values())
    job.success_count = count_map.get(models.JobStatus.COMPLETED, 0)
    job.failed_count = count_map.get(models.JobStatus.FAILED, 0)
    job.pending_count = count_map.get(models.JobStatus.PENDING, 0) + count_map.get(models.JobStatus.RUNNING, 0)
    
    data = schemas.YoutubeJob.model_validate(job).model_dump(mode='json')
    cache.set(cache_key, data, expire=600)
    
    return job

@router.get("/{job_id}/records", response_model=schemas.YoutubeRecordPage)
def read_job_records(job_id: int, page: int = 1, size: int = 50, db: Session = Depends(database.get_db)):
    skip = (page - 1) * size
    query = db.query(models.YoutubeRecord).filter(models.YoutubeRecord.job_id == job_id)
    total = query.count()
    records = query.offset(skip).limit(size).all()
    
    return schemas.YoutubeRecordPage(
        items=records,
        total=total,
        page=page,
        size=size
    )

@router.delete("/pending")
def delete_pending_jobs(db: Session = Depends(database.get_db)):
    # Find Pending Jobs
    pending_jobs = db.query(models.YoutubeJob).filter(models.YoutubeJob.status == models.JobStatus.PENDING).all()
    
    if not pending_jobs:
        return {"message": "No pending jobs to delete"}
    
    ids = [job.id for job in pending_jobs]
    
    # Delete Records first (safe approach if cascade isn't fully trusted/configured)
    db.query(models.YoutubeRecord).filter(models.YoutubeRecord.job_id.in_(ids)).delete(synchronize_session=False)
    
    # Delete Jobs
    db.query(models.YoutubeJob).filter(models.YoutubeJob.id.in_(ids)).delete(synchronize_session=False)
    
    db.commit()
    
    cache.invalidate_prefix("youtube_jobs_list")
    for jid in ids:
        cache.delete(f"youtube_job:{jid}")
    
    return {"message": f"Deleted {len(ids)} pending jobs"}
