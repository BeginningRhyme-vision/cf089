from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from sqlalchemy import func
from .. import schemas, models, database

router = APIRouter(
    prefix="/youtube-jobs",
    tags=["youtube-jobs"]
)

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
    
    return created_jobs

@router.get("/", response_model=List[schemas.YoutubeJob])
def read_jobs(skip: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
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

    return jobs

@router.get("/{job_id}", response_model=schemas.YoutubeJob)
def read_job(job_id: int, db: Session = Depends(database.get_db)):
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
