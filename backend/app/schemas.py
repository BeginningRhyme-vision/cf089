from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from .models import JobStatus

# User Schemas
class UserBase(BaseModel):
    feishu_open_id: str
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None

class UserCreate(UserBase):
    pass

class User(UserBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

# Metadata Schemas
class MetadataBase(BaseModel):
    client_name: str
    endpoint: str
    ak: str

class MetadataCreate(MetadataBase):
    sk: str # Input as plain text, stored encrypted

class MetadataUpdate(BaseModel):
    client_name: Optional[str] = None
    endpoint: Optional[str] = None
    ak: Optional[str] = None
    sk: Optional[str] = None

class Metadata(MetadataBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    # We generally don't return the secret key or return a masked version
    
    class Config:
        from_attributes = True

# Job Schemas
class JobBase(BaseModel):
    metadata_id: int
    src_dir: str
    dst_dir: str
    include: Optional[str] = None
    exclude: Optional[str] = None
    delete_source: bool = False
    is_incremental: bool = False

class JobCreate(JobBase):
    pass

class Job(JobBase):
    job_id: int
    status: JobStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = 0
    execution_count: int = 0
    result_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str
    user: User

# Youtube Job Schemas
class YoutubeTaskBase(BaseModel):
    url: str
    status: JobStatus = JobStatus.PENDING
    title: Optional[str] = None
    video_id: Optional[str] = None
    error_message: Optional[str] = None

class YoutubeTask(YoutubeTaskBase):
    id: int
    job_id: int
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class YoutubeTaskPage(BaseModel):
    items: List[YoutubeTask]
    total: int
    page: int
    size: int

class YoutubeTaskUpdate(BaseModel):
    status: Optional[JobStatus] = None
    title: Optional[str] = None
    video_id: Optional[str] = None
    error_message: Optional[str] = None

class YoutubeJobCreate(BaseModel):
    r2_prefix: str
    urls: List[str] # List of URLs to process

class YoutubeJobStatusUpdate(BaseModel):
    status: JobStatus

class YoutubeJob(BaseModel):
    id: int
    r2_prefix: str
    status: JobStatus
    created_at: datetime
    
    total_count: int = 0
    pending_count: int = 0
    running_count: int = 0
    success_count: int = 0
    failed_count: int = 0

    class Config:
        from_attributes = True

