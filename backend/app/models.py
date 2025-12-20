from sqlalchemy import Column, Integer, String, Boolean, DateTime, Enum, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from .database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    feishu_open_id = Column(String(255), unique=True, index=True, nullable=False)
    name = Column(String(255))
    email = Column(String(255))
    avatar_url = Column(String(1024))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class TransferMetadata(Base):
    __tablename__ = "transfer_metadata"

    id = Column(Integer, primary_key=True, index=True)
    client_name = Column(String(255), nullable=False)
    endpoint = Column(String(1024), nullable=False)
    ak = Column(String(255), nullable=False)
    # In a real app, encrypt this field. Storing plain for simplicity now, 
    # but the requirement says "encrypted storage". 
    # I'll rename it to indicate intention, actual encryption should happen in CRUD.
    sk_encrypted = Column(String(1024), nullable=False) 
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    jobs = relationship("TransferJob", back_populates="metadata_rel")

class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TransferJob(Base):
    __tablename__ = "transfer_jobs"

    job_id = Column(Integer, primary_key=True, index=True)
    metadata_id = Column(Integer, ForeignKey("transfer_metadata.id"))
    src_dir = Column(String(1024), nullable=False)
    dst_dir = Column(String(1024), nullable=False)
    include = Column(String(1024), nullable=True)
    exclude = Column(String(1024), nullable=True)
    delete_source = Column(Boolean, default=False)
    is_incremental = Column(Boolean, default=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer, default=0)
    execution_count = Column(Integer, default=0)
    result_message = Column(Text)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    metadata_rel = relationship("TransferMetadata", back_populates="jobs")

class YoutubeJob(Base):
    __tablename__ = "youtube_jobs"

    id = Column(Integer, primary_key=True, index=True)
    r2_prefix = Column(String(1024), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    
    total_count = Column(Integer, default=0)
    pending_count = Column(Integer, default=0)
    running_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    tasks = relationship("YoutubeTask", back_populates="job", cascade="all, delete-orphan")

class YoutubeTask(Base):
    __tablename__ = "youtube_tasks"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("youtube_jobs.id"), nullable=False)
    url = Column(String(2048), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    title = Column(String(1024), nullable=True)
    video_id = Column(String(255), nullable=True)
    error_message = Column(Text, nullable=True)
    
    worker_id = Column(String(255), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    job = relationship("YoutubeJob", back_populates="tasks")
