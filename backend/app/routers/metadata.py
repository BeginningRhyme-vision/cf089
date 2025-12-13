from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from .. import schemas, models, database

router = APIRouter(
    prefix="/metadata",
    tags=["metadata"]
)

@router.post("/", response_model=schemas.Metadata)
def create_metadata(meta: schemas.MetadataCreate, db: Session = Depends(database.get_db)):
    # Encrypt SK here. For now, we store it with a prefix to simulate encryption.
    encrypted_sk = f"enc_{meta.sk}"
    
    db_meta = models.TransferMetadata(
        client_name=meta.client_name,
        endpoint=meta.endpoint,
        ak=meta.ak,
        sk_encrypted=encrypted_sk
    )
    db.add(db_meta)
    db.commit()
    db.refresh(db_meta)
    return db_meta

@router.get("/", response_model=List[schemas.Metadata])
def read_metadata_list(skip: int = 0, limit: int = 100, db: Session = Depends(database.get_db)):
    metadata = db.query(models.TransferMetadata).offset(skip).limit(limit).all()
    return metadata

@router.get("/{metadata_id}", response_model=schemas.Metadata)
def read_metadata(metadata_id: int, db: Session = Depends(database.get_db)):
    db_meta = db.query(models.TransferMetadata).filter(models.TransferMetadata.id == metadata_id).first()
    if db_meta is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    return db_meta

@router.put("/{metadata_id}", response_model=schemas.Metadata)
def update_metadata(metadata_id: int, meta: schemas.MetadataUpdate, db: Session = Depends(database.get_db)):
    db_meta = db.query(models.TransferMetadata).filter(models.TransferMetadata.id == metadata_id).first()
    if db_meta is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    if meta.client_name:
        db_meta.client_name = meta.client_name
    if meta.endpoint:
        db_meta.endpoint = meta.endpoint
    if meta.ak:
        db_meta.ak = meta.ak
    if meta.sk:
        db_meta.sk_encrypted = f"enc_{meta.sk}"
        
    db.commit()
    db.refresh(db_meta)
    return db_meta

@router.delete("/{metadata_id}")
def delete_metadata(metadata_id: int, db: Session = Depends(database.get_db)):
    db_meta = db.query(models.TransferMetadata).filter(models.TransferMetadata.id == metadata_id).first()
    if db_meta is None:
        raise HTTPException(status_code=404, detail="Metadata not found")
    
    db.delete(db_meta)
    db.commit()
    return {"ok": True}
