from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import timedelta, datetime
from jose import jwt
import requests
from .. import schemas, models, database, config

router = APIRouter(
    prefix="/auth",
    tags=["auth"]
)

# Helper to create access token
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, config.settings.jwt_secret, algorithm=config.settings.jwt_algorithm)
    return encoded_jwt

@router.get("/feishu/login_url")
def get_feishu_login_url():
    """
    Returns the Feishu OAuth2 authorization URL.
    """
    app_id = config.settings.feishu_app_id
    redirect_uri = config.settings.feishu_redirect_uri
    # Feishu OAuth2 URL format
    url = f"https://open.feishu.cn/open-apis/authen/v1/index?redirect_uri={redirect_uri}&app_id={app_id}"
    return {"url": url}

@router.post("/feishu/callback", response_model=schemas.Token)
def feishu_callback(code: str, db: Session = Depends(database.get_db)):
    """
    Exchanges the auth code for a user token, retrieves user info, 
    and issues a JWT for the app.
    """
    app_id = config.settings.feishu_app_id
    app_secret = config.settings.feishu_app_secret

    # 1. Get App Access Token (Tenant Access Token)
    app_token_url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
    app_token_res = requests.post(app_token_url, json={
        "app_id": app_id,
        "app_secret": app_secret
    })
    
    if app_token_res.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to get app access token from Feishu")
    
    app_access_token = app_token_res.json().get("tenant_access_token")
    if not app_access_token:
        raise HTTPException(status_code=400, detail="Invalid app access token response")

    # 2. Get User Access Token
    token_url = "https://open.feishu.cn/open-apis/authen/v1/oidc/access_token"
    headers = {
        "Authorization": f"Bearer {app_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "grant_type": "authorization_code",
        "code": code
    }
    
    token_res = requests.post(token_url, json=payload, headers=headers)
    if token_res.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to get user access token from Feishu")
        
    token_data = token_res.json()
    if token_data.get("code") != 0: # Feishu error code
         raise HTTPException(status_code=400, detail=f"Feishu Error: {token_data.get('msg')}")

    user_access_token = token_data.get("data", {}).get("access_token")
    if not user_access_token:
        raise HTTPException(status_code=400, detail="Failed to retrieve user access token")

    # 3. Get User Info
    user_info_url = "https://open.feishu.cn/open-apis/authen/v1/user_info"
    user_info_headers = {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    user_info_res = requests.get(user_info_url, headers=user_info_headers)
    if user_info_res.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to get user info from Feishu")
    
    user_info_data = user_info_res.json()
    if user_info_data.get("code") != 0:
        raise HTTPException(status_code=400, detail=f"Feishu Error: {user_info_data.get('msg')}")
        
    feishu_user = user_info_data.get("data", {})
    open_id = feishu_user.get("open_id")
    
    if not open_id:
        raise HTTPException(status_code=400, detail="Incomplete user info from Feishu")

    # 4. Check/Create User in DB
    user = db.query(models.User).filter(models.User.feishu_open_id == open_id).first()
    
    if not user:
        user = models.User(
            feishu_open_id=open_id,
            name=feishu_user.get("name"),
            email=feishu_user.get("email") or feishu_user.get("enterprise_email"), # Try both
            avatar_url=feishu_user.get("avatar_url")
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    
    # 5. Issue Local JWT
    access_token_expires = timedelta(minutes=config.settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"sub": user.feishu_open_id}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer", "user": user}

@router.get("/me", response_model=schemas.User)
def read_users_me(token: str = Depends(lambda x: x), db: Session = Depends(database.get_db)):
    # Simple dependency to parse token
    try:
        payload = jwt.decode(token, config.settings.jwt_secret, algorithms=[config.settings.jwt_algorithm])
        feishu_open_id: str = payload.get("sub")
        if feishu_open_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
        
    user = db.query(models.User).filter(models.User.feishu_open_id == feishu_open_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user
