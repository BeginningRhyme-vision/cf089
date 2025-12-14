import yaml
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "Unbound Future Admin"
    db_url: str
    feishu_app_id: str
    feishu_app_secret: str
    feishu_redirect_uri: str
    jwt_secret: str
    jwt_algorithm: str
    access_token_expire_minutes: int
    
    storage_transfer_service_url: str
    storage_src_endpoint: str
    storage_src_access_key: str
    storage_src_secret_key: str
    storage_src_region: str = "auto"
    max_worker_threads: int = 32

    class Config:
        env_file = ".env"

def load_config() -> Settings:
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config.yaml")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    return Settings(
        app_name=config_data["app"]["name"],
        db_url=config_data["database"]["url"],
        feishu_app_id=config_data["feishu"]["app_id"],
        feishu_app_secret=config_data["feishu"]["app_secret"],
        feishu_redirect_uri=config_data["feishu"]["redirect_uri"],
        jwt_secret=config_data["security"]["jwt_secret"],
        jwt_algorithm=config_data["security"]["jwt_algorithm"],
        access_token_expire_minutes=config_data["security"]["access_token_expire_minutes"],
        storage_transfer_service_url=config_data.get("storage", {}).get("transfer_service_url", "http://localhost:8787/initiate-copy"),
        storage_src_endpoint=config_data.get("storage", {}).get("src", {}).get("endpoint", ""),
        storage_src_access_key=config_data.get("storage", {}).get("src", {}).get("access_key", ""),
        storage_src_secret_key=config_data.get("storage", {}).get("src", {}).get("secret_key", ""),
        storage_src_region=config_data.get("storage", {}).get("src", {}).get("region", "auto"),
        max_worker_threads=config_data.get("worker", {}).get("max_threads", 32)
    )

settings = load_config()
