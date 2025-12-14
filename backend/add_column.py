from sqlalchemy import create_engine, text
from app.config import settings

def add_column():
    engine = create_engine(settings.db_url)
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE transfer_jobs ADD COLUMN is_incremental BOOLEAN DEFAULT FALSE"))
            conn.commit()
            print("Column added successfully.")
        except Exception as e:
            print(f"Error (maybe column exists?): {e}")

if __name__ == "__main__":
    add_column()
