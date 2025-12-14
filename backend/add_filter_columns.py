from sqlalchemy import create_engine, text
from app.config import settings

def add_columns():
    engine = create_engine(settings.db_url)
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE transfer_jobs ADD COLUMN include VARCHAR(1024)"))
            conn.execute(text("ALTER TABLE transfer_jobs ADD COLUMN exclude VARCHAR(1024)"))
            conn.commit()
            print("Columns added successfully.")
        except Exception as e:
            print(f"Error (maybe columns exist?): {e}")

if __name__ == "__main__":
    add_columns()
