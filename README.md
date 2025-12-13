# Unbound Future Admin

A business management dashboard with Feishu integration and Data Transfer Task management.

## Project Structure

- `backend/`: FastAPI application (Python).
- `frontend/`: React + Vite application (JavaScript/React).
- `config.yaml`: Centralized configuration for the project.

## Prerequisites

- Python 3.9+
- Node.js 18+
- PostgreSQL 12+

## Setup & Configuration

1.  **Database**:
    - Ensure your PostgreSQL server is running.
    - Create a database named `unbound_future_db`.
    - Update `config.yaml` with your database credentials:
        ```yaml
        database:
          url: "postgresql://user:password@localhost:5432/unbound_future_db"
        ```

2.  **Feishu (Lark) Integration**:
    - Update `config.yaml` with your Feishu App ID and Secret.
    - If you don't have one, the system uses a mock login flow if the App ID is left as "YOUR_APP_ID".

## Running the Application

### Backend

1.  Navigate to the `backend` directory:
    ```bash
    cd backend
    ```
2.  Create a virtual environment and install dependencies (already done if you followed the agent's setup):
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
3.  Run the server:
    ```bash
    uvicorn app.main:app --reload --port 8000
    ```
    The API will be available at `http://localhost:8000`.

### Frontend

1.  Navigate to the `frontend` directory:
    ```bash
    cd frontend
    ```
2.  Install dependencies (already done if you followed the agent's setup):
    ```bash
    npm install
    ```
3.  Run the development server:
    ```bash
    npm run dev
    ```
    The UI will be available at `http://localhost:5173`.

## Features

- **Authentication**: Feishu OAuth2 (Mocked if no credentials provided).
- **Metadata Management**: CRUD for client connection details.
- **Job Management**: Create, Monitor, and Control transfer tasks.
- **Mock Transfer**: Starting a job triggers a background simulation of a file transfer.

## Development Notes

- The backend reads `config.yaml` from the project root.
- The frontend connects to `http://localhost:8000` by default.
