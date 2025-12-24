# Unbound Future Admin

A business management dashboard with Feishu integration, Data Transfer Task management, and Youtube Job processing.

## Project Structure

- `backend/go-app/`: API Server (Go/Gin).
- `backend/worker_downloader/`: Youtube Job Workers (Python for Metadata, Go for Download).
- `backend/worker_transfer/`: Transfer Job Workers (Go for Scanner and Transfer).
- `frontend/`: React + Vite application (JavaScript/React).
- `config.yaml`: Centralized configuration for the project.

## Workflow & Logic Chain (中文)

### 1. Youtube Jobs (Video Processing)

Youtube 任务处理分为两个阶段，由不同的 Worker 协同完成：

**阶段一：元数据获取 (Python)**
*   **代码位置**: `backend/worker_downloader/get_yt_metadata.py`
*   **流程**:
    1.  **获取任务**: Worker 向后端 `POST /api/tasks/acquire` 请求任务，指定 `stage="metadata"`。
    2.  **处理**: 使用 `yt-dlp` 解析 Youtube URL，提取视频标题、Video ID 以及最佳音频/视频流的下载链接。
    3.  **更新**: 将提取到的元数据 (Title, VideoID, AudioURL, VideoURL) 和状态 `METADATA_FETCHED` 回传给后端 (`POST /api/tasks/update`)。
    4.  **状态流转**: 后端收到 `METADATA_FETCHED` 状态更新后，会自动将该任务推送到 Redis 队列 `queue:youtube:download_ready`，供下一阶段消费。

**阶段二：视频下载 (Go)**
*   **代码位置**: `backend/worker_downloader/downloader.go`
*   **流程**:
    1.  **获取任务**: Worker 向后端 `POST /api/tasks/acquire` 请求任务，指定 `stage="download"`。后端从 `queue:youtube:download_ready` 队列中弹出任务。
    2.  **准备上传**: 根据任务所属 Job 获取 R2 存储前缀，通过 AWS SDK 初始化 R2 分片上传 (Multipart Upload)。
    3.  **数据传输**: 调用外部下载服务 (`download_service_url`)，该服务负责流式传输数据（边下边传）。Worker 负责协调分片和重试。
    4.  **完成**: 所有分片上传完成后，调用 S3 CompleteMultipartUpload，并通知后端任务状态为 `COMPLETED`。

### 2. Transfer Jobs (Data Migration)

数据迁移任务同样分为扫描和传输两个阶段，均由 Go 编写的 Worker 执行：

**阶段一：源数据扫描 (Go Scanner)**
*   **代码位置**: `backend/worker_transfer/scanner.go`
*   **流程**:
    1.  **发现任务**: 轮询后端 `GET /api/jobs/pending` 获取状态为 `PENDING` 的迁移作业。
    2.  **锁定状态**: 将作业状态更新为 `RUNNING`。
    3.  **扫描源站**: 使用 AWS SDK 遍历源 S3/R2 Bucket 中的对象。
    4.  **批量入库**: 将扫描到的对象 Key 分批发送给后端 (`POST /api/jobs/:id/tasks`)。后端负责去重并创建 `PENDING` 状态的 Transfer Tasks，这些任务会自动进入队列供传输 Worker 消费。
    5.  **完成**: 扫描结束后，若作业配置了 `periodic_interval` (周期性轮询)，Scanner 会保持作业状态为 `RUNNING` 并更新 `last_scan_time`，等待下一次扫描；若未配置周期性任务且无新任务，则标记作业为 `COMPLETED`。

**阶段二：数据传输 (Go Transfer)**
*   **代码位置**: `backend/worker_transfer/transfer.go`
*   **流程**:
    1.  **获取任务**: Worker 向后端 `POST /api/transfer-tasks/acquire` 请求 `PENDING` 的传输任务。
    2.  **获取上下文**: 获取作业的元数据（包括目标端 Endpoint 和凭证）。
    3.  **生成签名**: 为源对象生成预签名下载链接 (Presigned GET)，为目标对象生成预签名上传链接 (Presigned PUT/UploadPart)。
    4.  **执行传输**: 调用外部传输服务 (`transfer_service_url`)，将源链接和目标链接传递给它，由其执行数据 Copy。支持大文件分片传输。
    5.  **更新状态**: 传输成功或失败后，向后端汇报任务状态 (`POST /api/transfer-tasks/update`)。

---

## Prerequisites

- Go 1.25+
- Node.js 18+
- PostgreSQL 12+
- Redis 7+

## Setup & Configuration

1.  **Database**:
    - Ensure your PostgreSQL server is running.
    - Create a database named `unbound_future_db`.
    - Update `config.yaml` with your database credentials:
        ```yaml
        database:
          url: "postgres://user:password@localhost:5432/unbound_future_db?sslmode=disable"
        ```

2.  **Redis**:
    - Ensure your Redis server is running.
    - Update `config.yaml` with your Redis URL:
        ```yaml
        redis:
          url: "redis://localhost:6379/0"
        ```

3.  **Feishu (Lark) Integration**:
    - Update `config.yaml` with your Feishu App ID and Secret.
    - If you don't have one, the system uses a mock login flow if the App ID is left as "YOUR_APP_ID".

## Running the Application

### Backend (Go)

1.  Navigate to the `backend/go-app` directory:
    ```bash
    cd backend/go-app
    ```
2.  Install dependencies:
    ```bash
    go mod download
    ```
3.  Run the server:
    ```bash
    go run main.go
    # OR build and run
    # go build -o main . && ./main
    ```
    The API will be available at `http://localhost:8080`.

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
- **Transfer Jobs**: Manage data transfer tasks with deduplication and status tracking.
- **Youtube Jobs**: High-throughput video processing job management backed by Redis.
- **Task Batching**: Efficient batch APIs for inserting, updating, and fetching tasks.

## Development Notes

- The backend reads `config.yaml` from the project root (or parent directories).
- The backend server runs on port `8080`.
- Redis is required for job queuing and task buffering.
