# Unbound Future Admin

A business management dashboard with Feishu integration, Data Transfer Task management, Youtube Job processing, and FFmpeg video merging.

## Project Structure

- `backend/go-app/`: API Server (Go/Gin).
- `backend/worker_downloader/`: Youtube Job Workers (Python for Metadata, Go for Download).
- `backend/worker_transfer/`: Transfer Job Workers (Go for Scanner and Transfer).
- `backend/worker_ffmpeg/`: FFmpeg Job Workers (Go for Scanner and Worker).
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
    4.  **批量入库**: 将扫描到的对象 Key 分批发送给后端 (`POST /api/jobs/:id/tasks`)。后端负责去重并创建 `PENDING` 状态的 Transfer Tasks，这些任务会自动进入 Redis 队列供传输 Worker 消费。
    5.  **完成**: 扫描结束后，若作业配置了 `periodic_interval` (周期性轮询)，Scanner 会保持作业状态为 `RUNNING` 并更新 `last_scan_time`，等待下一次扫描；若未配置周期性任务且无新任务，则标记作业为 `COMPLETED`。

**阶段二：数据传输 (Go Transfer)**
*   **代码位置**: `backend/worker_transfer/transfer.go`
*   **流程**:
    1.  **获取任务**: Worker 向后端 `POST /api/transfer-tasks/acquire` 请求 `PENDING` 的传输任务。
    2.  **获取上下文**: 获取作业的元数据（包括目标端 Endpoint 和凭证）。
    3.  **生成签名**: 为源对象生成预签名下载链接 (Presigned GET)，为目标对象生成预签名上传链接 (Presigned PUT/UploadPart)。
    4.  **执行传输**: 调用外部传输服务 (`transfer_service_url`)，将源链接和目标链接传递给它，由其执行数据 Copy。支持大文件分片传输。
    5.  **更新状态**: 传输成功或失败后，向后端汇报任务状态 (`POST /api/transfer-tasks/update`)。

### 3. FFmpeg Jobs (Video Merging)

FFmpeg 任务用于将 S3/R2 上分离的视频流 (`*_video.mp4`) 和音频流 (`*_audio.m4a`) 合并为完整视频。

**阶段一：配对扫描 (Go Scanner)**
*   **代码位置**: `backend/worker_ffmpeg/cmd/scanner/main.go`
*   **流程**:
    1.  **发现任务**: 轮询后端 `GET /api/ffmpeg-jobs/pending` 获取待处理作业。
    2.  **扫描源站**: 遍历作业指定的 S3/R2 前缀。
    3.  **配对逻辑**: 在内存中匹配同名的 `_video` 和 `_audio` 文件。
    4.  **去重与分发**: 使用 Redis Set (`queue:ffmpeg:dedup:{JobId}`) 进行任务去重。对于新发现的配对任务，直接推送到 Redis 队列 `queue:ffmpeg:pending`。
    5.  **状态更新**: 定期调用 `PATCH /api/ffmpeg-jobs/:id/status` 更新作业的扫描进度和状态。

**阶段二：合并处理 (Go Worker)**
*   **代码位置**: `backend/worker_ffmpeg/cmd/worker/main.go`
*   **流程**:
    1.  **获取任务**: 直接从 Redis `queue:ffmpeg:pending` 阻塞弹出任务 (BLPop)。
    2.  **下载素材**: 将视频流和音频流下载到本地临时目录。
    3.  **执行合并**: 调用本地 `ffmpeg` 命令执行流复制合并 (`-c copy`)，无需重新编码，速度快。
    4.  **上传结果**: 将合并后的 MP4 文件上传回 S3/R2 (通常是同一 Bucket 的不同路径)。
    5.  **清理与汇报**: 删除本地临时文件，并调用后端接口 `PATCH /api/ffmpeg-jobs/:id/status` 更新作业的成功/失败计数。

---

## Prerequisites

- Go 1.25+
- Node.js 18+
- PostgreSQL 12+
- Redis 7+
- Python 3.9+ (for Metadata Worker)
- **Runtime Dependencies**:
    - `ffmpeg` (must be installed on the system/container running the FFmpeg Worker)
    - `yt-dlp` (must be installed on the system/container running the Metadata Worker)

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

3.  **Environment Variables**:
    - Workers require `BACKEND_API_URL` to be set (default: `http://localhost:8080/api`).
    - FFmpeg Worker respects `MAX_THREADS` (concurrency limit).

4.  **Feishu (Lark) Integration**:
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
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Run the development server:
    ```bash
    npm run dev
    ```
    The UI will be available at `http://localhost:5173`.

### Workers

Workers can be run independently or via Docker/Kubernetes.

- **Youtube Metadata (Python)**: `python3 backend/worker_downloader/get_yt_metadata.py`
- **Youtube Downloader (Go)**: `go run backend/worker_downloader/downloader.go`
- **Transfer Scanner (Go)**: `go run backend/worker_transfer/scanner.go`
- **Transfer Agent (Go)**: `go run backend/worker_transfer/transfer.go`
- **FFmpeg Scanner (Go)**: `go run backend/worker_ffmpeg/cmd/scanner/main.go`
- **FFmpeg Worker (Go)**: `go run backend/worker_ffmpeg/cmd/worker/main.go`

## Features

- **Authentication**: Feishu OAuth2 (Mocked if no credentials provided).
- **Metadata Management**: CRUD for client connection details.
- **Transfer Jobs**: Manage data transfer tasks with deduplication and status tracking.
- **Youtube Jobs**: High-throughput video processing job management backed by Redis.
- **FFmpeg Jobs**: Automated scanning and merging of audio/video streams from S3/R2.
- **Task Batching**: Efficient batch APIs for inserting, updating, and fetching tasks.
- **Metrics**: Prometheus metrics exposed by all components for monitoring.