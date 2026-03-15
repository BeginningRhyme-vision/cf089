#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

export BACKEND_API_URL="${BACKEND_API_URL:-http://209.222.101.19:8080/api}"
export REDIS_URL="${REDIS_URL:-redis://:redis_password@209.222.101.19:6379/0}"
export MAX_THREADS="${MAX_THREADS:-24}"
export FFMPEG_HOST_TEMP_DIR="${FFMPEG_HOST_TEMP_DIR:-/data/ffmpeg-tmp}"
export FFMPEG_TEMP_DIR="${FFMPEG_TEMP_DIR:-/data/ffmpeg-tmp}"
export FFMPEG_SHM_SIZE="${FFMPEG_SHM_SIZE:-8gb}"

mkdir -p "$FFMPEG_HOST_TEMP_DIR"

docker compose -p ffmpeg-volc -f docker-compose-ffmpeg.volc.yml pull
docker compose -p ffmpeg-volc -f docker-compose-ffmpeg.volc.yml up -d --force-recreate
docker compose -p ffmpeg-volc -f docker-compose-ffmpeg.volc.yml ps
