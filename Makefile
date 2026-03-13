# Registry Config
REGISTRY := 209.222.101.19:5000
TAG := latest

# Image Names
IMAGE_BACKEND := $(REGISTRY)/unbound-backend:$(TAG)
IMAGE_FRONTEND := $(REGISTRY)/unbound-frontend:$(TAG)
IMAGE_DOWNLOADER := $(REGISTRY)/unbound-worker-downloader:$(TAG)
IMAGE_TRANSFER := $(REGISTRY)/unbound-worker-transfer:$(TAG)
IMAGE_FFMPEG := $(REGISTRY)/unbound-worker-ffmpeg:$(TAG)
IMAGE_PROXY_TESTER := $(REGISTRY)/unbound-proxy-tester:$(TAG)

.PHONY: all build push build-backend push-backend build-frontend push-frontend build-downloader push-downloader build-transfer push-transfer build-ffmpeg push-ffmpeg build-proxy-tester push-proxy-tester

all: build push

# --- Build ---

build: build-backend build-frontend build-downloader build-transfer build-ffmpeg build-proxy-tester

build-backend:
	docker build -t $(IMAGE_BACKEND) backend/go-app

build-frontend:
	docker build -t $(IMAGE_FRONTEND) frontend

build-downloader:
	docker build -t $(IMAGE_DOWNLOADER) -f backend/worker_downloader/Dockerfile backend

build-transfer:
	docker build -t $(IMAGE_TRANSFER) backend/worker_transfer

build-ffmpeg:
	docker build -t $(IMAGE_FFMPEG) backend/worker_ffmpeg

build-proxy-tester:
	docker build -t $(IMAGE_PROXY_TESTER) -f backend/proxy_tester/Dockerfile .

# --- Push ---

push: push-backend push-frontend push-downloader push-transfer push-ffmpeg push-proxy-tester

push-backend:
	docker push $(IMAGE_BACKEND)

push-frontend:
	docker push $(IMAGE_FRONTEND)

push-downloader:
	docker push $(IMAGE_DOWNLOADER)

push-transfer:
	docker push $(IMAGE_TRANSFER)

push-ffmpeg:
	docker push $(IMAGE_FFMPEG)

push-proxy-tester:
	docker push $(IMAGE_PROXY_TESTER)

# --- Deploy (Optional K8s helpers) ---

apply-k8s:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/registry-secret.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/backend-api.yaml
	kubectl apply -f k8s/frontend.yaml
	kubectl apply -f k8s/worker-downloader-go.yaml
	kubectl apply -f k8s/worker-downloader-py.yaml
	kubectl apply -f k8s/worker-transfer-scanner.yaml
	kubectl apply -f k8s/worker-transfer-agent.yaml

restart-master:
	kubectl rollout restart deployment/backend-api -n unbound-future
	kubectl rollout restart deployment/frontend -n unbound-future

restart-worker:
	kubectl rollout restart deployment/backend-api -n unbound-future
	kubectl rollout restart deployment/frontend -n unbound-future
	kubectl rollout restart deployment/worker-downloader-go -n unbound-future
	kubectl rollout restart deployment/worker-downloader-py -n unbound-future
	kubectl rollout restart deployment/worker-transfer-scanner -n unbound-future
	kubectl rollout restart deployment/worker-transfer-agent -n unbound-future