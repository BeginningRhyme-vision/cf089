# Registry Config
REGISTRY := image.unboundfuture.ai
TAG := latest

# Image Names
IMAGE_BACKEND := $(REGISTRY)/unbound-backend:$(TAG)
IMAGE_FRONTEND := $(REGISTRY)/unbound-frontend:$(TAG)
IMAGE_DOWNLOADER := $(REGISTRY)/unbound-worker-downloader:$(TAG)
IMAGE_TRANSFER := $(REGISTRY)/unbound-worker-transfer:$(TAG)

.PHONY: all build push build-backend push-backend build-frontend push-frontend build-downloader push-downloader build-transfer push-transfer

all: build push

# --- Build ---

build: build-backend build-frontend build-downloader build-transfer

build-backend:
	docker build -t $(IMAGE_BACKEND) backend/go-app

build-frontend:
	docker build -t $(IMAGE_FRONTEND) frontend

build-downloader:
	docker build -t $(IMAGE_DOWNLOADER) backend/worker_downloader

build-transfer:
	docker build -t $(IMAGE_TRANSFER) backend/worker_transfer

# --- Push ---

push: push-backend push-frontend push-downloader push-transfer

push-backend:
	docker push $(IMAGE_BACKEND)

push-frontend:
	docker push $(IMAGE_FRONTEND)

push-downloader:
	docker push $(IMAGE_DOWNLOADER)

push-transfer:
	docker push $(IMAGE_TRANSFER)

# --- Deploy (Optional K8s helpers) ---

apply-k8s:
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/backend-api.yaml
	kubectl apply -f k8s/frontend.yaml
	kubectl apply -f k8s/worker-downloader-go.yaml
	kubectl apply -f k8s/worker-downloader-py.yaml
	kubectl apply -f k8s/worker-transfer-scanner.yaml
	kubectl apply -f k8s/worker-transfer-agent.yaml

restart-k8s:
	kubectl rollout restart deployment/backend-api
	kubectl rollout restart deployment/frontend
	kubectl rollout restart deployment/worker-downloader-go
	kubectl rollout restart deployment/worker-downloader-py
	kubectl rollout restart deployment/worker-transfer-scanner
	kubectl rollout restart deployment/worker-transfer-agent