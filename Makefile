build:
	docker buildx build -t hub.hmf.xyz/base/data-generator:1.0.0 -f docker/Dockerfile . --platform linux/amd64 -f docker/Dockerfile
	docker push hub.hmf.xyz/base/data-generator:1.0.0