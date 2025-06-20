build-batch:
	docker buildx build -t hub.hmf.xyz/base/data-generator:1.0.0 -f docker/Dockerfile . --platform linux/amd64
	docker push hub.hmf.xyz/base/data-generator:1.0.0

build-realtime:
	docker buildx build -t hub.hmf.xyz/base/data-generator-realtime:1.0.0 -f docker/Dockerfile_realtime . --platform linux/amd64
	docker push hub.hmf.xyz/base/data-generator-realtime:1.0.0