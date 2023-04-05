all: build dbuild dpush #kpush

build:
	builder --ldflags "-linkmode external -extldflags -static" --config builder.yaml

dbuild:
	docker build -t  hvacrbespin01.azurecr.io/custom-otel-ce:latest .

dpush:
	docker push hvacrbespin01.azurecr.io/custom-otel-ce:latest

kpush:
	kubectl delete -f pod.yaml || true
	kubectl apply -f pod.yaml

cleanall: cleank cleand

cleank:
	kubectl delete -f pod.yaml || true

cleand:
	for i in $$(docker images | grep custom | cut -f 24 -d ' '); do docker image rm $$i; done
