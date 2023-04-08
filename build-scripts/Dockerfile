FROM scratch

WORKDIR /
COPY	./hv-otel/hv-otelcol /otel

# Can be removed if building on some workflow
COPY	./configs/config.yaml /config.yaml	

EXPOSE	4317
#CMD	["/otel", "--config", "/config.yaml"]
