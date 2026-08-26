FROM node:22-alpine3.22 AS frontend-builder
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --registry=https://registry.npmmirror.com
COPY frontend/ ./
RUN npm run build

FROM golang:1.25-alpine3.22 AS go-builder
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod ./
RUN go mod download
COPY . .
COPY --from=frontend-builder /src/frontend/dist ./frontend/dist
RUN go test ./... && go build -trimpath -ldflags="-s -w" -o /out/minilogbackd ./cmd/minilogbackd && go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen && go build -trimpath -ldflags="-s -w" -o /out/perfcheck ./cmd/perfcheck

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates wget \
    && addgroup -S -g 10001 minilogback \
    && adduser -S -D -H -u 10001 -G minilogback minilogback \
    && mkdir -p /app/frontend /data \
    && chown -R minilogback:minilogback /app /data
WORKDIR /app
COPY --from=go-builder --chown=minilogback:minilogback /out/minilogbackd /usr/local/bin/minilogbackd
COPY --from=go-builder --chown=minilogback:minilogback /out/loadgen /usr/local/bin/loadgen
COPY --from=go-builder --chown=minilogback:minilogback /out/perfcheck /usr/local/bin/perfcheck
COPY --from=go-builder --chown=minilogback:minilogback /src/frontend/dist ./frontend/dist
USER minilogback
EXPOSE 8080 9010
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=6 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/minilogbackd"]
