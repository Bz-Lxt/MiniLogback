FROM golang:1.25.12
WORKDIR /app
COPY . /app/
ENV GOTOOLCHAIN=local TZ=Asia/Shanghai
RUN go build ./...
