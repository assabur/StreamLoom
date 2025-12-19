FROM golang:1.21 as builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o clickhouse-sink main.go

FROM gcr.io/distroless/base-debian12
COPY --from=builder /app/clickhouse-sink /bin/clickhouse-sink
EXPOSE 2112
ENTRYPOINT ["/bin/clickhouse-sink"]
