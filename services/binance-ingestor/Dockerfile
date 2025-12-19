FROM golang:1.21 as builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o binance-ingestor main.go

FROM gcr.io/distroless/base-debian12
COPY --from=builder /app/binance-ingestor /bin/binance-ingestor
EXPOSE 2112
ENTRYPOINT ["/bin/binance-ingestor"]
