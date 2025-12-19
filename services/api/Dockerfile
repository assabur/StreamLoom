FROM golang:1.21 as builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o api main.go

FROM gcr.io/distroless/base-debian12
COPY --from=builder /app/api /bin/api
EXPOSE 8088 2112
ENTRYPOINT ["/bin/api"]
