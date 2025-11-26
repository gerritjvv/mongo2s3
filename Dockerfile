FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o mongo2s3 ./cmd/main.go

FROM alpine:3.18

WORKDIR /app

COPY --from=builder /app/mongo2s3 .

COPY ./testconfig.yaml .

ENTRYPOINT ["./mongo2s3"]
