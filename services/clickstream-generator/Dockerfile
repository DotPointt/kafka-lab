FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/clickstream-generator .

FROM alpine:3.22
COPY --from=build /out/clickstream-generator /usr/local/bin/clickstream-generator
EXPOSE 8080
ENTRYPOINT ["clickstream-generator"]
