FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY dashboard.html ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /traffic-analyzer .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /traffic-analyzer /traffic-analyzer
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/traffic-analyzer"]
