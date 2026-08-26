FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway
RUN mkdir -p /out/data && chown 65532:65532 /out/data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/gateway /gateway
COPY --from=build --chown=65532:65532 /out/data /data
WORKDIR /data
USER 65532:65532
ENV LLMGW_DATABASE_PATH=/data/llmgw.db
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/gateway"]
