FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/forgequeue ./cmd/api
FROM alpine:3.20
RUN adduser -D -H app
USER app
COPY --from=build /out/forgequeue /forgequeue
EXPOSE 8080
ENTRYPOINT ["/forgequeue"]
