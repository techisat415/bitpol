FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /engine ./cmd/api

FROM gcr.io/distroless/static-debian12
COPY --from=build /engine /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
