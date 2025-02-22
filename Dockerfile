# Step 1: Use a Go image with Alpine for the build
FROM golang:1.23-alpine as builder

# Step 2: Install build tools and the GEOS C library
RUN apk add --no-cache build-base geos geos-dev git

# Step 3: Set the working directory
WORKDIR /app

# Step 4: Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Step 5: Copy the rest of the application code
COPY . .

# Step 6: Build the application
RUN go build -o main .

# Step 7: Use a minimal Alpine image for the runtime
FROM alpine:latest

# Step 8: Install the runtime dependency for GEOS
RUN apk add --no-cache geos

# Step 9: Set up a user for running the app (optional for security)
RUN adduser -D appuser

# Step 10: Copy the built binary from the builder stage
COPY --from=builder /app/main /app/main

# Step 11: Change ownership and permissions
RUN chown appuser:appuser /app/main

# Step 12: Switch to the non-root user
USER appuser

# Step 13: Expose the port the app runs on
EXPOSE 8080

# Step 14: Specify the command to run the app
CMD ["/app/main"]
