# Stage 1: Get Docker CLI from official image
FROM docker:27-cli AS docker-cli

# Stage 2: Build the fetcher image
FROM python:3.12-slim

# Install bash tools needed by run-all.sh
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash jq gawk && \
    rm -rf /var/lib/apt/lists/*

# Copy Docker CLI from official image (avoids docker.io which only provides the daemon)
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker

WORKDIR /app/scripts

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts into the image
COPY t212_fetch.py .
COPY run-all.sh .
RUN chmod +x run-all.sh

ENTRYPOINT ["python3", "t212_fetch.py"]
