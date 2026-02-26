FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Basic tools
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        git \
        curl \
        wget \
        unzip \
        gnupg \
        ca-certificates \
        lsb-release \
        python3 \
        python3-pip \
        python3-venv \
        jq \
        groff \
        less \
        pkg-config \
        build-essential && \
    rm -rf /var/lib/apt/lists/*

# Terraform (HashiCorp apt repo)
RUN curl -fsSL https://apt.releases.hashicorp.com/gpg | \
        gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
        https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
        > /etc/apt/sources.list.d/hashicorp.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y terraform && \
    rm -rf /var/lib/apt/lists/*

# uv (Python package / dependency manager)
# Install uv directly into /usr/local/bin so it's on PATH for all users
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL=/usr/local/bin/uv sh

# Confluent CLI
WORKDIR /tmp
RUN curl -sL https://cnfl.io/cli | sh -s -- latest && \
    mv bin/confluent /usr/local/bin/confluent && \
    rmdir bin

# AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip

# Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash || true

WORKDIR /workspaces
