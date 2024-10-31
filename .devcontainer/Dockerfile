# Start from the official Python 3.11 slim image
FROM python:3.11-slim

# Install OpenJDK 17, Git, and procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jdk-headless \
        git \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Create a non-root user and group
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

# Set the working directory
WORKDIR /workspace/flight-statistic

# Change ownership of the workspace directory
RUN chown -R $USERNAME:$USERNAME /workspace/flight-statistic

# Switch to the non-root user
USER $USERNAME

RUN pip install --upgrade pip