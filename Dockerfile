FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        nodejs \
        npm \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @openai/codex opencode-ai

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -e .

CMD ["telecode", "--mode", "polling", "--engine", "codex", "--projects-file", "/config/projects.json", "--state-file", "/data/telecode-state.json", "-v"]
