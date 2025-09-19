FROM node:22-alpine

# Preinstall deps into a fixed path that won't be masked by bind mounts
WORKDIR /opt/frontend
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund || npm i --no-audit --no-fund

# Make preinstalled node_modules available to the app even if /app is bind-mounted
ENV NODE_PATH=/opt/frontend/node_modules
ENV PATH=/opt/frontend/node_modules/.bin:$PATH

# App workspace (will be bind-mounted by docker-compose)
WORKDIR /app
COPY frontend/ .

EXPOSE 3000
ENV PORT=3000 \
    NEXT_TELEMETRY_DISABLED=1

# Default command is overridden by docker-compose for dev; keep as a safe fallback
CMD ["sh", "-lc", "npm run dev -- -p 3000 -H 0.0.0.0"]
