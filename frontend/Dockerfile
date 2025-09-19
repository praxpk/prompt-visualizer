FROM node:22-alpine

WORKDIR /app

COPY frontend/package*.json ./
RUN npm ci || npm i

COPY frontend/ .

EXPOSE 3000
ENV PORT=3000 \
    NEXT_TELEMETRY_DISABLED=1

# Build if possible; fall back to dev
RUN npm run build || echo "Skipping build; will use dev server"

CMD ["npm", "run", "dev"]

