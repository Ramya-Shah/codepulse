FROM node:20-alpine
WORKDIR /app
RUN npm install -g pnpm@10.33.0
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml ./
COPY codepulse-sdk ./codepulse-sdk
COPY codepulse-ingest ./codepulse-ingest

RUN pnpm install --frozen-lockfile --filter codepulse-sdk --filter codepulse-ingest
RUN pnpm run --filter codepulse-sdk build
RUN pnpm run --filter codepulse-ingest build

EXPOSE 3000
CMD ["node", "codepulse-ingest/dist/index.js"]
