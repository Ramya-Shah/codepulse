FROM node:20-alpine
WORKDIR /app
RUN npm install -g pnpm@10.33.0
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml ./
COPY codepulse-sdk ./codepulse-sdk
COPY demo ./demo

RUN pnpm install --frozen-lockfile --filter codepulse-sdk --filter demo
RUN pnpm run --filter codepulse-sdk build

WORKDIR /app/demo
CMD ["node", "index.js"]
