FROM node:20-alpine
WORKDIR /app
RUN npm install -g pnpm
COPY pnpm-workspace.yaml package.json ./
COPY codepulse-sdk ./codepulse-sdk
COPY demo ./demo

RUN pnpm install --filter codepulse-sdk --filter demo
RUN pnpm run --filter codepulse-sdk build

WORKDIR /app/demo
EXPOSE 4000
CMD ["node", "index.js"]
