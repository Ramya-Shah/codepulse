#!/bin/sh
# One-time setup for a fresh Debian 12 VM: Docker, a swap file, and the repo.
#   curl -fsSL https://raw.githubusercontent.com/Ramya-Shah/codepulse/main/deploy/setup-vm.sh | sudo sh
set -eu

# 2 GB swap so image builds and Postgres fit in 1 GB of RAM.
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

if ! command -v docker >/dev/null; then
  apt-get update
  apt-get install -y ca-certificates curl git
  curl -fsSL https://get.docker.com | sh
fi

if [ ! -d /opt/codepulse ]; then
  git clone https://github.com/Ramya-Shah/codepulse.git /opt/codepulse
fi

echo "Done. Next: create /opt/codepulse/.env from deploy/.env.prod.example, then:"
echo "  cd /opt/codepulse && docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build"
