# Deploying the backend on a GCP e2-micro

The dashboard is on Vercel. The backend (Postgres, ingest API, demo traffic app,
and Caddy for HTTPS) runs with Docker Compose on one always-on VM.

## Cost

- **e2-micro VM:** free under the Always Free tier when it runs in `us-west1`,
  `us-central1` or `us-east1`, with up to 30 GB of standard disk.
- **External IPv4 address:** Google lists in-use external IPs at about
  $0.005/hour (~$3.65/month). It's unclear whether the free tier covers one,
  so budget **$0–3.65/month**.
- **Egress:** 1 GB/month free from North America. One dashboard viewer uses
  about 2 MB per hour (a WebSocket message per second plus a stats poll every 5 s).
- **Gemini:** the demo analysis is cached for 30 days, so about $0.
- A billing account is required even for free-tier usage.

## Resource budget (1 GB RAM + 2 GB swap)

| Service | Memory limit |
|---|---|
| Postgres (64 MB shared buffers, 20 connections) | 320 MB |
| Ingest API | 256 MB |
| Demo traffic app (10 req/s) | 128 MB |
| Caddy | 64 MB |

## Steps

1. **Domain:** sign in at https://www.duckdns.org, create a subdomain, and point it
   at the VM's external IP.
2. **VM:** e2-micro, Debian 12, 30 GB standard disk, in a free-tier region, with
   HTTP and HTTPS allowed.
3. **Set up the VM:** SSH in and run
   `curl -fsSL https://raw.githubusercontent.com/Ramya-Shah/codepulse/main/deploy/setup-vm.sh | sudo sh`
4. **Configure:** `sudo cp /opt/codepulse/deploy/.env.prod.example /opt/codepulse/.env`
   and fill it in.
5. **Start:** `cd /opt/codepulse && sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build`
6. **Check:** `curl https://<DOMAIN>/health` should return `"ok":true` and, after
   about a minute, `"demo_traffic":true`.
7. **Point the dashboard at it:** in Vercel, set `VITE_API_URL=https://<DOMAIN>`
   and redeploy.

## Updating

```bash
cd /opt/codepulse && sudo git pull && sudo docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```
