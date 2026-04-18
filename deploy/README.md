# Deploying observability stack to a remote host (OTel Collector, Prometheus, Grafana, Loki, Jaeger)

This README describes how to copy the `deploy/` stack to another machine on the same local network, start it with Docker Compose, open required firewall ports, and point your local `relay` app to the remote OpenTelemetry Collector.

Replace `REMOTE_HOST` (e.g. `192.168.1.50`) and `USER` with real values in the examples.

---

## What this stack provides (as configured)
- OpenTelemetry Collector (OTLP gRPC: 4317, OTLP HTTP: 4318)
- Collector Prometheus exporter (8889)
- Prometheus UI (9090)
- Grafana UI (3000)
- Loki (3100)
- Jaeger UI (16686)

These services are already configured in `deploy/docker-compose.yaml` and `deploy/otel-collector-config.yaml` to run together in a single Docker Compose network.

## Important: Env var name used by the app
The application binary was updated to read `OTEL_ENDPOINT` (host:port, e.g. `192.168.1.50:4317`) to send OTLP gRPC to the collector. Your existing `.env` file uses `OTEL_EXPORTER_ENDPOINT`; you can either set `OTEL_ENDPOINT` in the runtime environment, or change the key in the `.env` you load to `OTEL_ENDPOINT`.

Examples in this README use `OTEL_ENDPOINT`.

---

## 1) Copy the `deploy/` folder to the remote host

Option A - from your Windows machine using scp (OpenSSH must be available):

```bash
# from Windows cmd (PowerShell users can use the same scp command)
scp -r "D:\Backend Projects\relay\deploy" USER@REMOTE_HOST:~/relay-deploy
```

Option B - zip and transfer (if scp not available):

```powershell
# on Windows: zip folder then transfer via SMB, USB, or SFTP
Compress-Archive -Path "D:\Backend Projects\relay\deploy" -DestinationPath C:\temp\relay-deploy.zip
# then copy the zip to the remote host and unzip there
```

Option C - push to a git repo and `git clone` on remote host (recommended for repeated deployments).

---

## 2) Start the observe stack on the remote host

SSH into the remote host and run the compose stack.

```bash
# on the remote host (Linux example)
cd ~/relay-deploy
# If using Docker Compose v2 (recommended):
docker compose up -d
# Older installs may need:
# docker-compose up -d

# confirm services are running
docker compose ps
```

If you need to edit configuration (for example `deploy/prometheus.yaml`) do so and then restart only that service:

```bash
# edit files using your editor of choice, then:
docker compose restart prometheus
```

---

## 3) Open firewall ports on the remote host

These ports must be reachable from the machine(s) running the `relay` app and from your browser for UIs.

- 4317/tcp – OTLP gRPC (accepts traces/metrics/logs)
- 4318/tcp – OTLP HTTP (optional)
- 8889/tcp – Collector Prometheus exporter
- 9090/tcp – Prometheus UI
- 3000/tcp – Grafana UI
- 3100/tcp – Loki
- 16686/tcp – Jaeger UI

Ubuntu (ufw) example:

```bash
sudo ufw allow 4317/tcp
sudo ufw allow 4318/tcp
sudo ufw allow 8889/tcp
sudo ufw allow 9090/tcp
sudo ufw allow 3000/tcp
sudo ufw allow 3100/tcp
sudo ufw allow 16686/tcp
```

Windows Server example (run in Administrator cmd):

```cmd
netsh advfirewall firewall add rule name="OTEL 4317" dir=in action=allow protocol=TCP localport=4317
netsh advfirewall firewall add rule name="OTEL 4318" dir=in action=allow protocol=TCP localport=4318
netsh advfirewall firewall add rule name="Collector metrics" dir=in action=allow protocol=TCP localport=8889
netsh advfirewall firewall add rule name="Prometheus" dir=in action=allow protocol=TCP localport=9090
netsh advfirewall firewall add rule name="Grafana" dir=in action=allow protocol=TCP localport=3000
netsh advfirewall firewall add rule name="Loki" dir=in action=allow protocol=TCP localport=3100
netsh advfirewall firewall add rule name="Jaeger" dir=in action=allow protocol=TCP localport=16686
```

Note: For security, only open the ports on your private network or place the remote host behind a VPN. The default stack does not enable TLS or auth.

---

## 4) Point your local app at the remote collector

The app reads `OTEL_ENDPOINT` (format: host:port). Set it to the remote host IP and start the app.

Local Windows (cmd.exe) example — build and run:

```cmd
cd /d "D:\Backend Projects\relay"
REM build the binary
go build -o relay.exe ./cmd
REM run using the remote collector IP
set OTEL_ENDPOINT=REMOTE_HOST:4317 && relay.exe
```

PowerShell example:

```powershell
cd "D:\Backend Projects\relay"
go build -o relay.exe ./cmd
$env:OTEL_ENDPOINT = "REMOTE_HOST:4317"
.\relay.exe
```

If you run the app in Docker, pass the env var:

```bash
docker run -e OTEL_ENDPOINT=REMOTE_HOST:4317 my-relay-image
```

If your app currently reads environment variables from a `.env` file, update that file (on the machine where the app runs) to set `OTEL_ENDPOINT=REMOTE_HOST:4317`. Note: the repo's `.env` currently uses `OTEL_EXPORTER_ENDPOINT`; the app will only honor `OTEL_ENDPOINT` unless you edit the config loader.

---

## 5) Prometheus considerations

- If you run Prometheus inside the same `docker compose` stack on the remote host (the default), Prometheus scrapes the collector using the compose service name `otel-collector:8889` and you do not need to change `deploy/prometheus.yaml`.

- If you run Prometheus separately (on a different machine), update `deploy/prometheus.yaml` to target the remote host IP instead of `otel-collector`:

```yaml
- job_name: 'otel-collector'
  static_configs:
    - targets: ['REMOTE_HOST:8889']
```

Then restart Prometheus to pick up the new config.

---

## 6) Quick verification

From any machine on the LAN (or on the remote host itself):

- Verify collector metrics endpoint (Prometheus exporter):

```bash
curl http://REMOTE_HOST:8889/metrics
```

- Check collector logs (remote host):

```bash
cd ~/relay-deploy
docker compose logs -f otel-collector
```

- Check Prometheus target list from Prometheus UI: http://REMOTE_HOST:9090/targets

- Open Jaeger UI (traces): http://REMOTE_HOST:16686

- Open Grafana (dashboards): http://REMOTE_HOST:3000

- Check your app is sending telemetry by viewing traces in Jaeger or metrics in Prometheus/Grafana and logs in Loki.

---

## 7) Troubleshooting

- "Connection refused" to `REMOTE_HOST:4317`:
  - Confirm `docker compose ps` shows `otel-collector` running and ports are bound.
  - Confirm firewall is open.
  - From the app host run `telnet REMOTE_HOST 4317` or `curl` (for HTTP endpoint 4318) to test connectivity.

- Prometheus shows `DOWN` for the collector metrics target:
  - Confirm Prometheus can resolve and reach `REMOTE_HOST:8889` (or leave as `otel-collector:8889` if Prometheus runs inside the same compose network).

- No traces in Jaeger:
  - Confirm the collector logs and the app logs show no exporter errors.
  - Check your app's `OTEL_ENDPOINT` is correctly set and not prefixed with `http://` (the app expects `host:port` for gRPC).

- Env variable mismatch:
  - If you currently rely on `.env` and a loader to set env vars, update it to include `OTEL_ENDPOINT` or set export in the shell/service manager used to run the binary.

---

## 8) Example: Full quick-start (assumes Linux remote)

1. Copy `deploy/` to remote host `192.168.1.50`:

```bash
scp -r "D:/Backend Projects/relay/deploy" user@192.168.1.50:~/relay-deploy
```

2. SSH and start the stack:

```bash
ssh user@192.168.1.50
cd ~/relay-deploy
docker compose up -d
```

3. On your local dev machine run the app pointing to the remote collector:

```cmd
cd /d "D:\Backend Projects\relay"
set OTEL_ENDPOINT=192.168.1.50:4317 && go run ./cmd
```

4. Verify metrics endpoint:

```bash
curl http://192.168.1.50:8889/metrics
```

---

## 9) Security & hardening notes
- Do not expose these ports to the public internet.
- For production, enable TLS and authentication for the collector endpoints and/or place the stack behind a VPN.
- Lock down Grafana and Jaeger with authentication and network rules.

---

If you want, I can also:
- Edit `deploy/prometheus.yaml` to use an explicit `REMOTE_HOST` instead of `otel-collector`.
- Add a systemd service file example to run your `relay` binary and set `OTEL_ENDPOINT` at boot.
- Add a small `deploy/compose.env` file template to centralize the HOST/PORT variables.

If you'd like those, tell me which one and I will add it next.

