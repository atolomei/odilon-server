# Odilon Monitoring: Prometheus and Grafana Setup

This guide explains how to configure Prometheus to collect metrics from an Odilon Server instance and how to import the official **Odilon Server** Grafana dashboard.

## 1. Prerequisites

You need:

- An Odilon Server instance running and exposing its Prometheus metrics endpoint.
- Prometheus.
- Grafana.
- The Odilon monitoring credentials configured in `odilon.properties`.

The examples below use:

- Odilon metrics endpoint: `http://localhost:9234/actuator/prometheus`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Metrics username: `monitor`

If Odilon is running on another machine, replace `localhost` with its hostname or IP address.

## 2. Configure Prometheus

Prometheus reads its configuration from a YAML file, usually named `prometheus.yml`.

Create or edit `prometheus.yml` with the following configuration:

```yaml
global:
  scrape_interval: 15s

scrape_configs:

  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "odilon"
    metrics_path: /actuator/prometheus
    basic_auth:
      username: monitor        # monitor.accessKey in odilon.properties
      password: CHANGE_ME      # monitor.secretKey in odilon.properties
    static_configs:
      - targets: ["localhost:9234"]
```

Replace `CHANGE_ME` with the value of `monitor.secretKey` configured in Odilon's `odilon.properties`. The username and password must match Odilon's `monitor.accessKey` and `monitor.secretKey` settings.

**Security:** Do not publish real credentials or commit them to a public repository. For production, protect the configuration file with restrictive file permissions or use Prometheus' `password_file` option to keep the password outside the YAML file.

### Monitoring more than one Odilon instance

You can add more targets under the same `odilon` job. For example:

```yaml
    static_configs:
      - targets: ["192.168.1.101:9234"]
        labels:
          instance_name: odilon-1
      - targets: ["192.168.1.102:9234"]
        labels:
          instance_name: odilon-2
```

All targets in this job use the same `basic_auth` credentials. If instances use different credentials, define separate scrape jobs with their own authentication settings.

Make sure the Prometheus host can reach each Odilon metrics endpoint over the network. Configure firewalls to allow access only from trusted monitoring hosts.

## 3. Locate the Prometheus configuration file

The path depends on how Prometheus was installed and started.

On macOS with Homebrew, a common location is:

```text
/opt/homebrew/etc/prometheus.yml
```

To inspect the Homebrew installation:

```bash
brew info prometheus
```

To see which configuration file the running Prometheus process uses:

```bash
ps aux | grep '[p]rometheus'
```

Look for the `--config.file` argument. Edit the file at that path.

## 4. Validate and start Prometheus

Validate the YAML configuration before restarting Prometheus:

```bash
promtool check config /path/to/prometheus.yml
```

If Prometheus was installed with Homebrew on macOS:

```bash
brew services restart prometheus
```

To start it for the first time:

```bash
brew services start prometheus
```

Open the Prometheus web interface:

[http://localhost:9090](http://localhost:9090)

Open **Status → Targets** or visit:

[http://localhost:9090/targets](http://localhost:9090/targets)

The `prometheus` and `odilon` targets should show **UP**. If the Odilon target is DOWN, check the target address and port, network/firewall access, metrics endpoint, and authentication credentials.

You can also test the endpoint from the Prometheus host:

```bash
curl -u monitor:YOUR_PASSWORD \
  http://localhost:9234/actuator/prometheus
```

A successful response contains Prometheus-format metrics. Avoid putting real passwords in shell history on shared or production systems.

## 5. Configure Grafana's Prometheus data source

Open Grafana:

[http://localhost:3000](http://localhost:3000)

1. Sign in.
2. Open **Connections → Data sources** (in some Grafana versions, this is under **Configuration → Data sources**).
3. Select **Add data source**.
4. Choose **Prometheus**.
5. Set the Prometheus server URL to:

   ```text
   http://localhost:9090
   ```

6. Click **Save & test**.

The URL must point to the Prometheus server as seen from Grafana. If Grafana and Prometheus run on different machines, use the Prometheus host address instead of `localhost`.

## 6. Import the Odilon Server dashboard

The official dashboard JSON is available in the Odilon Server GitHub repository:

[Odilon Server-1794774180021.json](https://github.com/atolomei/odilon-server/blob/main/monitoring/grafana/Odilon%20Server-1794774180021.json)

You can download the JSON file from GitHub using the **Raw** or **Download raw file** option, or use the direct raw file URL:

[Download dashboard JSON](https://raw.githubusercontent.com/atolomei/odilon-server/main/monitoring/grafana/Odilon%20Server-1794774180021.json)

Then import it:

1. In Grafana, open **Dashboards**.
2. Select **New → Import**.
3. Upload `Odilon Server-1794774180021.json` (or paste its JSON contents).
4. If Grafana asks you to select or map a data source, choose the Prometheus data source configured in the previous section.
5. Complete the import.

The dashboard should now appear in Grafana under **Dashboards**.

### Dashboard compatibility and data source mapping

This dashboard file was saved from **Grafana 13.3.0** and uses Grafana's dashboard resource `v2` JSON format. Use a Grafana version that supports this format; older Grafana releases may not be able to import it directly.

The exported file also contains a reference to the Prometheus data source UID used by the original Grafana installation. If the import process does not offer data source mapping and the panels report that the data source cannot be found, the dashboard's Prometheus references must be mapped to the local Prometheus data source. Do not assume that creating a data source with the display name `Prometheus` automatically matches the UID embedded in the file.

## 7. Verify the dashboard

After importing:

1. Open **Odilon Server** from the Grafana dashboard list.
2. Check that the panels display data.
3. If panels show **No data**, first verify that the Odilon target is **UP** in Prometheus.
4. In Prometheus, test a metric such as:

   ```promql
   odilon_objects_uploaded
   ```

If Prometheus returns data but Grafana does not, check the dashboard's data source mapping and the selected time range.

## 8. Important notes

- Prometheus collects metrics; Grafana visualizes them. Both services must be running.
- Odilon must also be running and its metrics endpoint must be reachable by Prometheus.
- The dashboard JSON contains dashboard definitions and queries, not Prometheus itself, historical metrics, or Odilon configuration.
- The sample configuration monitors one Odilon instance on port `9234`. Change the target address and port to match your installation.
- For multiple Odilon instances, add one target per instance and use labels such as `instance_name` to distinguish them.

## References

- [Odilon Server repository](https://github.com/atolomei/odilon-server)
- [Odilon Grafana dashboard JSON](https://github.com/atolomei/odilon-server/blob/main/monitoring/grafana/Odilon%20Server-1794774180021.json)
- [Prometheus configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)
- [Grafana dashboard import documentation](https://grafana.com/docs/grafana/latest/dashboards/)
