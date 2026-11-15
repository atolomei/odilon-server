# Starting Prometheus and Grafana to Monitor Odilon on macOS

This guide describes how to start the local Prometheus and Grafana
services installed with Homebrew, and how to verify that they are
collecting and displaying Odilon metrics.

## 1. Prerequisites

The following services should be installed with Homebrew:

``` bash
brew install prometheus
brew install grafana
```

Odilon must also be running and exposing its Prometheus metrics
endpoint:

``` text
http://localhost:9234/actuator/prometheus
```

The endpoint may require HTTP Basic Authentication.

## 2. Start Prometheus

Start Prometheus as a background service:

``` bash
brew services start prometheus
```

Prometheus listens on port `9090` by default.

Open the Prometheus web interface:

<http://localhost:9090>

Check that Prometheus is healthy:

``` bash
curl -s http://localhost:9090/-/healthy
```

## 3. Start Grafana

Start Grafana as a background service:

``` bash
brew services start grafana
```

Grafana listens on port `3000` by default.

Open Grafana:

<http://localhost:3000>

Sign in with your configured Grafana credentials.

## 4. Verify both services

Check the Homebrew service status:

``` bash
brew services list | grep -E 'prometheus|grafana'
```

You can also check Grafana's health endpoint:

``` bash
curl -s http://localhost:3000/api/health
```

Both services should be running.

## 5. Verify the Odilon metrics endpoint

With Odilon running, request its metrics endpoint:

``` bash
curl -u monitor \
  http://localhost:9234/actuator/prometheus
```

Curl prompts for the password. Use the monitoring credentials configured
in Odilon.

The response should contain Prometheus-formatted metrics, including
metrics such as:

-   `odilon_objects_total`
-   `odilon_objects_get_total`
-   `odilon_objects_put_total`
-   `odilon_replication_lag_ms_milliseconds`
-   `odilon_cache_object_hit_total`
-   `jvm_memory_used_bytes`

## 6. Verify Prometheus is scraping Odilon

Open the Prometheus Targets page:

<http://localhost:9090/targets>

Find the `odilon` target. Its state should be **UP**.

You can also run this query in the Prometheus expression browser:

``` promql
up
```

A value of `1` indicates that a target is being scraped successfully. A
value of `0` indicates a scrape failure.

To check an Odilon-specific metric, run:

``` promql
odilon_objects_total
```

## 7. View metrics in Grafana

Open Grafana:

<http://localhost:3000>

The Prometheus data source should be configured with:

  Setting   Value
  --------- -------------------------
  Name      `prometheus`
  Type      Prometheus
  URL       `http://localhost:9090`
  Access    Server (default)

In Grafana, open **Connections → Data sources**, select `prometheus`,
and use **Save & test** to verify the connection.

Then open the **Odilon Dashboard**. A typical dashboard can include:

-   Storage Overview
-   Object Operations
-   Cache Performance
-   Replication
-   Encryption
-   JVM / System
-   HTTP API Performance

If panels unexpectedly show **No data**, first check the dashboard time
range and the Prometheus target status. Also verify that the dashboard
is using the correct Prometheus data source. If the Grafana UI appears
stale or behaves inconsistently, logging out and back in may refresh the
session.

## 8. Service management commands

  ----------------------------------------------------------------------------------------------
  Action                  Prometheus                           Grafana
  ----------------------- ------------------------------------ ---------------------------------
  Start                   `brew services start prometheus`     `brew services start grafana`

  Stop                    `brew services stop prometheus`      `brew services stop grafana`

  Restart                 `brew services restart prometheus`   `brew services restart grafana`
  ----------------------------------------------------------------------------------------------

To check both services:

``` bash
brew services list | grep -E 'prometheus|grafana'
```

## 9. Start both services

After the initial installation, the usual startup sequence is:

``` bash
brew services start prometheus
brew services start grafana
```

Then:

1.  Start Odilon, if it is not already running.
2.  Check <http://localhost:9090/targets> and confirm the Odilon target
    is **UP**.
3.  Open <http://localhost:3000> and view the **Odilon Dashboard**.

**Note:** Starting Prometheus and Grafana does not start Odilon. Odilon
must run separately for its metrics to be available. Prometheus collects
metrics from Odilon at the scrape interval configured in its
`prometheus.yml` file (15 seconds in the example configuration).
