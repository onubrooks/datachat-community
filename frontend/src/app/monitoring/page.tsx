"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataChatAPI, type MonitoringSummaryResponse } from "@/lib/api";

const api = new DataChatAPI();

function formatPercent(value: number): string {
  return (value * 100).toFixed(1) + "%";
}

function formatDuration(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "n/a";
  if (value < 1000) return String(Math.round(value)) + " ms";
  return (value / 1000).toFixed(2) + " s";
}

function formatTimestamp(value?: string | null): string {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function MetricCard({
  label,
  value,
  helper,
}: {
  label: string;
  value: string;
  helper?: string;
}) {
  return (
    <Card className="p-4">
      <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-foreground">{value}</div>
      {helper ? <div className="mt-1 text-xs text-muted-foreground">{helper}</div> : null}
    </Card>
  );
}

export default function MonitoringPage() {
  const [windowHours, setWindowHours] = useState(24);
  const [summary, setSummary] = useState<MonitoringSummaryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadSummary = async (hours: number) => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.getMonitoringSummary(hours);
      setSummary(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load monitoring summary.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadSummary(windowHours);
  }, [windowHours]);

  return (
    <main className="min-h-screen bg-background">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
              Reliability
            </div>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">Monitoring</h1>
            <p className="max-w-2xl text-sm text-muted-foreground">
              Rollups from persisted runs: latency, failure classes, retrieval misses, and route health.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button asChild variant="secondary">
              <Link href="/runs">Runs</Link>
            </Button>
            <Button asChild variant="secondary">
              <Link href="/databases">Databases</Link>
            </Button>
            <Button asChild variant="secondary">
              <Link href="/settings">Settings</Link>
            </Button>
            <div className="flex items-center gap-1 rounded-lg border border-border/70 bg-muted/20 p-1">
              {[6, 24, 72].map((hours) => (
                <Button
                  key={hours}
                  type="button"
                  size="sm"
                  variant={windowHours === hours ? "default" : "secondary"}
                  onClick={() => setWindowHours(hours)}
                >
                  {hours}h
                </Button>
              ))}
            </div>
            <Button variant="outline" onClick={() => void loadSummary(windowHours)} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh"}
            </Button>
          </div>
        </div>

        {error ? <Card className="border-destructive/40 p-4 text-sm text-destructive">{error}</Card> : null}

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard label="Success Rate" value={summary ? formatPercent(summary.success_rate) : "..."} helper={String(windowHours) + " hour window"} />
          <MetricCard label="P50 Latency" value={summary ? formatDuration(summary.p50_latency_ms) : "..."} helper="Median response time" />
          <MetricCard label="P95 Latency" value={summary ? formatDuration(summary.p95_latency_ms) : "..."} helper="Tail latency" />
          <MetricCard label="Retrieval Miss Rate" value={summary ? formatPercent(summary.retrieval_miss_rate) : "..."} helper="Runs with zero retrieved datapoints" />
        </div>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div className="space-y-6">
            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Route Health</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Which routes are carrying most of the load and where failures are clustering.
              </p>
              <div className="mt-4 overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-xs uppercase tracking-[0.14em] text-muted-foreground">
                    <tr>
                      <th className="pb-3">Route</th>
                      <th className="pb-3">Count</th>
                      <th className="pb-3">Success</th>
                      <th className="pb-3">Failed</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border/70">
                    {(summary?.route_breakdown || []).map((route) => (
                      <tr key={route.route}>
                        <td className="py-3 font-medium text-foreground">{route.route}</td>
                        <td className="py-3 text-muted-foreground">{route.count}</td>
                        <td className="py-3 text-muted-foreground">{formatPercent(route.success_rate)}</td>
                        <td className="py-3 text-muted-foreground">{route.failed}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>

            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Recent Failures</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Latest failed runs in the selected time window.
              </p>
              <div className="mt-4 space-y-3">
                {(summary?.recent_failures || []).length === 0 ? (
                  <div className="text-sm text-muted-foreground">No failed runs in this window.</div>
                ) : (
                  (summary?.recent_failures || []).map((failure) => (
                    <div key={failure.run_id} className="rounded-lg border border-border/70 p-4">
                      <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
                        <div>
                          <div className="text-sm font-medium text-foreground">{failure.query || failure.route}</div>
                          <div className="mt-1 text-xs text-muted-foreground">
                            {failure.route} · {failure.failure_class || "unknown failure"}
                          </div>
                        </div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(failure.created_at)}</div>
                      </div>
                      <div className="mt-2 text-[11px] text-muted-foreground">Run ID: {failure.run_id}</div>
                    </div>
                  ))
                )}
              </div>
            </Card>
          </div>

          <div className="space-y-6">
            <MetricCard label="Total Runs" value={summary ? String(summary.total_runs) : "..."} helper="All persisted runs in window" />
            <MetricCard label="Completed" value={summary ? String(summary.completed_runs) : "..."} helper="Successful runs" />
            <MetricCard label="Failed" value={summary ? String(summary.failed_runs) : "..."} helper="Failed runs" />
            <MetricCard label="Clarification Rate" value={summary ? formatPercent(summary.clarification_rate) : "..."} helper="Runs that asked for clarification" />

            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Failure Classes</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Top grouped failure categories from persisted runs.
              </p>
              <div className="mt-4 space-y-2">
                {(summary?.failure_breakdown || []).length === 0 ? (
                  <div className="text-sm text-muted-foreground">No failure classes in this window.</div>
                ) : (
                  (summary?.failure_breakdown || []).map((item) => (
                    <div key={item.failure_class} className="flex items-center justify-between rounded-md border border-border/70 px-3 py-2 text-sm">
                      <span className="font-medium text-foreground">{item.failure_class}</span>
                      <span className="text-muted-foreground">{item.count}</span>
                    </div>
                  ))
                )}
              </div>
            </Card>
          </div>
        </div>
      </div>
    </main>
  );
}
