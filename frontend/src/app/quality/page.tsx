"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { DataChatAPI, type QualitySummaryResponse } from "@/lib/api";

const api = new DataChatAPI();

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

export default function QualityPage() {
  const searchParams = useSearchParams();
  const [windowHours, setWindowHours] = useState(24);
  const [summary, setSummary] = useState<QualitySummaryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [severityFilter, setSeverityFilter] = useState(searchParams.get("severity") || "all");
  const [categoryFilter, setCategoryFilter] = useState(searchParams.get("category") || "all");
  const [codeFilter, setCodeFilter] = useState(searchParams.get("code") || "all");

  const loadSummary = async (hours: number) => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.getQualitySummary(hours);
      setSummary(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load quality findings.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadSummary(windowHours);
  }, [windowHours]);

  const severityCount = (severity: string) =>
    summary?.severity_breakdown.find((item) => item.severity === severity)?.count ?? 0;

  useEffect(() => {
    setSeverityFilter(searchParams.get("severity") || "all");
    setCategoryFilter(searchParams.get("category") || "all");
    setCodeFilter(searchParams.get("code") || "all");
  }, [searchParams]);

  const filteredFindings = useMemo(
    () =>
      (summary?.recent_findings || []).filter((finding) => {
        if (severityFilter !== "all" && finding.severity !== severityFilter) return false;
        if (categoryFilter !== "all" && finding.category !== categoryFilter) return false;
        if (codeFilter !== "all" && finding.code !== codeFilter) return false;
        return true;
      }),
    [summary, severityFilter, categoryFilter, codeFilter]
  );

  return (
    <main className="min-h-screen bg-background">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
              Reliability
            </div>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">Quality</h1>
            <p className="max-w-2xl text-sm text-muted-foreground">
              Advisory findings grouped across runs so retrieval misses, validation issues, and low-confidence answers are visible immediately.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
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
          <MetricCard label="Total Findings" value={String(summary?.total_findings ?? 0)} helper="All advisories in window" />
          <MetricCard label="Runs Affected" value={String(summary?.runs_with_findings ?? 0)} helper="Distinct runs with findings" />
          <MetricCard label="Errors" value={String(severityCount("error"))} helper="Immediate investigation needed" />
          <MetricCard label="Warnings" value={String(severityCount("warning"))} helper="Advisories to review soon" />
        </div>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_340px]">
          <div className="space-y-6">
            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Filters</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Narrow findings by severity, category, or code before jumping into affected runs.
              </p>
              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <select
                  value={severityFilter}
                  onChange={(event) => setSeverityFilter(event.target.value)}
                  className="h-10 rounded-md border border-input bg-background px-3 text-sm"
                >
                  <option value="all">All severities</option>
                  {(summary?.severity_breakdown || []).map((item) => (
                    <option key={item.severity} value={item.severity}>
                      {item.severity}
                    </option>
                  ))}
                </select>
                <select
                  value={categoryFilter}
                  onChange={(event) => setCategoryFilter(event.target.value)}
                  className="h-10 rounded-md border border-input bg-background px-3 text-sm"
                >
                  <option value="all">All categories</option>
                  {(summary?.category_breakdown || []).map((item) => (
                    <option key={item.category} value={item.category}>
                      {item.category}
                    </option>
                  ))}
                </select>
                <select
                  value={codeFilter}
                  onChange={(event) => setCodeFilter(event.target.value)}
                  className="h-10 rounded-md border border-input bg-background px-3 text-sm"
                >
                  <option value="all">All codes</option>
                  {(summary?.code_breakdown || []).map((item) => (
                    <option key={item.code} value={item.code}>
                      {item.code}
                    </option>
                  ))}
                </select>
              </div>
              <div className="mt-3">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setSeverityFilter("all");
                    setCategoryFilter("all");
                    setCodeFilter("all");
                  }}
                >
                  Clear Filters
                </Button>
              </div>
            </Card>

            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Recent Findings</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Latest advisories captured from chat, profiling, and generation runs.
              </p>
              <div className="mt-4 space-y-3">
                {filteredFindings.length === 0 ? (
                  <div className="text-sm text-muted-foreground">No findings in this window.</div>
                ) : (
                  filteredFindings.map((finding) => (
                    <div key={finding.finding_id} className="rounded-lg border border-border/70 p-4">
                      <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
                        <div>
                          <div className="text-sm font-medium text-foreground">{finding.message}</div>
                          <div className="mt-1 text-xs text-muted-foreground">
                            {finding.severity} · {finding.category} · {finding.code}
                          </div>
                        </div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(finding.created_at)}</div>
                      </div>
                      <div className="mt-2 text-xs text-muted-foreground">
                        {finding.route} {finding.query ? `· ${finding.query}` : ""}
                      </div>
                      <div className="mt-3">
                        <Link
                          href={`/runs?run_id=${encodeURIComponent(finding.run_id)}`}
                          className="text-xs font-medium text-foreground underline-offset-4 hover:underline"
                        >
                          Open affected run
                        </Link>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </Card>
          </div>

          <div className="space-y-6">
            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Top Codes</h2>
              <p className="mt-1 text-xs text-muted-foreground">
                Most common issue signatures in the selected window.
              </p>
              <div className="mt-4 space-y-2">
                {(summary?.code_breakdown || []).length === 0 ? (
                  <div className="text-sm text-muted-foreground">No code breakdown available.</div>
                ) : (
                  (summary?.code_breakdown || []).map((item) => (
                    <button
                      key={item.code}
                      type="button"
                      onClick={() => setCodeFilter(item.code)}
                      className={`w-full rounded-md border px-3 py-2 text-left ${
                        codeFilter === item.code ? "border-primary bg-primary/5" : "border-border/70"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-sm font-medium text-foreground">{item.code}</span>
                        <span className="text-sm text-muted-foreground">{item.count}</span>
                      </div>
                      <div className="mt-1 text-[11px] text-muted-foreground">
                        {item.severity} · {item.category}
                      </div>
                    </button>
                  ))
                )}
              </div>
            </Card>

            <Card className="p-5">
              <h2 className="text-sm font-semibold text-foreground">Categories</h2>
              <div className="mt-4 space-y-2">
                {(summary?.category_breakdown || []).map((item) => (
                  <div key={item.category} className="flex items-center justify-between rounded-md border border-border/70 px-3 py-2 text-sm">
                    <span className="font-medium text-foreground">{item.category}</span>
                    <span className="text-muted-foreground">{item.count}</span>
                  </div>
                ))}
              </div>
            </Card>
          </div>
        </div>
      </div>
    </main>
  );
}
