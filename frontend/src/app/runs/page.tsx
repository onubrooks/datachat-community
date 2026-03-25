"use client";

import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import {
  DataChatAPI,
  type RunDetailResponse,
  type RunStepResponse,
  type RunSummaryResponse,
} from "@/lib/api";

const api = new DataChatAPI();

type DetailTab = "overview" | "steps" | "retrieval";

function formatTimestamp(value?: string | null): string {
  if (!value) return "n/a";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatDuration(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "n/a";
  if (value < 1000) return `${Math.round(value)} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function formatCount(value: number): string {
  return new Intl.NumberFormat().format(value);
}

function StatusPill({ value }: { value: string }) {
  const normalized = value.toLowerCase();
  const className =
    normalized === "completed"
      ? "bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200"
      : normalized === "failed"
        ? "bg-rose-100 text-rose-900 dark:bg-rose-950 dark:text-rose-200"
        : "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200";
  return (
    <span className={`rounded-full px-2 py-1 text-[11px] font-medium ${className}`}>
      {value}
    </span>
  );
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

function SectionTitle({
  title,
  subtitle,
}: {
  title: string;
  subtitle?: string;
}) {
  return (
    <div className="space-y-1">
      <h2 className="text-sm font-semibold text-foreground">{title}</h2>
      {subtitle ? <p className="text-xs text-muted-foreground">{subtitle}</p> : null}
    </div>
  );
}

function extractGeneratedSql(step: RunStepResponse | null): string | null {
  if (!step) return null;
  const outputs = step.summary?.outputs;
  if (outputs && typeof outputs === "object") {
    const generatedSql = (outputs as Record<string, unknown>).generated_sql;
    if (typeof generatedSql === "string" && generatedSql.trim()) return generatedSql;
  }
  const direct = (step.summary as Record<string, unknown>)?.generated_sql;
  if (typeof direct === "string" && direct.trim()) return direct;
  return null;
}

function compactJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function runTitle(run: RunSummaryResponse | RunDetailResponse | null): string {
  if (!run) return "n/a";
  return String(run.summary?.query || run.route || run.run_type);
}

function extractRunGeneratedSql(run: RunDetailResponse | null): string | null {
  if (!run) return null;
  const sqlStep = run.steps.find((step) => step.step_name === "sql") || run.steps[0] || null;
  return extractGeneratedSql(sqlStep);
}

function extractSelectedDatapoints(run: RunDetailResponse | null): Array<Record<string, unknown>> {
  if (!run) return [];
  const trace = (run.output?.retrieval_trace || {}) as Record<string, unknown>;
  return Array.isArray(trace.selected_datapoints)
    ? (trace.selected_datapoints as Array<Record<string, unknown>>)
    : [];
}

export default function RunsPage() {
  const [runs, setRuns] = useState<RunSummaryResponse[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRun, setSelectedRun] = useState<RunDetailResponse | null>(null);
  const [compareRunId, setCompareRunId] = useState<string | null>(null);
  const [compareRun, setCompareRun] = useState<RunDetailResponse | null>(null);
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<DetailTab | "compare">("overview");
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [compareLoading, setCompareLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [compareError, setCompareError] = useState<string | null>(null);

  const loadRuns = async (preferredRunId?: string | null) => {
    setLoading(true);
    setError(null);
    try {
      const response = await api.listRuns(100);
      setRuns(response.runs);
      const nextRunId = preferredRunId || response.runs[0]?.run_id || null;
      setSelectedRunId(nextRunId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load runs.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRuns();
  }, []);

  useEffect(() => {
    if (!selectedRunId) {
      setSelectedRun(null);
      setSelectedStepId(null);
      return;
    }
    let cancelled = false;
    const loadRun = async () => {
      setDetailLoading(true);
      setDetailError(null);
      try {
        const response = await api.getRun(selectedRunId);
        if (!cancelled) {
          setSelectedRun(response);
          setSelectedStepId(response.steps[0]?.step_id || null);
        }
      } catch (err) {
        if (!cancelled) {
          setDetailError(err instanceof Error ? err.message : "Failed to load run detail.");
        }
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    };
    void loadRun();
    return () => {
      cancelled = true;
    };
  }, [selectedRunId]);

  useEffect(() => {
    if (!compareRunId || compareRunId === selectedRunId) {
      setCompareRun(null);
      setCompareError(null);
      if (compareRunId === selectedRunId) {
        setCompareRunId(null);
      }
      return;
    }
    let cancelled = false;
    const loadCompareRun = async () => {
      setCompareLoading(true);
      setCompareError(null);
      try {
        const response = await api.getRun(compareRunId);
        if (!cancelled) {
          setCompareRun(response);
        }
      } catch (err) {
        if (!cancelled) {
          setCompareError(err instanceof Error ? err.message : "Failed to load comparison run.");
        }
      } finally {
        if (!cancelled) setCompareLoading(false);
      }
    };
    void loadCompareRun();
    return () => {
      cancelled = true;
    };
  }, [compareRunId, selectedRunId]);

  const stats = useMemo(() => {
    const total = runs.length;
    const completed = runs.filter((run) => run.status === "completed").length;
    const failed = runs.filter((run) => run.status === "failed").length;
    const medianLatency = (() => {
      const values = runs
        .map((run) => run.latency_ms)
        .filter((value): value is number => typeof value === "number")
        .sort((a, b) => a - b);
      if (!values.length) return null;
      const mid = Math.floor(values.length / 2);
      return values.length % 2 === 0 ? (values[mid - 1] + values[mid]) / 2 : values[mid];
    })();
    return { total, completed, failed, medianLatency };
  }, [runs]);

  const selectedStep = useMemo(() => {
    if (!selectedRun || !selectedStepId) return selectedRun?.steps[0] || null;
    return selectedRun.steps.find((step) => step.step_id === selectedStepId) || selectedRun.steps[0] || null;
  }, [selectedRun, selectedStepId]);

  const retrievalTrace = (selectedRun?.output?.retrieval_trace || {}) as Record<string, unknown>;
  const vectorCandidates = Array.isArray(retrievalTrace.vector_candidates)
    ? retrievalTrace.vector_candidates
    : [];
  const graphCandidates = Array.isArray(retrievalTrace.graph_candidates)
    ? retrievalTrace.graph_candidates
    : [];
  const rrfCandidates = Array.isArray(retrievalTrace.rrf_candidates)
    ? retrievalTrace.rrf_candidates
    : [];
  const selectedDatapoints = Array.isArray(retrievalTrace.selected_datapoints)
    ? retrievalTrace.selected_datapoints
    : [];
  const precedence = (retrievalTrace.precedence || {}) as Record<string, unknown>;
  const connectionScope = (retrievalTrace.connection_scope || {}) as Record<string, unknown>;
  const liveSchemaFilter = (retrievalTrace.live_schema_filter || {}) as Record<string, unknown>;
  const precedenceFiltered = Array.isArray(precedence.filtered_out) ? precedence.filtered_out : [];
  const connectionFiltered = Array.isArray(connectionScope.filtered_out)
    ? connectionScope.filtered_out
    : [];
  const liveSchemaFiltered = Array.isArray(liveSchemaFilter.filtered_out)
    ? liveSchemaFilter.filtered_out
    : [];
  const generatedSql = extractGeneratedSql(selectedStep);
  const primarySql = extractRunGeneratedSql(selectedRun);
  const comparisonSql = extractRunGeneratedSql(compareRun);
  const primaryDatapoints = extractSelectedDatapoints(selectedRun);
  const comparisonDatapoints = extractSelectedDatapoints(compareRun);
  const compareCandidates = runs.filter((run) => run.run_id !== selectedRunId);

  return (
    <main className="min-h-screen bg-background">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <div className="text-[11px] uppercase tracking-[0.22em] text-muted-foreground">
              Reliability
            </div>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">Runs</h1>
            <p className="max-w-2xl text-sm text-muted-foreground">
              Inspect execution flow, generated SQL, retrieval choices, and failure reasons in one place.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="outline" onClick={() => void loadRuns(selectedRunId)} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh"}
            </Button>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard label="Total Runs" value={formatCount(stats.total)} helper="Latest persisted activity" />
          <MetricCard label="Completed" value={formatCount(stats.completed)} helper="Healthy successful runs" />
          <MetricCard label="Failed" value={formatCount(stats.failed)} helper="Runs needing inspection" />
          <MetricCard
            label="Median Latency"
            value={formatDuration(stats.medianLatency)}
            helper="Across runs with timing data"
          />
        </div>

        {error ? (
          <Card className="border-destructive/40 p-4 text-sm text-destructive">{error}</Card>
        ) : null}

        <div className="grid gap-6 xl:grid-cols-[360px_minmax(0,1fr)]">
          <Card className="overflow-hidden">
            <div className="border-b border-border/70 px-5 py-4">
              <SectionTitle
                title="Recent Runs"
                subtitle="Choose a run to inspect execution detail."
              />
            </div>
            <div className="max-h-[74vh] overflow-y-auto">
              {loading ? (
                <div className="p-5 text-sm text-muted-foreground">Loading runs...</div>
              ) : runs.length === 0 ? (
                <div className="p-5 text-sm text-muted-foreground">
                  No runs recorded yet. Ask a question or run profiling/generation first.
                </div>
              ) : (
                <div className="divide-y divide-border/70">
                  {runs.map((run) => {
                    const active = run.run_id === selectedRunId;
                    return (
                      <button
                        key={run.run_id}
                        type="button"
                        onClick={() => {
                          setSelectedRunId(run.run_id);
                          setActiveTab("overview");
                        }}
                        className={`flex w-full flex-col gap-2 px-5 py-4 text-left transition ${
                          active ? "bg-primary/5" : "hover:bg-muted/40"
                        }`}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-semibold text-foreground">
                              {String(run.summary?.query || run.route || run.run_type)}
                            </div>
                            <div className="mt-1 text-xs text-muted-foreground">
                              {run.run_type} · {run.route || "unknown route"}
                            </div>
                          </div>
                          <StatusPill value={run.status} />
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-[11px] text-muted-foreground">
                          <div>Latency: {formatDuration(run.latency_ms)}</div>
                          <div>Confidence: {run.confidence?.toFixed(2) || "n/a"}</div>
                          <div>Warnings: {run.warning_count}</div>
                          <div>Errors: {run.error_count}</div>
                        </div>
                        <div className="text-[11px] text-muted-foreground">
                          {formatTimestamp(run.created_at)}
                        </div>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          </Card>

          <Card className="overflow-hidden xl:sticky xl:top-6 xl:max-h-[82vh]">
            <div className="border-b border-border/70 px-5 py-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div className="space-y-1">
                  <SectionTitle
                    title="Run Detail"
                    subtitle="Overview, steps, retrieval diagnostics, and before/after comparison."
                  />
                  {selectedRun ? (
                    <div className="text-xs text-muted-foreground">
                      {formatTimestamp(selectedRun.created_at)}
                    </div>
                  ) : null}
                </div>
                {selectedRun ? <StatusPill value={selectedRun.status} /> : null}
              </div>
              {selectedRun ? (
                <div className="mt-4 flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                  <div className="text-xs text-muted-foreground">
                    Compare the selected run against a prior retry, training attempt, or alternate answer path.
                  </div>
                  <select
                    value={compareRunId || ""}
                    onChange={(event) => setCompareRunId(event.target.value || null)}
                    className="h-9 min-w-[280px] rounded-md border border-input bg-background px-3 text-sm"
                  >
                    <option value="">No comparison selected</option>
                    {compareCandidates.map((run) => (
                      <option key={run.run_id} value={run.run_id}>
                        {runTitle(run)} · {formatTimestamp(run.created_at)}
                      </option>
                    ))}
                  </select>
                </div>
              ) : null}
              <div className="mt-4 flex flex-wrap gap-2 rounded-lg border border-border/70 bg-muted/20 p-1">
                {([
                  { key: "overview", label: "Overview" },
                  { key: "steps", label: "Steps" },
                  { key: "retrieval", label: "Retrieval" },
                  { key: "compare", label: "Compare" },
                ] as const).map((tab) => (
                  <Button
                    key={tab.key}
                    type="button"
                    size="sm"
                    variant={activeTab === tab.key ? "default" : "secondary"}
                    onClick={() => setActiveTab(tab.key)}
                  >
                    {tab.label}
                  </Button>
                ))}
              </div>
            </div>

            <div className="max-h-[68vh] overflow-y-auto p-5">
              {detailLoading ? (
                <div className="text-sm text-muted-foreground">Loading run detail...</div>
              ) : detailError ? (
                <div className="text-sm text-destructive">{detailError}</div>
              ) : !selectedRun ? (
                <div className="text-sm text-muted-foreground">Select a run to inspect it.</div>
              ) : activeTab === "overview" ? (
                <div className="space-y-6">
                  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                    <MetricCard label="Route" value={selectedRun.route || "n/a"} helper={selectedRun.run_type} />
                    <MetricCard label="Latency" value={formatDuration(selectedRun.latency_ms)} helper={`Started ${formatTimestamp(selectedRun.started_at)}`} />
                    <MetricCard label="Confidence" value={selectedRun.confidence?.toFixed(2) || "n/a"} helper={`Warnings ${selectedRun.warning_count} · Errors ${selectedRun.error_count}`} />
                    <MetricCard label="Failure Class" value={selectedRun.failure_class || "none"} helper="Top-level classification" />
                  </div>

                  <div className="grid gap-3 rounded-lg border border-border/70 bg-muted/20 p-4 md:grid-cols-2">
                    <div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Run ID</div>
                      <div className="mt-1 break-all text-xs text-foreground">{selectedRun.run_id}</div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Connection</div>
                      <div className="mt-1 break-all text-xs text-foreground">{selectedRun.connection_id || "n/a"}</div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Completed</div>
                      <div className="mt-1 text-xs text-foreground">{formatTimestamp(selectedRun.completed_at)}</div>
                    </div>
                    <div>
                      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">Step Count</div>
                      <div className="mt-1 text-xs text-foreground">{selectedRun.steps.length}</div>
                    </div>
                  </div>

                  <Card className="p-4">
                    <SectionTitle title="Summary" subtitle="Persisted run summary used for monitoring rollups." />
                    <pre className="mt-3 overflow-x-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                      {compactJson(selectedRun.summary)}
                    </pre>
                  </Card>

                  <Card className="p-4">
                    <SectionTitle title="Quality Findings" subtitle="Advisories captured for this run." />
                    <div className="mt-3 space-y-3">
                      {(selectedRun.quality_findings || []).length === 0 ? (
                        <div className="text-sm text-muted-foreground">No persisted quality findings for this run.</div>
                      ) : (
                        selectedRun.quality_findings.map((finding) => (
                          <div key={finding.finding_id} className="rounded-md border border-border/70 p-3">
                            <div className="flex items-start justify-between gap-3">
                              <div>
                                <div className="text-sm font-medium text-foreground">{finding.message}</div>
                                <div className="mt-1 text-[11px] text-muted-foreground">
                                  {finding.severity} · {finding.category} · {finding.code}
                                </div>
                              </div>
                              <div className="text-[11px] text-muted-foreground">{formatTimestamp(finding.created_at)}</div>
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  </Card>
                </div>
              ) : activeTab === "steps" ? (
                <div className="grid gap-4 lg:grid-cols-[280px_minmax(0,1fr)]">
                  <div className="space-y-3">
                    <SectionTitle title="Ordered Steps" subtitle="Select a step to inspect its payload." />
                    <div className="space-y-2">
                      {selectedRun.steps.map((step) => {
                        const active = step.step_id === selectedStep?.step_id;
                        return (
                          <button
                            key={step.step_id}
                            type="button"
                            onClick={() => setSelectedStepId(step.step_id)}
                            className={`w-full rounded-lg border p-3 text-left transition ${
                              active
                                ? "border-primary bg-primary/5"
                                : "border-border/70 hover:bg-muted/40"
                            }`}
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div>
                                <div className="text-sm font-semibold text-foreground">{step.step_name}</div>
                                <div className="text-[11px] text-muted-foreground">Step {step.step_order}</div>
                              </div>
                              <StatusPill value={step.status} />
                            </div>
                            <div className="mt-2 text-[11px] text-muted-foreground">
                              {formatDuration(step.latency_ms)}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>

                  <div className="space-y-4">
                    {selectedStep ? (
                      <>
                        <Card className="p-4">
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <div className="text-sm font-semibold text-foreground">{selectedStep.step_name}</div>
                              <div className="mt-1 text-xs text-muted-foreground">
                                {formatTimestamp(selectedStep.created_at)} · {formatDuration(selectedStep.latency_ms)}
                              </div>
                            </div>
                            <StatusPill value={selectedStep.status} />
                          </div>
                        </Card>

                        {generatedSql ? (
                          <Card className="p-4">
                            <SectionTitle title="Generated SQL" subtitle="Captured directly from the SQL stage payload." />
                            <pre className="mt-3 overflow-x-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                              {generatedSql}
                            </pre>
                          </Card>
                        ) : null}

                        <Card className="p-4">
                          <SectionTitle title="Step Payload" subtitle="Full persisted action-step summary." />
                          <pre className="mt-3 overflow-x-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                            {compactJson(selectedStep.summary)}
                          </pre>
                        </Card>
                      </>
                    ) : (
                      <div className="text-sm text-muted-foreground">No step selected.</div>
                    )}
                  </div>
                </div>
              ) : activeTab === "retrieval" ? (
                <div className="space-y-6">
                  <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                    <MetricCard label="Vector" value={formatCount(vectorCandidates.length)} helper="Semantic candidates" />
                    <MetricCard label="Graph" value={formatCount(graphCandidates.length)} helper="Structural candidates" />
                    <MetricCard label="RRF" value={formatCount(rrfCandidates.length)} helper="Fused ranking pool" />
                    <MetricCard label="Selected" value={formatCount(selectedDatapoints.length)} helper="Final datapoints passed downstream" />
                  </div>

                  <div className="grid gap-4 xl:grid-cols-2">
                    <Card className="p-4">
                      <SectionTitle title="Selected Datapoints" subtitle="Final retrieval outputs after all filters." />
                      <div className="mt-3 space-y-2">
                        {selectedDatapoints.length === 0 ? (
                          <div className="text-xs text-muted-foreground">No selected datapoints.</div>
                        ) : (
                          selectedDatapoints.map((item, index) => {
                            const typed = item as Record<string, unknown>;
                            return (
                              <div key={`${typed.datapoint_id || index}`} className="rounded-md border border-border/70 p-3">
                                <div className="text-sm font-medium text-foreground">
                                  {String(typed.name || typed.datapoint_id || "unknown")}
                                </div>
                                <div className="mt-1 text-xs text-muted-foreground">
                                  {String(typed.source || "unknown")} · score {typeof typed.score === "number" ? typed.score.toFixed(3) : String(typed.score || "n/a")}
                                </div>
                              </div>
                            );
                          })
                        )}
                      </div>
                    </Card>

                    <Card className="p-4">
                      <SectionTitle title="Filtered Out" subtitle="Why candidates were removed before selection." />
                      <div className="mt-3 space-y-4 text-xs">
                        <div>
                          <div className="font-medium text-foreground">Precedence</div>
                          <pre className="mt-2 overflow-x-auto rounded-md bg-muted/50 p-3 leading-5 text-foreground">
                            {compactJson(precedenceFiltered)}
                          </pre>
                        </div>
                        <div>
                          <div className="font-medium text-foreground">Connection Scope</div>
                          <pre className="mt-2 overflow-x-auto rounded-md bg-muted/50 p-3 leading-5 text-foreground">
                            {compactJson(connectionFiltered)}
                          </pre>
                        </div>
                        <div>
                          <div className="font-medium text-foreground">Live Schema</div>
                          <pre className="mt-2 overflow-x-auto rounded-md bg-muted/50 p-3 leading-5 text-foreground">
                            {compactJson(liveSchemaFiltered)}
                          </pre>
                        </div>
                      </div>
                    </Card>
                  </div>

                  <Card className="p-4">
                    <SectionTitle title="Raw Retrieval Trace" subtitle="Low-level diagnostic payload for deeper debugging." />
                    <pre className="mt-3 max-h-[420px] overflow-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                      {compactJson(retrievalTrace)}
                    </pre>
                  </Card>
                </div>
              ) : (
                <div className="space-y-6">
                  {compareLoading ? (
                    <div className="text-sm text-muted-foreground">Loading comparison run...</div>
                  ) : compareError ? (
                    <div className="text-sm text-destructive">{compareError}</div>
                  ) : !compareRun ? (
                    <div className="text-sm text-muted-foreground">
                      Select a comparison run from the dropdown above to inspect before/after changes.
                    </div>
                  ) : (
                    <>
                      <div className="grid gap-4 md:grid-cols-2">
                        <Card className="p-4">
                          <SectionTitle title="Primary Run" subtitle={formatTimestamp(selectedRun.created_at)} />
                          <div className="mt-3 space-y-2 text-sm text-foreground">
                            <div>{runTitle(selectedRun)}</div>
                            <div className="text-xs text-muted-foreground">
                              {selectedRun.route || "n/a"} · {formatDuration(selectedRun.latency_ms)} · confidence {selectedRun.confidence?.toFixed(2) || "n/a"}
                            </div>
                          </div>
                        </Card>
                        <Card className="p-4">
                          <SectionTitle title="Comparison Run" subtitle={formatTimestamp(compareRun.created_at)} />
                          <div className="mt-3 space-y-2 text-sm text-foreground">
                            <div>{runTitle(compareRun)}</div>
                            <div className="text-xs text-muted-foreground">
                              {compareRun.route || "n/a"} · {formatDuration(compareRun.latency_ms)} · confidence {compareRun.confidence?.toFixed(2) || "n/a"}
                            </div>
                          </div>
                        </Card>
                      </div>

                      <div className="grid gap-4 md:grid-cols-2">
                        <Card className="p-4">
                          <SectionTitle title="Generated SQL" subtitle="Primary run SQL payload." />
                          <pre className="mt-3 overflow-x-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                            {primarySql || "No generated SQL captured for this run."}
                          </pre>
                        </Card>
                        <Card className="p-4">
                          <SectionTitle title="Generated SQL" subtitle="Comparison run SQL payload." />
                          <pre className="mt-3 overflow-x-auto rounded-md bg-muted/50 p-3 text-[11px] leading-5 text-foreground">
                            {comparisonSql || "No generated SQL captured for this run."}
                          </pre>
                        </Card>
                      </div>

                      <div className="grid gap-4 md:grid-cols-2">
                        <Card className="p-4">
                          <SectionTitle
                            title="Selected Datapoints"
                            subtitle={`Primary run selected ${primaryDatapoints.length} datapoint(s).`}
                          />
                          <div className="mt-3 space-y-2">
                            {primaryDatapoints.length === 0 ? (
                              <div className="text-sm text-muted-foreground">No selected datapoints.</div>
                            ) : (
                              primaryDatapoints.map((item, index) => (
                                <div key={`${String(item.datapoint_id || index)}-primary`} className="rounded-md border border-border/70 p-3">
                                  <div className="text-sm font-medium text-foreground">
                                    {String(item.name || item.datapoint_id || "unknown")}
                                  </div>
                                  <div className="mt-1 text-xs text-muted-foreground">
                                    {String(item.source || "unknown")}
                                  </div>
                                </div>
                              ))
                            )}
                          </div>
                        </Card>
                        <Card className="p-4">
                          <SectionTitle
                            title="Selected Datapoints"
                            subtitle={`Comparison run selected ${comparisonDatapoints.length} datapoint(s).`}
                          />
                          <div className="mt-3 space-y-2">
                            {comparisonDatapoints.length === 0 ? (
                              <div className="text-sm text-muted-foreground">No selected datapoints.</div>
                            ) : (
                              comparisonDatapoints.map((item, index) => (
                                <div key={`${String(item.datapoint_id || index)}-comparison`} className="rounded-md border border-border/70 p-3">
                                  <div className="text-sm font-medium text-foreground">
                                    {String(item.name || item.datapoint_id || "unknown")}
                                  </div>
                                  <div className="mt-1 text-xs text-muted-foreground">
                                    {String(item.source || "unknown")}
                                  </div>
                                </div>
                              ))
                            )}
                          </div>
                        </Card>
                      </div>

                      <div className="grid gap-4 md:grid-cols-2">
                        <Card className="p-4">
                          <SectionTitle
                            title="Quality Findings"
                            subtitle={`Primary run findings: ${selectedRun.quality_findings.length}`}
                          />
                          <div className="mt-3 space-y-2">
                            {selectedRun.quality_findings.length === 0 ? (
                              <div className="text-sm text-muted-foreground">No findings recorded.</div>
                            ) : (
                              selectedRun.quality_findings.map((finding) => (
                                <div key={finding.finding_id} className="rounded-md border border-border/70 p-3">
                                  <div className="text-sm font-medium text-foreground">{finding.message}</div>
                                  <div className="mt-1 text-xs text-muted-foreground">
                                    {finding.severity} · {finding.category} · {finding.code}
                                  </div>
                                </div>
                              ))
                            )}
                          </div>
                        </Card>
                        <Card className="p-4">
                          <SectionTitle
                            title="Quality Findings"
                            subtitle={`Comparison run findings: ${compareRun.quality_findings.length}`}
                          />
                          <div className="mt-3 space-y-2">
                            {compareRun.quality_findings.length === 0 ? (
                              <div className="text-sm text-muted-foreground">No findings recorded.</div>
                            ) : (
                              compareRun.quality_findings.map((finding) => (
                                <div key={finding.finding_id} className="rounded-md border border-border/70 p-3">
                                  <div className="text-sm font-medium text-foreground">{finding.message}</div>
                                  <div className="mt-1 text-xs text-muted-foreground">
                                    {finding.severity} · {finding.category} · {finding.code}
                                  </div>
                                </div>
                              ))
                            )}
                          </div>
                        </Card>
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
          </Card>
        </div>
      </div>
    </main>
  );
}
