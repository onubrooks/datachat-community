import type React from "react";
import { BarChart3, Settings2 } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  ChartTooltipState,
  InteractiveChartType,
  LineChartConfig,
} from "@/components/visualizations/types";
import {
  formatAxisTick,
  formatMetricNumber,
  isDateLikeColumn,
  toNumber,
} from "@/components/visualizations/types";

interface LineChartProps {
  columnNames: string[];
  numericColumns: string[];
  nonNumericColumns: string[];
  rowObjects: Array<Record<string, unknown>>;
  lineConfig: LineChartConfig;
  setLineConfig: React.Dispatch<React.SetStateAction<LineChartConfig>>;
  showChartSettings: boolean;
  setShowChartSettings: React.Dispatch<React.SetStateAction<boolean>>;
  setChartTooltip: React.Dispatch<React.SetStateAction<ChartTooltipState | null>>;
  chartTooltipDescription: string | null;
  renderChartSettings: (chartType: InteractiveChartType) => React.ReactNode;
  renderFallback: (messageText: string) => React.ReactNode;
}

export function LineChart({
  columnNames,
  numericColumns,
  nonNumericColumns,
  rowObjects,
  lineConfig,
  setLineConfig,
  showChartSettings,
  setShowChartSettings,
  setChartTooltip,
  chartTooltipDescription,
  renderChartSettings,
  renderFallback,
}: LineChartProps) {
  const valueCol = lineConfig.yCol || numericColumns[0];
  const xCol =
    lineConfig.xCol ||
    columnNames.find((column) => isDateLikeColumn(column)) ||
    nonNumericColumns[0] ||
    columnNames[0];
  if (!valueCol || !xCol) {
    return renderFallback("This result shape is not suitable for a line chart.");
  }

  const points = rowObjects
    .map((row, index) => ({
      xLabel: String(row[xCol] ?? index + 1),
      y: toNumber(row[valueCol]),
    }))
    .filter((row) => row.y !== null)
    .slice(0, Math.max(2, lineConfig.maxItems)) as Array<{ xLabel: string; y: number }>;

  if (points.length < 2) {
    return renderFallback("Not enough points to draw a line chart.");
  }

  const width = 640;
  const height = 280;
  const padLeft = 56;
  const padRight = 24;
  const padTop = 20;
  const padBottom = 58;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const ys = points.map((point) => point.y);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const yRange = maxY - minY || 1;
  const yTicks = [minY, minY + yRange / 2, maxY];
  const xTickIndexes = Array.from(
    new Set([0, Math.floor((points.length - 1) / 2), points.length - 1])
  );

  const linePoints = points
    .map((point, index) => {
      const x = padLeft + (index / (points.length - 1)) * plotWidth;
      const y = padTop + (1 - (point.y - minY) / yRange) * plotHeight;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <Card className="mt-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <BarChart3 size={16} />
          Line Chart
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() => setShowChartSettings((prev) => !prev)}
            aria-pressed={showChartSettings}
            aria-label="Toggle line chart settings"
          >
            <Settings2 size={12} />
            {showChartSettings ? "Hide settings" : "Chart settings"}
          </button>
        </div>
        {renderChartSettings("line_chart")}
        <div className="overflow-x-auto">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="min-w-[360px] h-[280px]"
            role="img"
            aria-label={`Line chart of ${valueCol} by ${xCol}`}
            style={{ width: `${Math.max(100, lineConfig.zoom * 100)}%` }}
          >
            {yTicks.map((tick, index) => {
              const y = padTop + (1 - (tick - minY) / yRange) * plotHeight;
              return (
                <g key={`line-y-tick-${index}`}>
                  {lineConfig.showGrid && (
                    <line
                      x1={padLeft}
                      y1={y}
                      x2={width - padRight}
                      y2={y}
                      stroke="#e2e8f0"
                      strokeDasharray="3 3"
                    />
                  )}
                  <text
                    x={padLeft - 8}
                    y={y + 4}
                    textAnchor="end"
                    fontSize="10"
                    fill="#64748b"
                  >
                    {formatAxisTick(tick)}
                  </text>
                </g>
              );
            })}
            <line
              x1={padLeft}
              y1={height - padBottom}
              x2={width - padRight}
              y2={height - padBottom}
              stroke="#94a3b8"
            />
            <line
              x1={padLeft}
              y1={padTop}
              x2={padLeft}
              y2={height - padBottom}
              stroke="#94a3b8"
            />
            <polyline
              fill="none"
              stroke="#2563eb"
              strokeWidth="2.5"
              points={linePoints}
              opacity={lineConfig.seriesVisible ? 1 : 0.2}
            />
            {lineConfig.seriesVisible &&
              points.map((point, index) => {
                const x = padLeft + (index / (points.length - 1)) * plotWidth;
                const y = padTop + (1 - (point.y - minY) / yRange) * plotHeight;
                return (
                  <circle
                    key={`${point.xLabel}-${index}`}
                    cx={x}
                    cy={y}
                    r="3"
                    fill="#2563eb"
                    tabIndex={0}
                    aria-label={`${xCol}: ${point.xLabel}, ${valueCol}: ${formatMetricNumber(point.y)}`}
                    onMouseEnter={() =>
                      setChartTooltip({
                        title: point.xLabel,
                        detail: `${valueCol}: ${formatMetricNumber(point.y)}`,
                      })
                    }
                    onMouseLeave={() => setChartTooltip(null)}
                    onFocus={() =>
                      setChartTooltip({
                        title: point.xLabel,
                        detail: `${valueCol}: ${formatMetricNumber(point.y)}`,
                      })
                    }
                    onBlur={() => setChartTooltip(null)}
                  />
                );
              })}
            {xTickIndexes.map((index) => {
              const x = padLeft + (index / Math.max(points.length - 1, 1)) * plotWidth;
              const raw = points[index]?.xLabel ?? "";
              const label = raw.length > 16 ? `${raw.slice(0, 15)}…` : raw;
              return (
                <text
                  key={`line-x-tick-${index}`}
                  x={x}
                  y={height - padBottom + 16}
                  textAnchor="middle"
                  fontSize="10"
                  fill="#64748b"
                >
                  {label}
                </text>
              );
            })}
            <text
              x={padLeft + plotWidth / 2}
              y={height - 8}
              textAnchor="middle"
              fontSize="11"
              fill="#334155"
            >
              {xCol}
            </text>
            <text
              x={14}
              y={padTop + plotHeight / 2}
              textAnchor="middle"
              fontSize="11"
              fill="#334155"
              transform={`rotate(-90 14 ${padTop + plotHeight / 2})`}
            >
              {valueCol}
            </text>
            <g transform={`translate(${width - padRight - 140}, ${padTop + 6})`}>
              {lineConfig.showLegend && (
                <>
                  <rect x="0" y="0" width="134" height="24" fill="#f8fafc" stroke="#e2e8f0" rx="4" />
                  <rect x="8" y="9" width="14" height="2.5" fill="#2563eb" />
                  <text x="28" y="13" fontSize="10" fill="#334155">
                    {valueCol}
                  </text>
                </>
              )}
            </g>
          </svg>
        </div>
        {lineConfig.showLegend && (
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() =>
              setLineConfig((prev) => ({ ...prev, seriesVisible: !prev.seriesVisible }))
            }
            aria-pressed={!lineConfig.seriesVisible}
          >
            {lineConfig.seriesVisible ? "Hide" : "Show"} {valueCol}
          </button>
        )}
        <p className="text-xs text-muted-foreground">
          X axis: {xCol} · Y axis: {valueCol} · Legend: {valueCol} · {points.length} points
        </p>
        {chartTooltipDescription && (
          <p className="text-xs text-muted-foreground" aria-live="polite">
            {chartTooltipDescription}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
