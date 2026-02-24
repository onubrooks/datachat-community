import type React from "react";
import { BarChart3, Settings2 } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  ChartTooltipState,
  InteractiveChartType,
  ScatterChartConfig,
} from "@/components/visualizations/types";
import {
  formatAxisTick,
  formatMetricNumber,
  toNumber,
} from "@/components/visualizations/types";

interface ScatterChartProps {
  numericColumns: string[];
  rowObjects: Array<Record<string, unknown>>;
  scatterConfig: ScatterChartConfig;
  setScatterConfig: React.Dispatch<React.SetStateAction<ScatterChartConfig>>;
  showChartSettings: boolean;
  setShowChartSettings: React.Dispatch<React.SetStateAction<boolean>>;
  setChartTooltip: React.Dispatch<React.SetStateAction<ChartTooltipState | null>>;
  chartTooltipDescription: string | null;
  renderChartSettings: (chartType: InteractiveChartType) => React.ReactNode;
  renderFallback: (messageText: string) => React.ReactNode;
}

export function ScatterChart({
  numericColumns,
  rowObjects,
  scatterConfig,
  setScatterConfig,
  showChartSettings,
  setShowChartSettings,
  setChartTooltip,
  chartTooltipDescription,
  renderChartSettings,
  renderFallback,
}: ScatterChartProps) {
  const xCol = scatterConfig.xCol || numericColumns[0];
  const yCol = scatterConfig.yCol || numericColumns[1] || numericColumns[0];
  if (!xCol || !yCol) {
    return renderFallback("Scatter plot needs at least two numeric columns.");
  }

  const points = rowObjects
    .map((row) => ({
      x: toNumber(row[xCol]),
      y: toNumber(row[yCol]),
    }))
    .filter((row) => row.x !== null && row.y !== null)
    .slice(0, Math.max(2, scatterConfig.maxItems)) as Array<{ x: number; y: number }>;

  if (points.length < 2) {
    return renderFallback("Not enough numeric points for scatter plot.");
  }

  const width = 640;
  const height = 280;
  const padLeft = 56;
  const padRight = 24;
  const padTop = 20;
  const padBottom = 58;
  const plotWidth = width - padLeft - padRight;
  const plotHeight = height - padTop - padBottom;
  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const xRange = maxX - minX || 1;
  const yRange = maxY - minY || 1;
  const xTicks = [minX, minX + xRange / 2, maxX];
  const yTicks = [minY, minY + yRange / 2, maxY];

  return (
    <Card className="mt-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <BarChart3 size={16} />
          Scatter Plot
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() => setShowChartSettings((prev) => !prev)}
            aria-pressed={showChartSettings}
            aria-label="Toggle scatter chart settings"
          >
            <Settings2 size={12} />
            {showChartSettings ? "Hide settings" : "Chart settings"}
          </button>
        </div>
        {renderChartSettings("scatter")}
        <div className="overflow-x-auto">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="min-w-[360px] h-[280px]"
            role="img"
            aria-label={`Scatter plot of ${yCol} by ${xCol}`}
            style={{ width: `${Math.max(100, scatterConfig.zoom * 100)}%` }}
          >
            {yTicks.map((tick, index) => {
              const y = padTop + (1 - (tick - minY) / yRange) * plotHeight;
              return (
                <g key={`scatter-y-tick-${index}`}>
                  {scatterConfig.showGrid && (
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
            {xTicks.map((tick, index) => {
              const x = padLeft + ((tick - minX) / xRange) * plotWidth;
              return (
                <g key={`scatter-x-tick-${index}`}>
                  {scatterConfig.showGrid && (
                    <line
                      x1={x}
                      y1={padTop}
                      x2={x}
                      y2={height - padBottom}
                      stroke="#e2e8f0"
                      strokeDasharray="3 3"
                    />
                  )}
                  <text
                    x={x}
                    y={height - padBottom + 16}
                    textAnchor="middle"
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
            {points.map((point, index) => {
              const x = padLeft + ((point.x - minX) / xRange) * plotWidth;
              const y = padTop + (1 - (point.y - minY) / yRange) * plotHeight;
              return (
                <circle
                  key={`scatter-${index}`}
                  cx={x}
                  cy={y}
                  r="3.2"
                  fill="#2563eb"
                  opacity={scatterConfig.seriesVisible ? 0.85 : 0.2}
                  tabIndex={0}
                  aria-label={`${xCol}: ${formatMetricNumber(point.x)}, ${yCol}: ${formatMetricNumber(point.y)}`}
                  onMouseEnter={() =>
                    setChartTooltip({
                      title: `${xCol}: ${formatMetricNumber(point.x)}`,
                      detail: `${yCol}: ${formatMetricNumber(point.y)}`,
                    })
                  }
                  onMouseLeave={() => setChartTooltip(null)}
                  onFocus={() =>
                    setChartTooltip({
                      title: `${xCol}: ${formatMetricNumber(point.x)}`,
                      detail: `${yCol}: ${formatMetricNumber(point.y)}`,
                    })
                  }
                  onBlur={() => setChartTooltip(null)}
                />
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
              {yCol}
            </text>
            <g transform={`translate(${width - padRight - 188}, ${padTop + 6})`}>
              {scatterConfig.showLegend && (
                <>
                  <rect x="0" y="0" width="182" height="24" fill="#f8fafc" stroke="#e2e8f0" rx="4" />
                  <circle cx="12" cy="12" r="3.2" fill="#2563eb" />
                  <text x="24" y="15" fontSize="10" fill="#334155">
                    {`${xCol} vs ${yCol}`}
                  </text>
                </>
              )}
            </g>
          </svg>
        </div>
        {scatterConfig.showLegend && (
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() =>
              setScatterConfig((prev) => ({
                ...prev,
                seriesVisible: !prev.seriesVisible,
              }))
            }
            aria-pressed={!scatterConfig.seriesVisible}
          >
            {scatterConfig.seriesVisible ? "Hide" : "Show"} points
          </button>
        )}
        <p className="text-xs text-muted-foreground">
          X axis: {xCol} · Y axis: {yCol} · Legend: points · {points.length} points
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
