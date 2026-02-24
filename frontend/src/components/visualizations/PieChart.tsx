import type React from "react";
import { BarChart3, Settings2 } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  ChartTooltipState,
  InteractiveChartType,
  PieChartConfig,
} from "@/components/visualizations/types";
import {
  CHART_COLORS,
  formatMetricNumber,
  toNumber,
} from "@/components/visualizations/types";

interface PieChartProps {
  messageId: string;
  columnNames: string[];
  numericColumns: string[];
  nonNumericColumns: string[];
  rowObjects: Array<Record<string, unknown>>;
  pieConfig: PieChartConfig;
  setPieConfig: React.Dispatch<React.SetStateAction<PieChartConfig>>;
  showChartSettings: boolean;
  setShowChartSettings: React.Dispatch<React.SetStateAction<boolean>>;
  hiddenPieLabels: string[];
  setHiddenPieLabels: React.Dispatch<React.SetStateAction<string[]>>;
  setChartTooltip: React.Dispatch<React.SetStateAction<ChartTooltipState | null>>;
  chartTooltipDescription: string | null;
  renderChartSettings: (chartType: InteractiveChartType) => React.ReactNode;
  renderFallback: (messageText: string) => React.ReactNode;
}

export function PieChart({
  messageId,
  columnNames,
  numericColumns,
  nonNumericColumns,
  rowObjects,
  pieConfig,
  setPieConfig,
  showChartSettings,
  setShowChartSettings,
  hiddenPieLabels,
  setHiddenPieLabels,
  setChartTooltip,
  chartTooltipDescription,
  renderChartSettings,
  renderFallback,
}: PieChartProps) {
  const valueCol = pieConfig.valueCol || numericColumns[0];
  const labelCol = pieConfig.labelCol || nonNumericColumns[0] || columnNames[0];
  if (!valueCol || !labelCol) {
    return renderFallback("This result shape is not suitable for a pie chart.");
  }

  const allSlices = rowObjects
    .map((row) => ({
      label: String(row[labelCol] ?? ""),
      value: toNumber(row[valueCol]),
    }))
    .filter((row) => row.value !== null && row.value > 0)
    .slice(0, Math.max(2, pieConfig.maxItems)) as Array<{ label: string; value: number }>;
  const slices = allSlices.filter((slice) => !hiddenPieLabels.includes(slice.label));

  if (allSlices.length < 2) {
    return renderFallback("Not enough positive categories for a pie chart.");
  }
  if (slices.length < 1) {
    return renderFallback("All slices are hidden. Re-enable legend categories.");
  }

  const total = slices.reduce((sum, slice) => sum + slice.value, 0);
  if (total <= 0) {
    return renderFallback("Pie values are not positive.");
  }
  let cumulative = 0;
  const stops = slices.map((slice, index) => {
    const start = (cumulative / total) * 360;
    cumulative += slice.value;
    const end = (cumulative / total) * 360;
    const color = CHART_COLORS[index % CHART_COLORS.length];
    return `${color} ${start}deg ${end}deg`;
  });
  const pieSize = Math.max(120, Math.round(160 * pieConfig.zoom));

  return (
    <Card className="mt-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <BarChart3 size={16} />
          Pie Chart
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() => setShowChartSettings((prev) => !prev)}
            aria-pressed={showChartSettings}
            aria-label="Toggle pie chart settings"
          >
            <Settings2 size={12} />
            {showChartSettings ? "Hide settings" : "Chart settings"}
          </button>
          {hiddenPieLabels.length > 0 && (
            <button
              type="button"
              className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
              onClick={() => setHiddenPieLabels([])}
            >
              Reset hidden slices
            </button>
          )}
        </div>
        {renderChartSettings("pie_chart")}
        <div className="flex flex-wrap items-start gap-4">
          <div
            className="rounded-full border border-border"
            style={{ background: `conic-gradient(${stops.join(", ")})` }}
            role="img"
            aria-label={`Pie chart with ${slices.length} visible categories`}
            aria-describedby={chartTooltipDescription ? `${messageId}-viz-tooltip` : undefined}
            onMouseLeave={() => setChartTooltip(null)}
            onBlur={() => setChartTooltip(null)}
          >
            <div style={{ height: pieSize, width: pieSize }} />
          </div>
          {pieConfig.showLegend && (
            <ul className="flex-1 space-y-1 text-xs" aria-label="Pie chart legend">
              {allSlices.map((slice, index) => {
                const isHidden = hiddenPieLabels.includes(slice.label);
                return (
                  <li key={`${slice.label}-${index}`} className="flex items-center gap-2">
                    <button
                      type="button"
                      className="inline-flex w-full items-center gap-2 rounded border border-border px-2 py-1 text-left hover:bg-secondary"
                      aria-pressed={!isHidden}
                      onClick={() =>
                        setHiddenPieLabels((prev) =>
                          prev.includes(slice.label)
                            ? prev.filter((label) => label !== slice.label)
                            : [...prev, slice.label]
                        )
                      }
                      onMouseEnter={() =>
                        setChartTooltip({
                          title: slice.label,
                          detail: `${formatMetricNumber(slice.value)} (${((slice.value / (allSlices.reduce((sum, item) => sum + item.value, 0) || 1)) * 100).toFixed(1)}%)`,
                        })
                      }
                      onFocus={() =>
                        setChartTooltip({
                          title: slice.label,
                          detail: `${formatMetricNumber(slice.value)} (${((slice.value / (allSlices.reduce((sum, item) => sum + item.value, 0) || 1)) * 100).toFixed(1)}%)`,
                        })
                      }
                      onMouseLeave={() => setChartTooltip(null)}
                      onBlur={() => setChartTooltip(null)}
                    >
                      <span
                        className="inline-block h-2.5 w-2.5 rounded-full"
                        style={{ backgroundColor: CHART_COLORS[index % CHART_COLORS.length] }}
                      />
                      <span className={`flex-1 truncate ${isHidden ? "line-through opacity-60" : ""}`}>
                        {slice.label}
                      </span>
                      <span className={isHidden ? "opacity-60" : ""}>
                        {formatMetricNumber(slice.value)}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
        <p className="text-xs text-muted-foreground">
          Legend: {labelCol} categories · Slice value: {valueCol}
        </p>
        {chartTooltipDescription && (
          <p
            id={`${messageId}-viz-tooltip`}
            className="text-xs text-muted-foreground"
            aria-live="polite"
          >
            {chartTooltipDescription}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
