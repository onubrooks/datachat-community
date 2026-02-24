import type React from "react";
import { BarChart3, Settings2 } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  BarChartConfig,
  ChartTooltipState,
  InteractiveChartType,
} from "@/components/visualizations/types";
import {
  CHART_COLORS,
  clampPercent,
  formatMetricNumber,
  toNumber,
} from "@/components/visualizations/types";

interface BarChartProps {
  columnNames: string[];
  numericColumns: string[];
  nonNumericColumns: string[];
  rowObjects: Array<Record<string, unknown>>;
  barConfig: BarChartConfig;
  setBarConfig: React.Dispatch<React.SetStateAction<BarChartConfig>>;
  showChartSettings: boolean;
  setShowChartSettings: React.Dispatch<React.SetStateAction<boolean>>;
  setChartTooltip: React.Dispatch<React.SetStateAction<ChartTooltipState | null>>;
  chartTooltipDescription: string | null;
  renderChartSettings: (chartType: InteractiveChartType) => React.ReactNode;
  renderFallback: (messageText: string) => React.ReactNode;
}

export function BarChart({
  columnNames,
  numericColumns,
  nonNumericColumns,
  rowObjects,
  barConfig,
  setBarConfig,
  showChartSettings,
  setShowChartSettings,
  setChartTooltip,
  chartTooltipDescription,
  renderChartSettings,
  renderFallback,
}: BarChartProps) {
  const valueCol = barConfig.valueCol || numericColumns[0];
  const labelCol = barConfig.labelCol || nonNumericColumns[0] || columnNames[0];
  if (!valueCol || !labelCol) {
    return renderFallback("This result shape is not suitable for a bar chart.");
  }

  const points = rowObjects
    .map((row) => ({
      label: String(row[labelCol] ?? ""),
      value: toNumber(row[valueCol]),
    }))
    .filter((row) => row.value !== null)
    .slice(0, Math.max(2, barConfig.maxItems)) as Array<{ label: string; value: number }>;

  if (points.length < 2) {
    return renderFallback("Not enough points to draw a bar chart.");
  }

  const maxValue = Math.max(...points.map((row) => Math.abs(row.value)));
  if (maxValue <= 0) {
    return renderFallback("Bar values are all zero.");
  }

  return (
    <Card className="mt-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <BarChart3 size={16} />
          Bar Chart
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
            onClick={() => setShowChartSettings((prev) => !prev)}
            aria-pressed={showChartSettings}
            aria-label="Toggle bar chart settings"
          >
            <Settings2 size={12} />
            {showChartSettings ? "Hide settings" : "Chart settings"}
          </button>
        </div>
        {renderChartSettings("bar_chart")}
        {!barConfig.seriesVisible ? (
          <div className="rounded border border-dashed p-3 text-xs text-muted-foreground">
            The <code>{valueCol}</code> series is hidden. Re-enable it from the legend toggle.
          </div>
        ) : (
          points.map((point, index) => (
            <div key={`${point.label}-${index}`} className="flex items-center gap-2">
              <div className="w-28 truncate text-xs">{point.label}</div>
              <div className="h-4 flex-1 rounded bg-secondary overflow-hidden">
                <div
                  className="h-full origin-left bg-primary transition-transform"
                  style={{
                    width: `${clampPercent((Math.abs(point.value) / maxValue) * 100)}%`,
                    transform: `scaleX(${barConfig.zoom})`,
                  }}
                  role="img"
                  tabIndex={0}
                  aria-label={`${labelCol}: ${point.label}, ${valueCol}: ${formatMetricNumber(point.value)}`}
                  onMouseEnter={() =>
                    setChartTooltip({
                      title: point.label,
                      detail: `${valueCol}: ${formatMetricNumber(point.value)}`,
                    })
                  }
                  onMouseLeave={() => setChartTooltip(null)}
                  onFocus={() =>
                    setChartTooltip({
                      title: point.label,
                      detail: `${valueCol}: ${formatMetricNumber(point.value)}`,
                    })
                  }
                  onBlur={() => setChartTooltip(null)}
                />
              </div>
              <div className="w-16 text-right text-xs">{formatMetricNumber(point.value)}</div>
            </div>
          ))
        )}
        <div className="rounded border border-border/80 bg-muted/30 px-2 py-2 text-xs text-muted-foreground">
          <div>
            <span className="font-medium text-foreground">X axis:</span> {labelCol}
          </div>
          <div>
            <span className="font-medium text-foreground">Y axis:</span> {valueCol}
          </div>
          {barConfig.showLegend && (
            <div className="mt-1 flex items-center gap-2">
              <button
                type="button"
                className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-[11px] hover:bg-secondary"
                aria-pressed={!barConfig.seriesVisible}
                onClick={() =>
                  setBarConfig((prev) => ({
                    ...prev,
                    seriesVisible: !prev.seriesVisible,
                  }))
                }
              >
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ backgroundColor: CHART_COLORS[0] }}
                />
                {barConfig.seriesVisible ? "Hide" : "Show"} {valueCol}
              </button>
            </div>
          )}
        </div>
        {chartTooltipDescription && (
          <p className="text-xs text-muted-foreground" aria-live="polite">
            {chartTooltipDescription}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
