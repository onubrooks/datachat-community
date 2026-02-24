import type React from "react";
import { BarChart3, Settings2 } from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart } from "@/components/visualizations/BarChart";
import { LineChart } from "@/components/visualizations/LineChart";
import { PieChart } from "@/components/visualizations/PieChart";
import { ScatterChart } from "@/components/visualizations/ScatterChart";
import type {
  BarChartConfig,
  ChartTooltipState,
  InteractiveChartType,
  LineChartConfig,
  PieChartConfig,
  ScatterChartConfig,
  VizHint,
} from "@/components/visualizations/types";

interface ChartContainerProps {
  messageId: string;
  hasTable: boolean;
  resolvedVizHint: VizHint;
  columnNames: string[];
  numericColumns: string[];
  nonNumericColumns: string[];
  rowObjects: Array<Record<string, unknown>>;
  showChartSettings: boolean;
  setShowChartSettings: React.Dispatch<React.SetStateAction<boolean>>;
  chartTooltip: ChartTooltipState | null;
  setChartTooltip: React.Dispatch<React.SetStateAction<ChartTooltipState | null>>;
  hiddenPieLabels: string[];
  setHiddenPieLabels: React.Dispatch<React.SetStateAction<string[]>>;
  barConfig: BarChartConfig;
  setBarConfig: React.Dispatch<React.SetStateAction<BarChartConfig>>;
  lineConfig: LineChartConfig;
  setLineConfig: React.Dispatch<React.SetStateAction<LineChartConfig>>;
  scatterConfig: ScatterChartConfig;
  setScatterConfig: React.Dispatch<React.SetStateAction<ScatterChartConfig>>;
  pieConfig: PieChartConfig;
  setPieConfig: React.Dispatch<React.SetStateAction<PieChartConfig>>;
}

export function ChartContainer({
  messageId,
  hasTable,
  resolvedVizHint,
  columnNames,
  numericColumns,
  nonNumericColumns,
  rowObjects,
  showChartSettings,
  setShowChartSettings,
  chartTooltip,
  setChartTooltip,
  hiddenPieLabels,
  setHiddenPieLabels,
  barConfig,
  setBarConfig,
  lineConfig,
  setLineConfig,
  scatterConfig,
  setScatterConfig,
  pieConfig,
  setPieConfig,
}: ChartContainerProps) {
  if (!hasTable) {
    return (
      <Card className="mt-4">
        <CardContent className="pt-6 text-sm text-muted-foreground">
          No data returned for visualization.
        </CardContent>
      </Card>
    );
  }

  const interactiveChartType: InteractiveChartType | null =
    resolvedVizHint === "bar_chart" ||
    resolvedVizHint === "line_chart" ||
    resolvedVizHint === "scatter" ||
    resolvedVizHint === "pie_chart"
      ? resolvedVizHint
      : null;

  const chartTooltipDescription = chartTooltip?.detail
    ? `${chartTooltip.title} · ${chartTooltip.detail}`
    : chartTooltip?.title || null;

  const renderChartSettings = (chartType: InteractiveChartType) => {
    if (!showChartSettings) {
      return null;
    }

    const commonClassName = "h-8 rounded-md border border-input bg-background px-2 text-xs";

    if (chartType === "bar_chart") {
      return (
        <Card className="mt-2">
          <CardContent className="space-y-3 pt-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">X axis column</span>
                <select
                  className={commonClassName}
                  value={barConfig.labelCol}
                  aria-label="Bar chart X axis column"
                  onChange={(event) =>
                    setBarConfig((prev) => ({ ...prev, labelCol: event.target.value }))
                  }
                >
                  {columnNames.map((column) => (
                    <option key={`bar-label-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Y axis column</span>
                <select
                  className={commonClassName}
                  value={barConfig.valueCol}
                  aria-label="Bar chart Y axis column"
                  onChange={(event) =>
                    setBarConfig((prev) => ({ ...prev, valueCol: event.target.value }))
                  }
                >
                  {numericColumns.map((column) => (
                    <option key={`bar-value-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">Max bars: {barConfig.maxItems}</span>
                <input
                  type="range"
                  min={2}
                  max={50}
                  value={barConfig.maxItems}
                  aria-label="Bar chart max bars"
                  onChange={(event) =>
                    setBarConfig((prev) => ({ ...prev, maxItems: Number(event.target.value) }))
                  }
                />
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Zoom: {barConfig.zoom.toFixed(1)}x</span>
                <input
                  type="range"
                  min={1}
                  max={2}
                  step={0.1}
                  value={barConfig.zoom}
                  aria-label="Bar chart zoom"
                  onChange={(event) =>
                    setBarConfig((prev) => ({ ...prev, zoom: Number(event.target.value) }))
                  }
                />
              </label>
            </div>
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={barConfig.showLegend}
                  onChange={(event) =>
                    setBarConfig((prev) => ({ ...prev, showLegend: event.target.checked }))
                  }
                />
                Show legend
              </label>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (chartType === "line_chart") {
      return (
        <Card className="mt-2">
          <CardContent className="space-y-3 pt-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">X axis column</span>
                <select
                  className={commonClassName}
                  value={lineConfig.xCol}
                  aria-label="Line chart X axis column"
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, xCol: event.target.value }))
                  }
                >
                  {columnNames.map((column) => (
                    <option key={`line-x-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Y axis column</span>
                <select
                  className={commonClassName}
                  value={lineConfig.yCol}
                  aria-label="Line chart Y axis column"
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, yCol: event.target.value }))
                  }
                >
                  {numericColumns.map((column) => (
                    <option key={`line-y-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">Max points: {lineConfig.maxItems}</span>
                <input
                  type="range"
                  min={2}
                  max={120}
                  value={lineConfig.maxItems}
                  aria-label="Line chart max points"
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, maxItems: Number(event.target.value) }))
                  }
                />
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Zoom: {lineConfig.zoom.toFixed(1)}x</span>
                <input
                  type="range"
                  min={1}
                  max={3}
                  step={0.1}
                  value={lineConfig.zoom}
                  aria-label="Line chart zoom"
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, zoom: Number(event.target.value) }))
                  }
                />
              </label>
            </div>
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={lineConfig.showLegend}
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, showLegend: event.target.checked }))
                  }
                />
                Show legend
              </label>
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={lineConfig.showGrid}
                  onChange={(event) =>
                    setLineConfig((prev) => ({ ...prev, showGrid: event.target.checked }))
                  }
                />
                Show grid
              </label>
            </div>
          </CardContent>
        </Card>
      );
    }

    if (chartType === "scatter") {
      return (
        <Card className="mt-2">
          <CardContent className="space-y-3 pt-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">X axis column</span>
                <select
                  className={commonClassName}
                  value={scatterConfig.xCol}
                  aria-label="Scatter chart X axis column"
                  onChange={(event) =>
                    setScatterConfig((prev) => ({ ...prev, xCol: event.target.value }))
                  }
                >
                  {numericColumns.map((column) => (
                    <option key={`scatter-x-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Y axis column</span>
                <select
                  className={commonClassName}
                  value={scatterConfig.yCol}
                  aria-label="Scatter chart Y axis column"
                  onChange={(event) =>
                    setScatterConfig((prev) => ({ ...prev, yCol: event.target.value }))
                  }
                >
                  {numericColumns.map((column) => (
                    <option key={`scatter-y-${column}`} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-xs">
                <span className="font-medium">Max points: {scatterConfig.maxItems}</span>
                <input
                  type="range"
                  min={2}
                  max={300}
                  value={scatterConfig.maxItems}
                  aria-label="Scatter chart max points"
                  onChange={(event) =>
                    setScatterConfig((prev) => ({
                      ...prev,
                      maxItems: Number(event.target.value),
                    }))
                  }
                />
              </label>
              <label className="space-y-1 text-xs">
                <span className="font-medium">Zoom: {scatterConfig.zoom.toFixed(1)}x</span>
                <input
                  type="range"
                  min={1}
                  max={3}
                  step={0.1}
                  value={scatterConfig.zoom}
                  aria-label="Scatter chart zoom"
                  onChange={(event) =>
                    setScatterConfig((prev) => ({ ...prev, zoom: Number(event.target.value) }))
                  }
                />
              </label>
            </div>
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={scatterConfig.showLegend}
                  onChange={(event) =>
                    setScatterConfig((prev) => ({ ...prev, showLegend: event.target.checked }))
                  }
                />
                Show legend
              </label>
              <label className="inline-flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={scatterConfig.showGrid}
                  onChange={(event) =>
                    setScatterConfig((prev) => ({ ...prev, showGrid: event.target.checked }))
                  }
                />
                Show grid
              </label>
            </div>
          </CardContent>
        </Card>
      );
    }

    return (
      <Card className="mt-2">
        <CardContent className="space-y-3 pt-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-1 text-xs">
              <span className="font-medium">Category column</span>
              <select
                className={commonClassName}
                value={pieConfig.labelCol}
                aria-label="Pie chart category column"
                onChange={(event) =>
                  setPieConfig((prev) => ({ ...prev, labelCol: event.target.value }))
                }
              >
                {columnNames.map((column) => (
                  <option key={`pie-label-${column}`} value={column}>
                    {column}
                  </option>
                ))}
              </select>
            </label>
            <label className="space-y-1 text-xs">
              <span className="font-medium">Value column</span>
              <select
                className={commonClassName}
                value={pieConfig.valueCol}
                aria-label="Pie chart value column"
                onChange={(event) =>
                  setPieConfig((prev) => ({ ...prev, valueCol: event.target.value }))
                }
              >
                {numericColumns.map((column) => (
                  <option key={`pie-value-${column}`} value={column}>
                    {column}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-1 text-xs">
              <span className="font-medium">Max slices: {pieConfig.maxItems}</span>
              <input
                type="range"
                min={2}
                max={20}
                value={pieConfig.maxItems}
                aria-label="Pie chart max slices"
                onChange={(event) =>
                  setPieConfig((prev) => ({ ...prev, maxItems: Number(event.target.value) }))
                }
              />
            </label>
            <label className="space-y-1 text-xs">
              <span className="font-medium">Zoom: {pieConfig.zoom.toFixed(1)}x</span>
              <input
                type="range"
                min={1}
                max={2.4}
                step={0.1}
                value={pieConfig.zoom}
                aria-label="Pie chart zoom"
                onChange={(event) =>
                  setPieConfig((prev) => ({ ...prev, zoom: Number(event.target.value) }))
                }
              />
            </label>
          </div>
          <div className="flex flex-wrap items-center gap-3 text-xs">
            <label className="inline-flex items-center gap-2">
              <input
                type="checkbox"
                checked={pieConfig.showLegend}
                onChange={(event) =>
                  setPieConfig((prev) => ({ ...prev, showLegend: event.target.checked }))
                }
              />
              Show legend
            </label>
          </div>
        </CardContent>
      </Card>
    );
  };

  const renderFallback = (messageText: string) => (
    <Card className="mt-4">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm flex items-center gap-2">
          <BarChart3 size={16} />
          Visualization
        </CardTitle>
      </CardHeader>
      <CardContent>
        {interactiveChartType && (
          <div className="mb-3 flex flex-wrap items-center gap-2">
            <button
              type="button"
              className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
              onClick={() => setShowChartSettings((prev) => !prev)}
              aria-pressed={showChartSettings}
              aria-label="Toggle chart settings panel"
            >
              <Settings2 size={12} />
              {showChartSettings ? "Hide settings" : "Chart settings"}
            </button>
          </div>
        )}
        {interactiveChartType && renderChartSettings(interactiveChartType)}
        <p className="text-sm text-muted-foreground">{messageText}</p>
        <p className="mt-2 text-xs text-muted-foreground">
          Tip: switch to the Table tab for raw results.
        </p>
      </CardContent>
    </Card>
  );

  if (resolvedVizHint === "none" || resolvedVizHint === "table") {
    return renderFallback("This query is better represented as a table.");
  }
  if (resolvedVizHint === "bar_chart") {
    return (
      <BarChart
        columnNames={columnNames}
        numericColumns={numericColumns}
        nonNumericColumns={nonNumericColumns}
        rowObjects={rowObjects}
        barConfig={barConfig}
        setBarConfig={setBarConfig}
        showChartSettings={showChartSettings}
        setShowChartSettings={setShowChartSettings}
        setChartTooltip={setChartTooltip}
        chartTooltipDescription={chartTooltipDescription}
        renderChartSettings={renderChartSettings}
        renderFallback={renderFallback}
      />
    );
  }
  if (resolvedVizHint === "line_chart") {
    return (
      <LineChart
        columnNames={columnNames}
        numericColumns={numericColumns}
        nonNumericColumns={nonNumericColumns}
        rowObjects={rowObjects}
        lineConfig={lineConfig}
        setLineConfig={setLineConfig}
        showChartSettings={showChartSettings}
        setShowChartSettings={setShowChartSettings}
        setChartTooltip={setChartTooltip}
        chartTooltipDescription={chartTooltipDescription}
        renderChartSettings={renderChartSettings}
        renderFallback={renderFallback}
      />
    );
  }
  if (resolvedVizHint === "scatter") {
    return (
      <ScatterChart
        numericColumns={numericColumns}
        rowObjects={rowObjects}
        scatterConfig={scatterConfig}
        setScatterConfig={setScatterConfig}
        showChartSettings={showChartSettings}
        setShowChartSettings={setShowChartSettings}
        setChartTooltip={setChartTooltip}
        chartTooltipDescription={chartTooltipDescription}
        renderChartSettings={renderChartSettings}
        renderFallback={renderFallback}
      />
    );
  }
  if (resolvedVizHint === "pie_chart") {
    return (
      <PieChart
        messageId={messageId}
        columnNames={columnNames}
        numericColumns={numericColumns}
        nonNumericColumns={nonNumericColumns}
        rowObjects={rowObjects}
        pieConfig={pieConfig}
        setPieConfig={setPieConfig}
        showChartSettings={showChartSettings}
        setShowChartSettings={setShowChartSettings}
        hiddenPieLabels={hiddenPieLabels}
        setHiddenPieLabels={setHiddenPieLabels}
        setChartTooltip={setChartTooltip}
        chartTooltipDescription={chartTooltipDescription}
        renderChartSettings={renderChartSettings}
        renderFallback={renderFallback}
      />
    );
  }
  return renderFallback("No suitable visualization is available.");
}
