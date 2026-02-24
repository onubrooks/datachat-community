export type VizHint = "table" | "bar_chart" | "line_chart" | "pie_chart" | "scatter" | "none";
export type InteractiveChartType = "bar_chart" | "line_chart" | "pie_chart" | "scatter";

export interface BarChartConfig {
  labelCol: string;
  valueCol: string;
  maxItems: number;
  zoom: number;
  showLegend: boolean;
  seriesVisible: boolean;
}

export interface LineChartConfig {
  xCol: string;
  yCol: string;
  maxItems: number;
  zoom: number;
  showLegend: boolean;
  showGrid: boolean;
  seriesVisible: boolean;
}

export interface ScatterChartConfig {
  xCol: string;
  yCol: string;
  maxItems: number;
  zoom: number;
  showLegend: boolean;
  showGrid: boolean;
  seriesVisible: boolean;
}

export interface PieChartConfig {
  labelCol: string;
  valueCol: string;
  maxItems: number;
  zoom: number;
  showLegend: boolean;
}

export interface ChartTooltipState {
  title: string;
  detail?: string;
}

export const CHART_COLORS = [
  "#2563eb",
  "#16a34a",
  "#ea580c",
  "#9333ea",
  "#0891b2",
  "#dc2626",
  "#ca8a04",
  "#4f46e5",
];

export const formatMetricNumber = (value: number): string => {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: Math.abs(value) < 100 ? 2 : 0,
  }).format(value);
};

export const formatAxisTick = (value: number): string => {
  if (!Number.isFinite(value)) {
    return "0";
  }
  const abs = Math.abs(value);
  const maxFractionDigits = abs >= 1000 ? 0 : abs >= 100 ? 1 : 2;
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: maxFractionDigits,
  }).format(value);
};

export const toNumber = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
};

export const isDateLikeColumn = (columnName: string): boolean => {
  const lowered = columnName.toLowerCase();
  const tokens = lowered.split(/[^a-z0-9]+/).filter(Boolean);
  const directMarkers = new Set(["date", "time", "timestamp", "datetime"]);
  const periodMarkers = new Set(["day", "week", "month", "quarter", "year"]);
  const periodDisqualifiers = new Set(["type", "category", "name", "code"]);
  if (tokens.some((token) => directMarkers.has(token))) {
    return true;
  }
  if (
    tokens.some((token) => periodMarkers.has(token)) &&
    !tokens.some((token) => periodDisqualifiers.has(token))
  ) {
    return true;
  }
  return false;
};

export function clampPercent(value: number): number {
  return Math.max(0, Math.min(100, value));
}
