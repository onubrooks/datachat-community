/**
 * Message Component
 *
 * Displays a single chat message with support for:
 * - User and assistant messages
 * - SQL code blocks
 * - Data tables
 * - Source citations
 * - Performance metrics
 * - Optional tabbed result layout + visualization
 */

"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  User,
  Bot,
  Code,
  Table as TableIcon,
  BookOpen,
  Clock,
  BadgeCheck,
  Copy,
  Download,
  Link2,
  ThumbsUp,
  ThumbsDown,
  Flag,
  Lightbulb,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "../ui/card";
import { cn } from "@/lib/utils";
import type { Message as MessageType } from "@/lib/stores/chat";
import type { ResultLayoutMode } from "@/lib/settings";
import { buildShareUrl } from "@/lib/share";
import { ChartContainer } from "@/components/visualizations/ChartContainer";
import {
  isDateLikeColumn,
  toNumber,
  type BarChartConfig,
  type ChartTooltipState,
  type LineChartConfig,
  type PieChartConfig,
  type VizHint,
  type ScatterChartConfig,
} from "@/components/visualizations/types";

interface MessageProps {
  message: MessageType;
  displayMode?: ResultLayoutMode;
  showAgentTimingBreakdown?: boolean;
  onClarifyingAnswer?: (question: string) => void;
  onEditSqlDraft?: (sql: string) => void;
  onSubmitFeedback?: (payload: MessageFeedbackPayload) => Promise<void>;
}

export interface MessageFeedbackPayload {
  category: "answer_feedback" | "issue_report" | "improvement_suggestion";
  sentiment?: "up" | "down" | null;
  message?: string | null;
  message_id: string;
  answer_source?: string | null;
  answer_confidence?: number | null;
  answer?: string | null;
  sql?: string | null;
  sources?: Array<Record<string, unknown>>;
}

type TabId = "answer" | "sql" | "table" | "visualization" | "sources" | "timing";
type SubAnswer = NonNullable<MessageType["sub_answers"]>[number];

function renderInlineMarkdown(text: string): React.ReactNode[] {
  const tokens = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).filter(Boolean);
  return tokens.map((token, index) => {
    if (token.startsWith("**") && token.endsWith("**") && token.length >= 4) {
      return <strong key={`b-${index}`}>{token.slice(2, -2)}</strong>;
    }
    if (token.startsWith("`") && token.endsWith("`") && token.length >= 3) {
      return (
        <code
          key={`c-${index}`}
          className="rounded bg-secondary/70 px-1 py-0.5 text-xs"
        >
          {token.slice(1, -1)}
        </code>
      );
    }
    return <React.Fragment key={`t-${index}`}>{token}</React.Fragment>;
  });
}

function renderMarkdownish(text: string): React.ReactNode {
  if (!text) {
    return null;
  }

  const lines = text.replace(/\r\n/g, "\n").split("\n");
  const blocks: React.ReactNode[] = [];
  const listItems: string[] = [];
  let paragraphBuffer: string[] = [];

  const flushParagraph = () => {
    if (!paragraphBuffer.length) {
      return;
    }
    const paragraph = paragraphBuffer.join("\n").trim();
    paragraphBuffer = [];
    if (!paragraph) {
      return;
    }
    blocks.push(
      <p key={`p-${blocks.length}`} className="whitespace-pre-wrap leading-relaxed">
        {renderInlineMarkdown(paragraph)}
      </p>
    );
  };

  const flushList = () => {
    if (!listItems.length) {
      return;
    }
    blocks.push(
      <ul key={`l-${blocks.length}`} className="list-disc space-y-1 pl-5">
        {listItems.map((item, index) => (
          <li key={`li-${index}`}>{renderInlineMarkdown(item)}</li>
        ))}
      </ul>
    );
    listItems.length = 0;
  };

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    const bulletMatch = line.match(/^\s*[*-]\s+(.+)$/);

    if (bulletMatch) {
      flushParagraph();
      listItems.push(bulletMatch[1].trim());
      continue;
    }

    if (line.trim() === "") {
      flushParagraph();
      flushList();
      continue;
    }

    flushList();
    paragraphBuffer.push(line);
  }

  flushParagraph();
  flushList();

  return <div className="space-y-2">{blocks}</div>;
}

const normalizeAnswerText = (value: string): string =>
  value
    .toLowerCase()
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/[*_`>#]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const formatDurationSeconds = (milliseconds: number): string => {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }
  const seconds = milliseconds / 1000;
  const maximumFractionDigits = seconds >= 10 ? 1 : 2;
  return `${new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits,
  }).format(seconds)}s`;
};

const toTabularRows = (data?: Record<string, unknown[]> | null) => {
  if (!data) {
    return { columns: [] as string[], rowCount: 0, rows: [] as unknown[][] };
  }
  const columns = Object.keys(data);
  if (!columns.length) {
    return { columns, rowCount: 0, rows: [] as unknown[][] };
  }
  const rowCount = Math.max(
    ...columns.map((column) =>
      Array.isArray(data[column]) ? data[column].length : 0
    )
  );
  const rows = Array.from({ length: rowCount }, (_, rowIndex) =>
    columns.map((column) => data[column]?.[rowIndex])
  );
  return { columns, rowCount, rows };
};

const inferVisualizationTypeForData = (
  data: Record<string, unknown[]> | null | undefined,
  hint: string | null | undefined
): VizHint => {
  const normalizedHint = (hint || "").toLowerCase();
  if (
    normalizedHint === "bar_chart" ||
    normalizedHint === "line_chart" ||
    normalizedHint === "pie_chart" ||
    normalizedHint === "scatter" ||
    normalizedHint === "table" ||
    normalizedHint === "none"
  ) {
    return normalizedHint;
  }

  const { columns, rowCount, rows } = toTabularRows(data);
  if (!columns.length || rowCount === 0) {
    return "none";
  }
  const numericColumns = columns.filter((column) =>
    rows.some((row) => toNumber(row[columns.indexOf(column)]) !== null)
  );

  if (numericColumns.length >= 2 && rowCount <= 200) {
    return "scatter";
  }
  if (numericColumns.length >= 1 && columns.some((column) => isDateLikeColumn(column))) {
    return "line_chart";
  }
  if (numericColumns.length >= 1 && rowCount <= 20) {
    return "bar_chart";
  }
  return "table";
};

const formatVizHintLabel = (hint: VizHint): string => {
  if (hint === "bar_chart") {
    return "Bar chart";
  }
  if (hint === "line_chart") {
    return "Line chart";
  }
  if (hint === "pie_chart") {
    return "Pie chart";
  }
  if (hint === "scatter") {
    return "Scatter plot";
  }
  if (hint === "table") {
    return "Table";
  }
  return "None";
};

export function Message({
  message,
  displayMode = "stacked",
  showAgentTimingBreakdown = true,
  onClarifyingAnswer,
  onEditSqlDraft,
  onSubmitFeedback,
}: MessageProps) {
  const isUser = message.role === "user";
  const [activeTab, setActiveTab] = useState<TabId>("answer");
  const [actionNotice, setActionNotice] = useState<string | null>(null);
  const [selectedSubAnswerIndex, setSelectedSubAnswerIndex] = useState<number>(0);
  const [tablePage, setTablePage] = useState<number>(0);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [showChartSettings, setShowChartSettings] = useState(false);
  const [feedbackMode, setFeedbackMode] = useState<"issue" | "suggestion" | null>(null);
  const [feedbackDraft, setFeedbackDraft] = useState("");
  const [feedbackSubmitting, setFeedbackSubmitting] = useState(false);
  const [feedbackSentiment, setFeedbackSentiment] = useState<"up" | "down" | null>(null);
  const [chartTooltip, setChartTooltip] = useState<ChartTooltipState | null>(null);
  const [hiddenPieLabels, setHiddenPieLabels] = useState<string[]>([]);
  const [barConfig, setBarConfig] = useState<BarChartConfig>({
    labelCol: "",
    valueCol: "",
    maxItems: 12,
    zoom: 1,
    showLegend: true,
    seriesVisible: true,
  });
  const [lineConfig, setLineConfig] = useState<LineChartConfig>({
    xCol: "",
    yCol: "",
    maxItems: 30,
    zoom: 1,
    showLegend: true,
    showGrid: true,
    seriesVisible: true,
  });
  const [scatterConfig, setScatterConfig] = useState<ScatterChartConfig>({
    xCol: "",
    yCol: "",
    maxItems: 120,
    zoom: 1,
    showLegend: true,
    showGrid: true,
    seriesVisible: true,
  });
  const [pieConfig, setPieConfig] = useState<PieChartConfig>({
    labelCol: "",
    valueCol: "",
    maxItems: 8,
    zoom: 1,
    showLegend: true,
  });
  const tabButtonRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const tabListId = `message-tablist-${message.id}`;
  const activeTabPanelId = `${message.id}-panel-${activeTab}`;

  const subAnswers = useMemo(() => message.sub_answers || [], [message.sub_answers]);
  const activeSubAnswer =
    subAnswers.length > 0
      ? subAnswers[Math.min(selectedSubAnswerIndex, subAnswers.length - 1)]
      : null;
  const activeContent = activeSubAnswer?.answer || message.content;
  const activeSql = activeSubAnswer?.sql ?? message.sql;
  const activeData = activeSubAnswer?.data ?? message.data;
  const activeVisualizationHint =
    activeSubAnswer?.visualization_hint ?? message.visualization_hint;
  const activeVisualizationMetadata =
    activeSubAnswer?.visualization_metadata ?? message.visualization_metadata;
  const activeClarifyingQuestions =
    activeSubAnswer?.clarifying_questions || message.clarifying_questions;
  const workflowArtifacts = message.workflow_artifacts;

  useEffect(() => {
    if (subAnswers.length === 0) {
      setSelectedSubAnswerIndex(0);
      return;
    }
    if (selectedSubAnswerIndex > subAnswers.length - 1) {
      setSelectedSubAnswerIndex(0);
    }
  }, [selectedSubAnswerIndex, subAnswers.length]);

  useEffect(() => {
    setTablePage(0);
    setRowsPerPage(10);
  }, [message.id, selectedSubAnswerIndex]);

  useEffect(() => {
    setShowChartSettings(false);
    setChartTooltip(null);
    setHiddenPieLabels([]);
    setFeedbackMode(null);
    setFeedbackDraft("");
    setFeedbackSubmitting(false);
    setFeedbackSentiment(null);
  }, [message.id, selectedSubAnswerIndex]);

  const columnNames = useMemo(
    () => (activeData ? Object.keys(activeData) : []),
    [activeData]
  );
  const rowCount =
    columnNames.length > 0
      ? Math.max(...columnNames.map((column) => activeData?.[column]?.length ?? 0))
      : 0;

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(rowCount / rowsPerPage));
    setTablePage((page) => Math.min(page, totalPages - 1));
  }, [rowCount, rowsPerPage]);

  const rows = useMemo(
    () =>
      Array.from({ length: rowCount }, (_, rowIndex) =>
        columnNames.map((column) => activeData?.[column]?.[rowIndex])
      ),
    [activeData, columnNames, rowCount]
  );

  const rowObjects = useMemo(
    () =>
      Array.from({ length: rowCount }, (_, rowIndex) => {
        const record: Record<string, unknown> = {};
        for (const column of columnNames) {
          record[column] = activeData?.[column]?.[rowIndex];
        }
        return record;
      }),
    [activeData, columnNames, rowCount]
  );

  const numericColumns = useMemo(
    () =>
      columnNames.filter((column) =>
        rowObjects.some((row) => toNumber(row[column]) !== null)
      ),
    [columnNames, rowObjects]
  );

  const nonNumericColumns = useMemo(
    () => columnNames.filter((column) => !numericColumns.includes(column)),
    [columnNames, numericColumns]
  );

  useEffect(() => {
    const defaultBarLabel = nonNumericColumns[0] || columnNames[0] || "";
    const defaultBarValue = numericColumns[0] || "";
    const defaultLineX =
      columnNames.find((column) => isDateLikeColumn(column)) ||
      nonNumericColumns[0] ||
      columnNames[0] ||
      "";
    const defaultLineY = numericColumns[0] || "";
    const defaultScatterX = numericColumns[0] || "";
    const defaultScatterY = numericColumns[1] || numericColumns[0] || "";
    const defaultPieLabel = nonNumericColumns[0] || columnNames[0] || "";
    const defaultPieValue = numericColumns[0] || "";

    setBarConfig((prev) => ({
      ...prev,
      labelCol:
        prev.labelCol && columnNames.includes(prev.labelCol)
          ? prev.labelCol
          : defaultBarLabel,
      valueCol:
        prev.valueCol && numericColumns.includes(prev.valueCol)
          ? prev.valueCol
          : defaultBarValue,
    }));

    setLineConfig((prev) => ({
      ...prev,
      xCol:
        prev.xCol && columnNames.includes(prev.xCol)
          ? prev.xCol
          : defaultLineX,
      yCol:
        prev.yCol && numericColumns.includes(prev.yCol)
          ? prev.yCol
          : defaultLineY,
    }));

    setScatterConfig((prev) => ({
      ...prev,
      xCol:
        prev.xCol && numericColumns.includes(prev.xCol)
          ? prev.xCol
          : defaultScatterX,
      yCol:
        prev.yCol &&
        numericColumns.includes(prev.yCol) &&
        (!prev.xCol || prev.yCol !== prev.xCol || numericColumns.length === 1)
          ? prev.yCol
          : defaultScatterY,
    }));

    setPieConfig((prev) => ({
      ...prev,
      labelCol:
        prev.labelCol && columnNames.includes(prev.labelCol)
          ? prev.labelCol
          : defaultPieLabel,
      valueCol:
        prev.valueCol && numericColumns.includes(prev.valueCol)
          ? prev.valueCol
          : defaultPieValue,
    }));
  }, [columnNames, nonNumericColumns, numericColumns]);

  useEffect(() => {
    if (!actionNotice) {
      return;
    }
    const timeout = window.setTimeout(() => setActionNotice(null), 2000);
    return () => window.clearTimeout(timeout);
  }, [actionNotice]);

  const formatCellValue = (value: unknown) => {
    if (value === null || value === undefined) {
      return { display: "", full: "", truncated: false };
    }
    const full = typeof value === "string" ? value : JSON.stringify(value);
    const maxLength = 160;
    if (full.length <= maxLength) {
      return { display: full, full, truncated: false };
    }
    return {
      display: `${full.slice(0, maxLength)}…`,
      full,
      truncated: true,
    };
  };

  const hasAnySubAnswerSql = useMemo(
    () => subAnswers.some((item) => Boolean(item.sql?.trim())),
    [subAnswers]
  );
  const hasAnySubAnswerTable = useMemo(
    () =>
      subAnswers.some((item) => {
        const { rowCount: subRowCount } = toTabularRows(item.data ?? null);
        return subRowCount > 0;
      }),
    [subAnswers]
  );
  const activeHasTable = Boolean(activeData) && rowCount > 0;
  const hasSources =
    Boolean(message.sources?.length) && message.answer_source !== "context";
  const hasEvidence = Boolean(message.evidence?.length);
  const hasSql = Boolean(activeSql?.trim()) || hasAnySubAnswerSql;
  const hasTable = activeHasTable || hasAnySubAnswerTable;
  const hasAgentTimings = Boolean(
    message.metrics?.agent_timings &&
      Object.keys(message.metrics.agent_timings).length > 0 &&
      showAgentTimingBreakdown
  );
  const hasLoopTelemetry = Boolean(
    message.loop_terminal_state ||
      message.loop_stop_reason ||
      (message.action_trace?.length ?? 0) > 0 ||
      (message.loop_shadow_decisions?.length ?? 0) > 0
  );
  const hasWorkflowArtifacts =
    !isUser &&
    Boolean(workflowArtifacts) &&
    (workflowArtifacts?.metrics.length ||
      workflowArtifacts?.drivers.length ||
      workflowArtifacts?.caveats.length ||
      workflowArtifacts?.sources.length ||
      workflowArtifacts?.follow_ups.length);

  const inferVisualizationType = (): VizHint => {
    const hint = (activeVisualizationHint || "").toLowerCase();
    if (
      hint === "bar_chart" ||
      hint === "line_chart" ||
      hint === "pie_chart" ||
      hint === "scatter" ||
      hint === "table" ||
      hint === "none"
    ) {
      return hint;
    }
    if (!hasTable) {
      return "none";
    }
    if (numericColumns.length >= 2 && rowCount <= 200) {
      return "scatter";
    }
    if (numericColumns.length >= 1 && columnNames.some((col) => isDateLikeColumn(col))) {
      return "line_chart";
    }
    if (numericColumns.length >= 1 && rowCount <= 20) {
      return "bar_chart";
    }
    return "table";
  };

  const resolvedVizHint = inferVisualizationType();

  const copySql = async () => {
    if (!activeSql) {
      return;
    }
    try {
      await navigator.clipboard.writeText(activeSql);
      setActionNotice("SQL copied");
    } catch {
      setActionNotice("Unable to copy SQL");
    }
  };

  const downloadCsv = () => {
    if (!activeHasTable) {
      return;
    }
    const escapeCsv = (value: unknown) => {
      if (value === null || value === undefined) {
        return "";
      }
      const text = String(value);
      if (text.includes(",") || text.includes('"') || text.includes("\n")) {
        return `"${text.replace(/"/g, '""')}"`;
      }
      return text;
    };

    const header = columnNames.join(",");
    const body = rowObjects
      .map((row) => columnNames.map((column) => escapeCsv(row[column])).join(","))
      .join("\n");
    const csv = `${header}\n${body}`;
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "datachat-results.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setActionNotice("CSV downloaded");
  };

  const downloadJson = () => {
    if (!activeHasTable) {
      return;
    }
    const payload = {
      columns: columnNames,
      rows: rowObjects,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: "application/json;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "datachat-results.json";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setActionNotice("JSON downloaded");
  };

  const copyMarkdownTable = async () => {
    if (!activeHasTable) {
      return;
    }
    const escapeMarkdown = (value: unknown) => {
      if (value === null || value === undefined) {
        return "";
      }
      return String(value).replace(/\|/g, "\\|").replace(/\r?\n/g, "<br/>");
    };
    const header = `| ${columnNames.join(" | ")} |`;
    const separator = `| ${columnNames.map(() => "---").join(" | ")} |`;
    const rowsMarkdown = rowObjects.map(
      (row) =>
        `| ${columnNames.map((column) => escapeMarkdown(row[column])).join(" | ")} |`
    );
    const markdown = [header, separator, ...rowsMarkdown].join("\n");
    try {
      await navigator.clipboard.writeText(markdown);
      setActionNotice("Markdown copied");
    } catch {
      setActionNotice("Unable to copy markdown");
    }
  };

  const copyShareLink = async () => {
    try {
      const shareUrl = buildShareUrl(
        {
          created_at: new Date().toISOString(),
          answer: activeContent || "",
          sql: activeSql ?? null,
          data: activeHasTable ? activeData ?? null : null,
          visualization_hint: resolvedVizHint,
          visualization_metadata:
            (activeVisualizationMetadata as Record<string, unknown> | null) ?? null,
          sources: message.sources || [],
          answer_source: message.answer_source ?? null,
          answer_confidence:
            typeof message.answer_confidence === "number" ? message.answer_confidence : null,
        },
        window.location.href
      );
      await navigator.clipboard.writeText(shareUrl);
      setActionNotice("Share link copied");
    } catch {
      setActionNotice("Unable to copy share link");
    }
  };

  const submitFeedback = async (
    category: "answer_feedback" | "issue_report" | "improvement_suggestion",
    sentiment?: "up" | "down" | null,
    messageText?: string | null
  ) => {
    if (!onSubmitFeedback) {
      setActionNotice("Feedback capture is not enabled");
      return;
    }
    setFeedbackSubmitting(true);
    try {
      const feedbackAnswerSource =
        activeSubAnswer?.answer_source ?? message.answer_source ?? null;
      const feedbackAnswerConfidence =
        typeof activeSubAnswer?.answer_confidence === "number"
          ? activeSubAnswer.answer_confidence
          : typeof message.answer_confidence === "number"
            ? message.answer_confidence
            : null;
      await onSubmitFeedback({
        category,
        sentiment: sentiment ?? null,
        message: messageText ?? null,
        message_id: message.id,
        answer_source: feedbackAnswerSource,
        answer_confidence: feedbackAnswerConfidence,
        answer: activeContent || null,
        sql: activeSql ?? null,
        sources: (message.sources || []) as Array<Record<string, unknown>>,
      });
      if (category === "answer_feedback") {
        setFeedbackSentiment(sentiment ?? null);
        setActionNotice(sentiment === "up" ? "Thanks for the positive feedback" : "Feedback noted");
      } else if (category === "issue_report") {
        setFeedbackMode(null);
        setFeedbackDraft("");
        setActionNotice("Issue report submitted");
      } else {
        setFeedbackMode(null);
        setFeedbackDraft("");
        setActionNotice("Improvement suggestion submitted");
      }
    } catch {
      setActionNotice("Unable to submit feedback");
    } finally {
      setFeedbackSubmitting(false);
    }
  };

  const renderClarifyingQuestions = () => {
    if (!activeClarifyingQuestions || activeClarifyingQuestions.length === 0) {
      return null;
    }
    return (
      <Card className="mt-4">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Clarifying questions</CardTitle>
        </CardHeader>
        <CardContent>
          <ul className="space-y-2 text-sm">
            {activeClarifyingQuestions.map((question, index) => (
              <li key={`${question}-${index}`} className="flex items-start gap-2">
                <span className="mt-0.5 flex-1">• {question}</span>
                {onClarifyingAnswer && (
                  <button
                    type="button"
                    className="text-xs text-primary underline"
                    onClick={() => onClarifyingAnswer(question)}
                    aria-label={`Answer clarifying question: ${question}`}
                  >
                    Answer
                  </button>
                )}
              </li>
            ))}
          </ul>
        </CardContent>
      </Card>
    );
  };

  const renderSqlSection = () => {
    if (!hasSql) {
      return (
        <Card className="mt-4">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No SQL generated for this answer.
          </CardContent>
        </Card>
      );
    }

    if (subAnswers.length > 1) {
      return (
        <Card className="mt-4">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <Code size={16} />
              Generated SQL by sub-question
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {subAnswers.map((item, idx) => {
              const subSql = item.sql?.trim() || "";
              return (
                <details
                  key={`sub-sql-${item.index}-${idx}`}
                  open={idx === selectedSubAnswerIndex}
                  className="rounded border border-border/80 bg-muted/10"
                >
                  <summary
                    className="cursor-pointer px-3 py-2 text-sm font-medium"
                    onClick={() => setSelectedSubAnswerIndex(idx)}
                  >
                    Q{item.index}: {item.query}
                  </summary>
                  <div className="px-3 pb-3">
                    {subSql ? (
                      <pre className="bg-secondary p-3 rounded text-sm overflow-x-auto">
                        <code>{subSql}</code>
                      </pre>
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No SQL was generated for this sub-question.
                      </p>
                    )}
                  </div>
                </details>
              );
            })}
          </CardContent>
        </Card>
      );
    }

    if (!activeSql) {
      return (
        <Card className="mt-4">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No SQL generated for this answer.
          </CardContent>
        </Card>
      );
    }

    return (
      <Card className="mt-4">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Code size={16} />
            Generated SQL
          </CardTitle>
        </CardHeader>
        <CardContent>
          <pre className="bg-secondary p-3 rounded text-sm overflow-x-auto">
            <code>{activeSql}</code>
          </pre>
        </CardContent>
      </Card>
    );
  };

  const renderTableSection = () => {
    if (!hasTable) {
      return (
        <Card className="mt-4">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No tabular data returned.
          </CardContent>
        </Card>
      );
    }

    const totalPages = Math.ceil(rowCount / rowsPerPage);
    const startRow = tablePage * rowsPerPage;
    const endRow = Math.min(startRow + rowsPerPage, rowCount);
    const pageRows = rows.slice(startRow, endRow);

    const handlePrevPage = () => {
      setTablePage((p) => Math.max(0, p - 1));
    };

    const handleNextPage = () => {
      setTablePage((p) => Math.min(totalPages - 1, p + 1));
    };

    if (subAnswers.length > 1) {
      return (
        <Card className="mt-4">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <TableIcon size={16} />
              Result tables by sub-question
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {subAnswers.map((item, idx) => {
              const { columns, rowCount: subRowCount, rows: subRows } = toTabularRows(item.data ?? null);
              const previewRows = subRows.slice(0, 10);
              const isActive = idx === selectedSubAnswerIndex;
              return (
                <details
                  key={`sub-table-${item.index}-${idx}`}
                  open={isActive}
                  className="rounded border border-border/80 bg-muted/10"
                >
                  <summary
                    className="cursor-pointer px-3 py-2 text-sm font-medium"
                    onClick={() => setSelectedSubAnswerIndex(idx)}
                  >
                    Q{item.index}: {item.query}{" "}
                    <span className="text-xs font-normal text-muted-foreground">
                      ({subRowCount} rows)
                    </span>
                  </summary>
                  <div className="px-3 pb-3">
                    {subRowCount > 0 ? (
                      isActive ? (
                        <>
                          <div className="mb-2 text-xs text-muted-foreground">
                            Showing {startRow + 1}-{endRow} of {rowCount} rows
                          </div>
                          <div className="overflow-x-auto">
                            <table className="w-full text-sm" aria-label={`Results for sub-question ${item.index}`}>
                              <thead>
                                <tr className="border-b">
                                  {columnNames.map((column) => (
                                    <th key={`sub-table-head-active-${item.index}-${column}`} className="text-left p-2 font-medium">
                                      {column}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {pageRows.map((row, rowIdx) => (
                                  <tr key={`sub-table-row-active-${item.index}-${rowIdx}`} className="border-b last:border-0">
                                    {row.map((value, valueIdx) => {
                                      const { display, full, truncated } = formatCellValue(value);
                                      return (
                                        <td key={`sub-table-cell-active-${item.index}-${rowIdx}-${valueIdx}`} className="p-2 align-top">
                                          <span
                                            className={truncated ? "block max-w-[320px] truncate" : "block"}
                                            title={truncated ? full : undefined}
                                          >
                                            {display}
                                          </span>
                                        </td>
                                      );
                                    })}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          {totalPages > 1 && (
                            <div className="flex items-center justify-between mt-3 pt-3 border-t">
                              <p className="text-xs text-muted-foreground">
                                Page {tablePage + 1} of {totalPages}
                              </p>
                              <div className="flex items-center gap-2">
                                <button
                                  type="button"
                                  onClick={handlePrevPage}
                                  disabled={tablePage === 0}
                                  className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary disabled:opacity-50 disabled:cursor-not-allowed"
                                  aria-label="Go to previous result page"
                                >
                                  <ChevronLeft size={14} />
                                  Previous
                                </button>
                                <button
                                  type="button"
                                  onClick={handleNextPage}
                                  disabled={tablePage >= totalPages - 1}
                                  className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary disabled:opacity-50 disabled:cursor-not-allowed"
                                  aria-label="Go to next result page"
                                >
                                  Next
                                  <ChevronRight size={14} />
                                </button>
                              </div>
                            </div>
                          )}
                        </>
                      ) : (
                        <>
                          <div className="mb-2 text-xs text-muted-foreground">
                            Showing {Math.min(10, subRowCount)} of {subRowCount} rows
                          </div>
                          <div className="overflow-x-auto">
                            <table className="w-full text-sm" aria-label={`Results for sub-question ${item.index}`}>
                              <thead>
                                <tr className="border-b">
                                  {columns.map((column) => (
                                    <th key={`sub-table-head-${item.index}-${column}`} className="text-left p-2 font-medium">
                                      {column}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {previewRows.map((row, rowIdx) => (
                                  <tr key={`sub-table-row-${item.index}-${rowIdx}`} className="border-b last:border-0">
                                    {row.map((value, valueIdx) => {
                                      const { display, full, truncated } = formatCellValue(value);
                                      return (
                                        <td key={`sub-table-cell-${item.index}-${rowIdx}-${valueIdx}`} className="p-2 align-top">
                                          <span
                                            className={truncated ? "block max-w-[320px] truncate" : "block"}
                                            title={truncated ? full : undefined}
                                          >
                                            {display}
                                          </span>
                                        </td>
                                      );
                                    })}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </>
                      )
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No tabular data returned for this sub-question.
                      </p>
                    )}
                  </div>
                </details>
              );
            })}
          </CardContent>
        </Card>
      );
    }

    return (
      <Card className="mt-4">
        <details>
          <summary className="cursor-pointer list-none px-6 py-4" aria-label="Toggle result table">
            <div className="flex items-center justify-between gap-3">
              <CardTitle className="text-sm flex items-center gap-2">
                <TableIcon size={16} />
                Results ({rowCount} rows)
              </CardTitle>
              <span className="text-xs text-muted-foreground">Expand</span>
            </div>
          </summary>
          <CardContent className="pt-0">
            <div className="overflow-x-auto">
              <table className="w-full text-sm" aria-label="Query result table">
                <caption className="sr-only">Query results</caption>
                <thead>
                  <tr className="border-b">
                    {columnNames.map((key) => (
                      <th key={key} className="text-left p-2 font-medium">
                        {key}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row, idx) => (
                    <tr key={startRow + idx} className="border-b last:border-0">
                      {row.map((value, vidx) => {
                        const { display, full, truncated } = formatCellValue(value);
                        return (
                          <td key={vidx} className="p-2 align-top">
                            <span
                              className={truncated ? "block max-w-[320px] truncate" : "block"}
                              title={truncated ? full : undefined}
                            >
                              {display}
                            </span>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-3 pt-3 border-t">
                <div className="flex items-center gap-3">
                  <p className="text-xs text-muted-foreground">
                    Showing {startRow + 1}-{endRow} of {rowCount} rows
                  </p>
                  <label className="flex items-center gap-1 text-xs text-muted-foreground">
                    Rows per page
                    <input
                      type="number"
                      min={1}
                      step={1}
                      value={rowsPerPage}
                      onChange={(event) => {
                        const value = Number(event.target.value);
                        if (!Number.isFinite(value) || value < 1) {
                          return;
                        }
                        setRowsPerPage(Math.floor(value));
                      }}
                      className="h-7 w-20 rounded border border-input bg-background px-2 text-xs"
                      aria-label="Rows per page"
                    />
                  </label>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={handlePrevPage}
                    disabled={tablePage === 0}
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Go to previous result page"
                  >
                    <ChevronLeft size={14} />
                    Previous
                  </button>
                  <span className="text-xs text-muted-foreground">
                    Page {tablePage + 1} of {totalPages}
                  </span>
                  <button
                    type="button"
                    onClick={handleNextPage}
                    disabled={tablePage >= totalPages - 1}
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Go to next result page"
                  >
                    Next
                    <ChevronRight size={14} />
                  </button>
                </div>
              </div>
            )}
            {totalPages === 1 && rowCount > 0 && (
              <div className="mt-2 flex items-center gap-3">
                <p className="text-xs text-muted-foreground">
                  Showing {rowCount} rows
                </p>
                <label className="flex items-center gap-1 text-xs text-muted-foreground">
                  Rows per page
                  <input
                    type="number"
                    min={1}
                    step={1}
                    value={rowsPerPage}
                    onChange={(event) => {
                      const value = Number(event.target.value);
                      if (!Number.isFinite(value) || value < 1) {
                        return;
                      }
                      setRowsPerPage(Math.floor(value));
                    }}
                    className="h-7 w-20 rounded border border-input bg-background px-2 text-xs"
                    aria-label="Rows per page"
                  />
                </label>
              </div>
            )}
          </CardContent>
        </details>
      </Card>
    );
  };

  const renderSourcesSection = () => {
    if (!hasSources && !hasEvidence) {
      return (
        <Card className="mt-4">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No sources available for this response.
          </CardContent>
        </Card>
      );
    }
    return (
      <div className="space-y-4 mt-4">
        {hasEvidence && (
          <Card>
            <details>
              <summary className="cursor-pointer list-none px-6 py-4">
                <div className="flex items-center justify-between gap-3">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <BookOpen size={16} />
                    Evidence ({message.evidence?.length || 0})
                  </CardTitle>
                  <span className="text-xs text-muted-foreground">Expand</span>
                </div>
              </summary>
              <CardContent className="pt-0">
                {message.sources && message.sources.length > 0 && (
                  <div className="mb-3 text-xs text-muted-foreground">
                    Context summary: {message.sources.length} sources · Top:{" "}
                    {message.sources
                      .slice(0, 3)
                      .map((source) => source.name)
                      .join(", ")}
                  </div>
                )}
                {activeSql && (
                  <div className="mb-3">
                    <div className="text-xs font-medium text-muted-foreground">Raw SQL</div>
                    <pre className="mt-1 rounded bg-secondary p-2 text-xs overflow-x-auto">
                      <code>{activeSql}</code>
                    </pre>
                  </div>
                )}
                <ul className="space-y-2">
                  {message.evidence?.map((item, index) => (
                    <li
                      key={`${item.datapoint_id}-${item.type || "datapoint"}-${index}`}
                      className="text-sm flex items-start gap-2"
                    >
                      <span className="text-xs px-2 py-0.5 rounded bg-secondary">
                        {item.type || "DataPoint"}
                      </span>
                      <span className="flex-1">
                        {item.name || item.datapoint_id}
                        {item.reason && (
                          <span className="text-xs text-muted-foreground ml-2">
                            ({item.reason})
                          </span>
                        )}
                      </span>
                    </li>
                  ))}
                </ul>
              </CardContent>
            </details>
          </Card>
        )}

        {hasSources && (
          <Card>
            <details>
              <summary className="cursor-pointer list-none px-6 py-4">
                <div className="flex items-center justify-between gap-3">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <BookOpen size={16} />
                    Retrieved Sources ({message.sources?.length || 0})
                  </CardTitle>
                  <span className="text-xs text-muted-foreground">Expand</span>
                </div>
              </summary>
              <CardContent className="pt-0">
                <ul className="space-y-2 text-sm">
                  {message.sources?.map((source, index) => (
                    <li
                      key={`${source.datapoint_id}-${source.type}-${index}`}
                      className="text-sm flex items-start gap-2"
                    >
                      <span className="text-xs px-2 py-0.5 rounded bg-secondary">
                        {source.type}
                      </span>
                      <span className="flex-1">
                        {source.name}
                        <span className="text-xs text-muted-foreground ml-2">
                          (score: {source.relevance_score.toFixed(2)})
                        </span>
                      </span>
                    </li>
                  ))}
                </ul>
              </CardContent>
            </details>
          </Card>
        )}
      </div>
    );
  };

  const renderVisualizationSection = () => {
    if (subAnswers.length > 1) {
      return (
        <Card className="mt-4">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <TableIcon size={16} />
              Visualizations by sub-question
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {subAnswers.map((item, idx) => {
              const { columns, rowCount: subRowCount } = toTabularRows(item.data ?? null);
              const subHint = inferVisualizationTypeForData(item.data ?? null, item.visualization_hint);
              const isActive = idx === selectedSubAnswerIndex;
              return (
                <details
                  key={`sub-viz-${item.index}-${idx}`}
                  open={isActive}
                  className="rounded border border-border/80 bg-muted/10"
                >
                  <summary
                    className="cursor-pointer px-3 py-2 text-sm font-medium"
                    onClick={() => setSelectedSubAnswerIndex(idx)}
                  >
                    Q{item.index}: {item.query}
                  </summary>
                  <div className="px-3 pb-3 space-y-2">
                    <p className="text-xs text-muted-foreground">
                      Suggested visualization: {formatVizHintLabel(subHint)} · {subRowCount} rows ·{" "}
                      {columns.length} columns
                    </p>
                    {isActive ? (
                      <ChartContainer
                        messageId={message.id}
                        hasTable={activeHasTable}
                        resolvedVizHint={resolvedVizHint}
                        columnNames={columnNames}
                        numericColumns={numericColumns}
                        nonNumericColumns={nonNumericColumns}
                        rowObjects={rowObjects}
                        showChartSettings={showChartSettings}
                        setShowChartSettings={setShowChartSettings}
                        chartTooltip={chartTooltip}
                        setChartTooltip={setChartTooltip}
                        hiddenPieLabels={hiddenPieLabels}
                        setHiddenPieLabels={setHiddenPieLabels}
                        barConfig={barConfig}
                        setBarConfig={setBarConfig}
                        lineConfig={lineConfig}
                        setLineConfig={setLineConfig}
                        scatterConfig={scatterConfig}
                        setScatterConfig={setScatterConfig}
                        pieConfig={pieConfig}
                        setPieConfig={setPieConfig}
                      />
                    ) : (
                      <button
                        type="button"
                        className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
                        onClick={() => setSelectedSubAnswerIndex(idx)}
                        aria-label={`View visualization for sub-question ${item.index}`}
                      >
                        View visualization
                      </button>
                    )}
                  </div>
                </details>
              );
            })}
          </CardContent>
        </Card>
      );
    }

    return (
      <ChartContainer
        messageId={message.id}
        hasTable={activeHasTable}
        resolvedVizHint={resolvedVizHint}
        columnNames={columnNames}
        numericColumns={numericColumns}
        nonNumericColumns={nonNumericColumns}
        rowObjects={rowObjects}
        showChartSettings={showChartSettings}
        setShowChartSettings={setShowChartSettings}
        chartTooltip={chartTooltip}
        setChartTooltip={setChartTooltip}
        hiddenPieLabels={hiddenPieLabels}
        setHiddenPieLabels={setHiddenPieLabels}
        barConfig={barConfig}
        setBarConfig={setBarConfig}
        lineConfig={lineConfig}
        setLineConfig={setLineConfig}
        scatterConfig={scatterConfig}
        setScatterConfig={setScatterConfig}
        pieConfig={pieConfig}
        setPieConfig={setPieConfig}
      />
    );
  };

  const renderTimingSection = () => {
    const showAgentTimings = hasAgentTimings && message.metrics?.agent_timings;
    if (!showAgentTimings && !hasLoopTelemetry) {
      return (
        <Card className="mt-4">
          <CardContent className="pt-6 text-sm text-muted-foreground">
            No timing or loop trace metadata available.
          </CardContent>
        </Card>
      );
    }

    return (
      <Card className="mt-4">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Clock size={16} />
            Execution Metadata
          </CardTitle>
        </CardHeader>
        <CardContent>
          {showAgentTimings ? (
            <div className="space-y-2">
              {Object.entries(message.metrics.agent_timings)
                .sort((a, b) => b[1] - a[1])
                .map(([agent, ms]) => (
                  <div key={agent} className="flex items-center justify-between gap-3 text-sm">
                    <span>{formatAgentTimingLabel(agent)}</span>
                    <span className="text-muted-foreground">{formatDurationSeconds(ms)}</span>
                  </div>
                ))}
            </div>
          ) : null}
          {hasLoopTelemetry ? (
            <div className={cn("space-y-2 text-sm", showAgentTimings ? "mt-4 border-t pt-3" : "")}>
              <div className="flex items-center justify-between gap-3">
                <span>Loop terminal state</span>
                <span className="text-muted-foreground">
                  {message.loop_terminal_state || "unknown"}
                </span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span>Loop stop reason</span>
                <span className="text-muted-foreground">{message.loop_stop_reason || "unknown"}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span>Action steps</span>
                <span className="text-muted-foreground">{message.action_trace?.length ?? 0}</span>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span>Shadow decisions</span>
                <span className="text-muted-foreground">
                  {message.loop_shadow_decisions?.length ?? 0}
                </span>
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
    );
  };

  const tabs: Array<{ id: TabId; label: string }> = useMemo(() => {
    const items: Array<{ id: TabId; label: string }> = [{ id: "answer", label: "Answer" }];
    if (hasSql) {
      items.push({ id: "sql", label: "SQL" });
    }
    if (hasTable) {
      items.push({ id: "table", label: "Table" });
      items.push({ id: "visualization", label: "Visualization" });
    }
    if (hasSources || hasEvidence) {
      items.push({ id: "sources", label: "Evidence" });
    }
    if (hasAgentTimings || hasLoopTelemetry) {
      items.push({ id: "timing", label: "Timing" });
    }
    return items;
  }, [hasSql, hasAgentTimings, hasEvidence, hasLoopTelemetry, hasSources, hasTable]);

  useEffect(() => {
    if (!tabs.some((tab) => tab.id === activeTab)) {
      setActiveTab("answer");
    }
  }, [activeTab, tabs]);

  const focusTabByIndex = (index: number) => {
    const nextIndex = (index + tabs.length) % tabs.length;
    tabButtonRefs.current[nextIndex]?.focus();
    setActiveTab(tabs[nextIndex].id);
  };

  const handleTabKeyDown = (event: React.KeyboardEvent<HTMLButtonElement>, index: number) => {
    if (event.key === "ArrowRight") {
      event.preventDefault();
      focusTabByIndex(index + 1);
      return;
    }
    if (event.key === "ArrowLeft") {
      event.preventDefault();
      focusTabByIndex(index - 1);
      return;
    }
    if (event.key === "Home") {
      event.preventDefault();
      focusTabByIndex(0);
      return;
    }
    if (event.key === "End") {
      event.preventDefault();
      focusTabByIndex(tabs.length - 1);
    }
  };

  const hasMultipleSubAnswers = subAnswers.length > 1;
  const assistantMeta =
    !isUser &&
    (message.answer_source || message.tool_approval_required || hasMultipleSubAnswers);
  const showActions =
    !isUser && (Boolean(activeSql) || activeHasTable || Boolean(activeContent?.trim()));

  const renderWorkflowPackage = () => {
    if (!hasWorkflowArtifacts || !workflowArtifacts) {
      return null;
    }

    const summaryText = workflowArtifacts.summary?.trim() || "";
    const normalizedSummary = normalizeAnswerText(summaryText);
    const normalizedAnswer = normalizeAnswerText(activeContent || "");
    const isDuplicateSummary =
      Boolean(normalizedSummary) &&
      Boolean(normalizedAnswer) &&
      (normalizedSummary === normalizedAnswer ||
        normalizedSummary.includes(normalizedAnswer) ||
        normalizedAnswer.includes(normalizedSummary));
    const showSummary = Boolean(summaryText) && !isDuplicateSummary;

    return (
      <div className="mt-3 rounded-lg border border-border/70 bg-secondary/20 p-3">
        <div className="mb-2 text-xs font-semibold tracking-wide text-muted-foreground">
          Finance Brief
        </div>
        {showSummary && <p className="text-sm leading-relaxed">{summaryText}</p>}

        {workflowArtifacts.metrics.length > 0 && (
          <div className="mt-3">
            <div className="mb-1 text-xs font-medium text-muted-foreground">Key Metrics</div>
            <div className="grid gap-2 sm:grid-cols-2">
              {workflowArtifacts.metrics.slice(0, 4).map((metric, index) => (
                <div
                  key={`metric-${index}-${metric.label}-${metric.value}`}
                  className="rounded border border-border/60 bg-background/80 px-2 py-1"
                >
                  <div className="text-[11px] text-muted-foreground">{metric.label}</div>
                  <div className="text-xs font-medium">{metric.value}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {workflowArtifacts.drivers.length > 0 && (
          <div className="mt-3">
            <div className="mb-1 text-xs font-medium text-muted-foreground">Top Drivers</div>
            <ul className="space-y-1">
              {workflowArtifacts.drivers.slice(0, 3).map((driver, index) => (
                <li key={`driver-${index}-${driver.dimension}-${driver.value}`} className="text-xs">
                  <span className="font-medium">{driver.dimension}:</span> {driver.value} ({driver.contribution})
                </li>
              ))}
            </ul>
          </div>
        )}

        {workflowArtifacts.caveats.length > 0 && (
          <div className="mt-3">
            <div className="mb-1 text-xs font-medium text-muted-foreground">Caveats</div>
            <ul className="space-y-1">
              {workflowArtifacts.caveats.slice(0, 3).map((item, index) => (
                <li key={`caveat-${index}-${item}`} className="text-xs text-muted-foreground">
                  • {item}
                </li>
              ))}
            </ul>
          </div>
        )}

        {workflowArtifacts.follow_ups.length > 0 && (
          <div className="mt-3">
            <div className="mb-1 text-xs font-medium text-muted-foreground">Suggested Follow-ups</div>
            <ul className="space-y-1">
              {workflowArtifacts.follow_ups.slice(0, 3).map((item, index) => (
                <li key={`follow-up-${index}-${item}`} className="text-xs">
                  • {item}
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    );
  };

  const renderAnswerOnly = () => (
    <>
      {renderMarkdownish(activeContent)}
      {renderWorkflowPackage()}
      {renderClarifyingQuestions()}
    </>
  );

  const renderTabContent = () => {
    if (activeTab === "answer") {
      return renderAnswerOnly();
    }
    if (activeTab === "sql") {
      return renderSqlSection();
    }
    if (activeTab === "table") {
      return renderTableSection();
    }
    if (activeTab === "visualization") {
      return renderVisualizationSection();
    }
    if (activeTab === "sources") {
      return renderSourcesSection();
    }
    if (activeTab === "timing") {
      return renderTimingSection();
    }
    return null;
  };

  const formatAgentTimingLabel = (agent: string) => {
    const labels: Record<string, string> = {
      tool_planner: "Tool Planner",
      classifier: "Classifier",
      context: "Context",
      sql: "SQL",
      validator: "Validator",
      executor: "Executor",
      context_answer: "Context Answer",
      response_synthesis: "Response Synthesis",
    };
    if (labels[agent]) {
      return labels[agent];
    }
    return agent
      .split("_")
      .map((part) => (part ? `${part[0].toUpperCase()}${part.slice(1)}` : part))
      .join(" ");
  };

  return (
    <div
      className={cn("flex gap-3 mb-4", isUser ? "justify-end" : "justify-start")}
      role="article"
      aria-label={isUser ? "User message" : "Assistant message"}
    >
      {!isUser && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full bg-primary flex items-center justify-center text-primary-foreground"
          aria-hidden="true"
        >
          <Bot size={18} />
        </div>
      )}

      <div className={cn("flex-1 max-w-4xl", isUser && "flex justify-end")}>
        <div
          className={cn(
            "rounded-xl px-4 py-3",
            isUser
              ? "bg-primary/90 text-primary-foreground shadow-sm"
              : "border border-border/70 bg-card text-foreground shadow-sm"
          )}
        >
          {assistantMeta && (
            <div className="mb-2 flex min-w-0 items-center gap-2 text-xs">
              {message.answer_source && (
                <span className="inline-flex items-center gap-1 rounded-full bg-secondary px-2 py-1 text-foreground">
                  <BadgeCheck size={12} />
                  {message.answer_source}
                  {typeof message.answer_confidence === "number" &&
                    ` · ${message.answer_confidence.toFixed(2)}`}
                </span>
              )}
              {hasMultipleSubAnswers && (
                <div className="inline-flex items-center gap-1">
                  {subAnswers.map((item, idx) => (
                    <button
                      key={`meta-sub-answer-${item.index}-${idx}`}
                      type="button"
                      className={cn(
                        "rounded px-2 py-0.5 transition",
                        idx === selectedSubAnswerIndex
                          ? "bg-primary text-primary-foreground"
                          : "bg-secondary text-foreground hover:bg-secondary/80"
                      )}
                      onClick={() => setSelectedSubAnswerIndex(idx)}
                      aria-label={`Focus sub-question ${item.index}`}
                    >
                      Q{item.index}
                    </button>
                  ))}
                </div>
              )}
              {hasMultipleSubAnswers && activeSubAnswer && (
                <div className="min-w-0 flex-1 truncate text-muted-foreground">
                  {activeSubAnswer.query}
                </div>
              )}
              {message.tool_approval_required && (
                <span className="inline-flex items-center gap-1 rounded-full bg-amber-100 px-2 py-1 text-amber-900">
                  Approval required
                </span>
              )}
            </div>
          )}

          {showActions && (
            <div className="mb-3 space-y-2 text-xs">
              <div className="flex flex-wrap items-center gap-2">
                {activeSql && (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                    onClick={copySql}
                    aria-label="Copy generated SQL"
                  >
                    <Copy size={12} />
                    Copy SQL
                  </button>
                )}
                {activeSql && onEditSqlDraft && (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                    onClick={() => onEditSqlDraft(activeSql)}
                    aria-label="Edit SQL draft"
                  >
                    <Code size={12} />
                    Edit SQL
                  </button>
                )}
                {activeHasTable && (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                    onClick={downloadCsv}
                    aria-label="Download query results as CSV"
                  >
                    <Download size={12} />
                    Download CSV
                  </button>
                )}
                {activeHasTable && (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                    onClick={downloadJson}
                    aria-label="Download query results as JSON"
                  >
                    <Download size={12} />
                    Download JSON
                  </button>
                )}
                {activeHasTable && (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                    onClick={copyMarkdownTable}
                    aria-label="Copy query results as markdown table"
                  >
                    <Copy size={12} />
                    Copy Markdown
                  </button>
                )}
                <button
                  type="button"
                  className="inline-flex items-center gap-1 rounded border border-border px-2 py-1 hover:bg-secondary"
                  onClick={copyShareLink}
                  aria-label="Copy share link for this result"
                >
                  <Link2 size={12} />
                  Share Link
                </button>
                {!onSubmitFeedback && actionNotice && (
                  <span className="text-muted-foreground" role="status" aria-live="polite">
                    {actionNotice}
                  </span>
                )}
              </div>
              {onSubmitFeedback && (
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    className={cn(
                      "inline-flex items-center gap-1 rounded border px-2 py-1 hover:bg-secondary",
                      feedbackSentiment === "up"
                        ? "border-emerald-400 bg-emerald-50 text-emerald-700"
                        : "border-border"
                    )}
                    onClick={() => submitFeedback("answer_feedback", "up")}
                    disabled={feedbackSubmitting}
                    aria-label="Rate answer helpful"
                  >
                    <ThumbsUp size={12} />
                    Helpful
                  </button>
                  <button
                    type="button"
                    className={cn(
                      "inline-flex items-center gap-1 rounded border px-2 py-1 hover:bg-secondary",
                      feedbackSentiment === "down"
                        ? "border-rose-400 bg-rose-50 text-rose-700"
                        : "border-border"
                    )}
                    onClick={() => submitFeedback("answer_feedback", "down")}
                    disabled={feedbackSubmitting}
                    aria-label="Rate answer not helpful"
                  >
                    <ThumbsDown size={12} />
                    Not Helpful
                  </button>
                  <button
                    type="button"
                    className={cn(
                      "inline-flex items-center gap-1 rounded border px-2 py-1 hover:bg-secondary",
                      feedbackMode === "issue" ? "border-primary bg-primary/10 text-primary" : "border-border"
                    )}
                    onClick={() => setFeedbackMode((prev) => (prev === "issue" ? null : "issue"))}
                    disabled={feedbackSubmitting}
                    aria-label="Report issue with this answer"
                  >
                    <Flag size={12} />
                    Report Issue
                  </button>
                  <button
                    type="button"
                    className={cn(
                      "inline-flex items-center gap-1 rounded border px-2 py-1 hover:bg-secondary",
                      feedbackMode === "suggestion"
                        ? "border-primary bg-primary/10 text-primary"
                        : "border-border"
                    )}
                    onClick={() =>
                      setFeedbackMode((prev) => (prev === "suggestion" ? null : "suggestion"))
                    }
                    disabled={feedbackSubmitting}
                    aria-label="Suggest improvement for this answer"
                  >
                    <Lightbulb size={12} />
                    Suggest Improvement
                  </button>
                  {actionNotice && (
                    <span className="text-muted-foreground" role="status" aria-live="polite">
                      {actionNotice}
                    </span>
                  )}
                </div>
              )}
            </div>
          )}

          {onSubmitFeedback && feedbackMode && (
            <div className="mb-3 rounded-md border border-border/70 bg-background/80 p-3">
              <div className="mb-2 text-xs font-medium">
                {feedbackMode === "issue" ? "Report issue" : "Suggest improvement"}
              </div>
              <textarea
                value={feedbackDraft}
                onChange={(event) => setFeedbackDraft(event.target.value)}
                className="min-h-[92px] w-full resize-y rounded border border-input bg-background px-2 py-2 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring"
                placeholder={
                  feedbackMode === "issue"
                    ? "Describe what was wrong (missing context, wrong SQL, wrong metric definition, etc.)"
                    : "Describe how this response or retrieval can improve."
                }
                aria-label={
                  feedbackMode === "issue" ? "Issue report details" : "Improvement suggestion details"
                }
              />
              <div className="mt-2 flex items-center gap-2">
                <button
                  type="button"
                  className="inline-flex items-center rounded border border-border px-2 py-1 text-xs hover:bg-secondary disabled:opacity-60"
                  onClick={() =>
                    submitFeedback(
                      feedbackMode === "issue" ? "issue_report" : "improvement_suggestion",
                      null,
                      feedbackDraft.trim()
                    )
                  }
                  disabled={feedbackSubmitting || !feedbackDraft.trim()}
                >
                  {feedbackSubmitting ? "Submitting..." : "Submit"}
                </button>
                <button
                  type="button"
                  className="inline-flex items-center rounded border border-border px-2 py-1 text-xs hover:bg-secondary"
                  onClick={() => {
                    setFeedbackMode(null);
                    setFeedbackDraft("");
                  }}
                  disabled={feedbackSubmitting}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {!isUser && displayMode === "tabbed" ? (
            <>
              <div
                className="mb-2 flex flex-wrap gap-2 border-b border-border pb-2"
                role="tablist"
                aria-label="Assistant response sections"
                id={tabListId}
              >
                {tabs.map((tab, index) => (
                  <button
                    key={tab.id}
                    type="button"
                    ref={(element) => {
                      tabButtonRefs.current[index] = element;
                    }}
                    role="tab"
                    id={`${message.id}-tab-${tab.id}`}
                    aria-selected={activeTab === tab.id}
                    aria-controls={`${message.id}-panel-${tab.id}`}
                    className={cn(
                      "rounded px-2.5 py-1 text-xs transition",
                      activeTab === tab.id
                        ? "bg-primary text-primary-foreground"
                        : "bg-secondary text-foreground hover:bg-secondary/80"
                    )}
                    onClick={() => setActiveTab(tab.id)}
                    onKeyDown={(event) => handleTabKeyDown(event, index)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
              <div
                role="tabpanel"
                id={activeTabPanelId}
                aria-labelledby={`${message.id}-tab-${activeTab}`}
              >
                {renderTabContent()}
              </div>
            </>
          ) : (
            <>
              {renderAnswerOnly()}
              {!isUser && activeSql && renderSqlSection()}
              {!isUser && hasTable && renderTableSection()}
              {!isUser && (hasSources || hasEvidence) && renderSourcesSection()}
            </>
          )}

          {message.metrics && (
            <div className="mt-3 text-xs text-muted-foreground">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-1">
                  <Clock size={12} />
                  {formatDurationSeconds(message.metrics.total_latency_ms)}
                </div>
                {message.metrics.llm_calls > 0 && <div>LLM calls: {message.metrics.llm_calls}</div>}
                {message.metrics.retry_count > 0 && <div>Retries: {message.metrics.retry_count}</div>}
                <div>
                  Formatter: {message.metrics.sql_formatter_fallback_calls ?? 0} (
                  {message.metrics.sql_formatter_fallback_successes ?? 0} recovered)
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {isUser && (
        <div
          className="flex-shrink-0 w-8 h-8 rounded-full bg-secondary flex items-center justify-center text-secondary-foreground"
          aria-hidden="true"
        >
          <User size={18} />
        </div>
      )}
    </div>
  );
}
