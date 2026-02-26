/**
 * Chat Interface Component
 *
 * Main chat UI with:
 * - Message list with auto-scroll
 * - Message input with send button
 * - Agent status display during processing
 * - WebSocket integration for real-time updates
 * - Error handling and loading states
 */

"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { createPortal } from "react-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Send,
  AlertCircle,
  Loader2,
  RefreshCw,
  Wifi,
  Clock,
  Database,
  AlertTriangle,
  Plus,
  Keyboard,
  FileCode2,
} from "lucide-react";
import {
  Message,
  type MessageFeedbackPayload,
  type MessageTrainPayload,
} from "./Message";
import { ConversationHistorySidebar } from "./ConversationHistorySidebar";
import { SchemaExplorerSidebar } from "./SchemaExplorerSidebar";
import type { ConversationSnapshot, SerializedMessage } from "./chatTypes";
import { AgentStatus } from "../agents/AgentStatus";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Card } from "../ui/card";
import { useChatStore, type Message as ChatStoreMessage } from "@/lib/stores/chat";
import {
  apiClient,
  wsClient,
  type ConversationSnapshotPayload,
  type DatabaseConnection,
  type DatabaseSchemaTable,
  type SetupStep,
} from "@/lib/api";
import { SystemSetup } from "../system/SystemSetup";
import {
  getResultLayoutMode,
  getShowLiveReasoning,
  getShowAgentTimingBreakdown,
  getSynthesizeSimpleSql,
  type ResultLayoutMode,
} from "@/lib/settings";
import { decodeShareToken } from "@/lib/share";
import { formatWaitingChipLabel } from "./loadingUx";

const ACTIVE_DATABASE_STORAGE_KEY = "datachat.active_connection_id";
const CONVERSATION_HISTORY_STORAGE_KEY = "datachat.conversation.history.v1";
const ENV_DATABASE_CONNECTION_ID = "00000000-0000-0000-0000-00000000dada";
const MAX_CONVERSATION_HISTORY = 20;
const MAX_CONVERSATION_MESSAGES = 50;

const serializeMessages = (items: ChatStoreMessage[]): SerializedMessage[] =>
  items.slice(-MAX_CONVERSATION_MESSAGES).map((message) => ({
    ...message,
    timestamp:
      message.timestamp instanceof Date
        ? message.timestamp.toISOString()
        : new Date(message.timestamp).toISOString(),
  }));

const deserializeMessages = (items: SerializedMessage[]): ChatStoreMessage[] =>
  items.map((message) => ({
    ...message,
    timestamp: new Date(message.timestamp),
  }));

const buildConversationTitle = (items: ChatStoreMessage[]): string => {
  const firstUserMessage = items.find((message) => message.role === "user")?.content?.trim();
  if (!firstUserMessage) {
    return "Untitled conversation";
  }
  const compact = firstUserMessage.replace(/\s+/g, " ");
  return compact.length > 70 ? `${compact.slice(0, 67)}...` : compact;
};

const QUERY_TEMPLATES: Array<{ id: string; label: string; build: (selectedTable?: string | null) => string }> = [
  {
    id: "list-tables",
    label: "List Tables",
    build: () => "List all available tables.",
  },
  {
    id: "show-columns",
    label: "Show Columns",
    build: (selectedTable) =>
      selectedTable
        ? `Show columns for ${selectedTable}.`
        : "Show columns for the table grocery_sales_transactions.",
  },
  {
    id: "sample-rows",
    label: "Sample 100 Rows",
    build: (selectedTable) =>
      selectedTable
        ? `Show first 100 rows from ${selectedTable}.`
        : "Show first 100 rows from grocery_sales_transactions.",
  },
  {
    id: "row-count",
    label: "Count Rows",
    build: (selectedTable) =>
      selectedTable
        ? `How many rows are in ${selectedTable}?`
        : "How many rows are in each table?",
  },
  {
    id: "top-10",
    label: "Top 10",
    build: (selectedTable) =>
      selectedTable
        ? `Show the top 10 records from ${selectedTable} by the most relevant numeric metric.`
        : "Show top 10 products by sales amount.",
  },
  {
    id: "trend",
    label: "Trend",
    build: (selectedTable) =>
      selectedTable
        ? `Show a monthly trend from ${selectedTable} for the last 12 months.`
        : "Show a monthly trend for revenue for the last 12 months.",
  },
  {
    id: "breakdown",
    label: "Category Breakdown",
    build: (selectedTable) =>
      selectedTable
        ? `Give me a category breakdown from ${selectedTable}.`
        : "Give me a category breakdown of sales by department.",
  },
];

type MetadataExplorerItem = {
  id: string;
  name: string;
  type: string;
  status: "pending" | "approved" | "managed";
  connectionId?: string | null;
  scope?: string | null;
  description?: string | null;
  businessPurpose?: string | null;
  sqlTemplate?: string | null;
  tableName?: string | null;
  relatedTables?: string[];
  confidence?: number | null;
  reviewNote?: string | null;
  sourceTier?: string | null;
  sourcePath?: string | null;
  lifecycleVersion?: string | null;
  lifecycleChangedAt?: string | null;
  lifecycleChangedBy?: string | null;
  payload?: Record<string, unknown> | null;
};

type TrainMode = "create" | "update";

type TrainManagedQueryOption = {
  datapointId: string;
  name: string;
  connectionId: string | null;
  scope: string | null;
  lifecycleVersion: string | null;
  lifecycleChangedAt: string | null;
  lifecycleChangedBy: string | null;
};

const normalizeMetadataItem = (
  item: Record<string, unknown>,
  status: "pending" | "approved"
): MetadataExplorerItem => {
  const datapoint = (item.datapoint as Record<string, unknown>) || {};
  const metadata = (datapoint.metadata as Record<string, unknown>) || {};
  return {
    id: String(datapoint.datapoint_id || item.pending_id || "unknown_datapoint"),
    name: String(datapoint.name || datapoint.datapoint_id || item.pending_id || "Unnamed DataPoint"),
    type: String(datapoint.type || "Unknown"),
    status,
    description:
      typeof datapoint.description === "string" ? datapoint.description : null,
    businessPurpose:
      typeof datapoint.business_purpose === "string"
        ? datapoint.business_purpose
        : null,
    sqlTemplate:
      typeof datapoint.sql_template === "string" ? datapoint.sql_template : null,
    tableName:
      typeof datapoint.table_name === "string"
        ? datapoint.table_name
        : typeof datapoint.table === "string"
          ? datapoint.table
          : null,
    relatedTables: Array.isArray(datapoint.related_tables)
      ? datapoint.related_tables
          .filter((entry): entry is string => typeof entry === "string")
          .slice(0, 10)
      : [],
    confidence:
      typeof item.confidence === "number"
        ? item.confidence
        : null,
    reviewNote:
      typeof item.review_note === "string" ? item.review_note : null,
    sourceTier:
      typeof metadata.source_tier === "string" ? metadata.source_tier : null,
    sourcePath:
      typeof metadata.source_path === "string" ? metadata.source_path : null,
    lifecycleVersion:
      typeof metadata.lifecycle_version === "string" ? metadata.lifecycle_version : null,
    lifecycleChangedAt:
      typeof metadata.lifecycle_changed_at === "string" ? metadata.lifecycle_changed_at : null,
    lifecycleChangedBy:
      typeof metadata.lifecycle_changed_by === "string" ? metadata.lifecycle_changed_by : null,
    payload: datapoint,
  };
};

const filterMetadataItems = (
  items: MetadataExplorerItem[],
  query: string
): MetadataExplorerItem[] => {
  const search = query.trim().toLowerCase();
  if (!search) {
    return items;
  }
  return items.filter((item) => {
    const haystack = `${item.id} ${item.name} ${item.type} ${item.description || ""} ${
      item.businessPurpose || ""
    } ${item.tableName || ""} ${(item.relatedTables || []).join(" ")} ${
      item.connectionId || ""
    } ${item.scope || ""} ${
      item.sourceTier || ""
    } ${item.sourcePath || ""}`.toLowerCase();
    return haystack.includes(search);
  });
};

const dedupeMetadataItems = (items: MetadataExplorerItem[]): MetadataExplorerItem[] => {
  const seen = new Set<string>();
  return items.filter((item) => {
    if (seen.has(item.id)) {
      return false;
    }
    seen.add(item.id);
    return true;
  });
};

const isEnvironmentConnection = (connection: DatabaseConnection): boolean =>
  String(connection.connection_id) === ENV_DATABASE_CONNECTION_ID ||
  (connection.tags || []).includes("env");

const slugifyDatapointToken = (value: string): string => {
  const slug = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
  return slug || "custom_query";
};

const extractRelatedTablesFromSql = (sql: string): string[] => {
  const matches = sql.match(/\b(?:from|join)\s+([a-zA-Z0-9_."]+)/gi) || [];
  const cleaned = matches
    .map((entry) =>
      entry
        .replace(/\b(?:from|join)\s+/i, "")
        .replace(/["`]/g, "")
        .trim()
        .toLowerCase()
    )
    .filter(Boolean);
  return Array.from(new Set(cleaned));
};

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });

const formatTrainHistoryTimestamp = (value: string | null): string => {
  if (!value) {
    return "Unknown";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
};

export function ChatInterface() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const queryClient = useQueryClient();
  const {
    messages,
    conversationId,
    frontendSessionId,
    sessionSummary,
    sessionState,
    isLoading,
    isConnected,
    agentHistory,
    agentStatus,
    setLoading,
    setConnected,
    setAgentUpdate,
    resetAgentStatus,
    clearMessages,
    addMessage,
    updateLastMessage,
    setConversationId,
    setSessionMemory,
    loadSession,
    appendToLastMessage,
  } = useChatStore();

  const [input, setInput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [errorCategory, setErrorCategory] = useState<"network" | "timeout" | "validation" | "database" | "unknown" | null>(null);
  const [lastFailedQuery, setLastFailedQuery] = useState<string | null>(null);
  const [retryCount, setRetryCount] = useState(0);
  const [setupSteps, setSetupSteps] = useState<SetupStep[]>([]);
  const [isInitialized, setIsInitialized] = useState(true);
  const [setupError, setSetupError] = useState<string | null>(null);
  const [setupNotice, setSetupNotice] = useState<string | null>(null);
  const [setupCompleted, setSetupCompleted] = useState(false);
  const [isInitializing, setIsInitializing] = useState(false);
  const [isBackendReachable, setIsBackendReachable] = useState(false);
  const [connections, setConnections] = useState<DatabaseConnection[]>([]);
  const [targetDatabaseId, setTargetDatabaseId] = useState<string | null>(null);
  const [conversationDatabaseId, setConversationDatabaseId] = useState<string | null>(null);
  const [resultLayoutMode, setResultLayoutMode] =
    useState<ResultLayoutMode>("stacked");
  const [showAgentTimingBreakdown, setShowAgentTimingBreakdown] = useState(true);
  const [synthesizeSimpleSql, setSynthesizeSimpleSql] = useState(true);
  const [showLiveReasoning, setShowLiveReasoning] = useState(true);
  const [thinkingNotes, setThinkingNotes] = useState<string[]>([]);
  const [loadingElapsedSeconds, setLoadingElapsedSeconds] = useState(0);
  const [toolApprovalOpen, setToolApprovalOpen] = useState(false);
  const [toolApprovalCalls, setToolApprovalCalls] = useState<
    { name: string; arguments?: Record<string, unknown> }[]
  >([]);
  const [toolApprovalMessage, setToolApprovalMessage] = useState<string | null>(null);
  const [toolApprovalRunning, setToolApprovalRunning] = useState(false);
  const [toolApprovalError, setToolApprovalError] = useState<string | null>(null);
  const [isHistorySidebarOpen, setIsHistorySidebarOpen] = useState(false);
  const [isSchemaSidebarOpen, setIsSchemaSidebarOpen] = useState(false);
  const [conversationHistory, setConversationHistory] = useState<ConversationSnapshot[]>([]);
  const [schemaTables, setSchemaTables] = useState<DatabaseSchemaTable[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [schemaError, setSchemaError] = useState<string | null>(null);
  const [schemaSearch, setSchemaSearch] = useState("");
  const [metadataSearch, setMetadataSearch] = useState("");
  const [includeExampleMetadata, setIncludeExampleMetadata] = useState(false);
  const [explorerMode, setExplorerMode] = useState<"schema" | "metadata">("schema");
  const [selectedMetadataKey, setSelectedMetadataKey] = useState<string | null>(null);
  const [metadataDetailCache, setMetadataDetailCache] = useState<
    Record<string, Record<string, unknown>>
  >({});
  const [metadataDetailLoadingKey, setMetadataDetailLoadingKey] = useState<string | null>(null);
  const [metadataDetailError, setMetadataDetailError] = useState<string | null>(null);
  const [selectedSchemaTable, setSelectedSchemaTable] = useState<string | null>(null);
  const [conversationSearch, setConversationSearch] = useState("");
  const [composerMode, setComposerMode] = useState<"nl" | "sql">("nl");
  const [sqlDraft, setSqlDraft] = useState("");
  const [trainModalOpen, setTrainModalOpen] = useState(false);
  const [trainMode, setTrainMode] = useState<TrainMode>("create");
  const [trainTargetDatapointId, setTrainTargetDatapointId] = useState("");
  const [trainHistorySearch, setTrainHistorySearch] = useState("");
  const [trainQuestion, setTrainQuestion] = useState("");
  const [trainSql, setTrainSql] = useState("");
  const [trainName, setTrainName] = useState("");
  const [trainNotes, setTrainNotes] = useState("");
  const [trainRelatedTables, setTrainRelatedTables] = useState("");
  const [trainSubmitting, setTrainSubmitting] = useState(false);
  const [trainLoadingExisting, setTrainLoadingExisting] = useState(false);
  const [trainSyncing, setTrainSyncing] = useState(false);
  const [trainError, setTrainError] = useState<string | null>(null);
  const [trainNotice, setTrainNotice] = useState<string | null>(null);
  const [isClientMounted, setIsClientMounted] = useState(false);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const sqlEditorRef = useRef<HTMLTextAreaElement>(null);
  const composerModeRef = useRef<"nl" | "sql">(composerMode);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const shortcutsCloseButtonRef = useRef<HTMLButtonElement>(null);
  const toolApprovalApproveButtonRef = useRef<HTMLButtonElement>(null);
  const loadedShareTokenRef = useRef<string | null>(null);
  const schemaDiscoveryStartRef = useRef<number>(Date.now());
  const schemaDiscoveryTrackedRef = useRef(false);
  const schemaLoadedTrackedRef = useRef(false);
  const restoreInputFocus = useCallback((targetMode?: "nl" | "sql") => {
    const mode = targetMode || composerModeRef.current;
    window.requestAnimationFrame(() => {
      if (mode === "sql") {
        sqlEditorRef.current?.focus();
        return;
      }
      inputRef.current?.focus();
    });
  }, []);

  useEffect(() => {
    composerModeRef.current = composerMode;
  }, [composerMode]);

  useEffect(() => {
    setIsClientMounted(true);
    return () => setIsClientMounted(false);
  }, []);

  const bootstrapQuery = useQuery({
    queryKey: ["chat-bootstrap"],
    queryFn: async () => {
      const status = await apiClient.systemStatus();
      const dbs = await apiClient.listDatabases().catch(() => []);
      return { status, dbs };
    },
  });

  const hasManagedConnection = connections.some(
    (connection) => !(connection.tags || []).includes("env")
  );
  const canRunQueries = isInitialized || hasManagedConnection;

  const metadataConnectionId = useMemo(() => {
    if (!connections.length) {
      return null;
    }
    const selected = connections.find((connection) => connection.connection_id === targetDatabaseId);
    if (selected && !isEnvironmentConnection(selected)) {
      return selected.connection_id;
    }
    const managedDefault = connections.find(
      (connection) => !isEnvironmentConnection(connection) && connection.is_default
    );
    if (managedDefault) {
      return managedDefault.connection_id;
    }
    const firstManaged = connections.find((connection) => !isEnvironmentConnection(connection));
    return firstManaged?.connection_id ?? null;
  }, [connections, targetDatabaseId]);

  const metadataContextNote = useMemo(() => {
    if (!targetDatabaseId) {
      return null;
    }
    if (!metadataConnectionId) {
      return "No managed metadata available yet. Run the onboarding wizard to profile, generate, and approve metadata for this source.";
    }
    if (targetDatabaseId === metadataConnectionId) {
      return null;
    }
    const selected = connections.find((connection) => connection.connection_id === targetDatabaseId);
    const metadataConnection = connections.find(
      (connection) => connection.connection_id === metadataConnectionId
    );
    if (!selected || !metadataConnection) {
      return null;
    }
    if (!isEnvironmentConnection(selected)) {
      return null;
    }
    return `Showing metadata for managed connection "${metadataConnection.name}" while target "${selected.name}" is environment-only.`;
  }, [connections, metadataConnectionId, targetDatabaseId]);

  const schemaQuery = useQuery({
    queryKey: ["database-schema", targetDatabaseId],
    queryFn: async () => apiClient.getDatabaseSchema(targetDatabaseId as string),
    enabled: Boolean(targetDatabaseId),
  });

  const pendingMetadataQuery = useQuery({
    queryKey: ["metadata-pending", metadataConnectionId],
    queryFn: async () =>
      apiClient.listPendingDatapoints({
        statusFilter: "pending",
        connectionId: metadataConnectionId,
      }),
    enabled: Boolean(metadataConnectionId),
  });

  const approvedMetadataQuery = useQuery({
    queryKey: ["metadata-approved", metadataConnectionId],
    queryFn: async () =>
      apiClient.listPendingDatapoints({
        statusFilter: "approved",
        connectionId: metadataConnectionId,
      }),
    enabled: Boolean(metadataConnectionId),
  });

  const managedMetadataQuery = useQuery({
    queryKey: ["metadata-managed"],
    queryFn: async () => apiClient.listDatapoints(),
  });

  const conversationHistoryQuery = useQuery({
    queryKey: ["ui-conversations"],
    queryFn: async () => apiClient.listConversations(MAX_CONVERSATION_HISTORY),
    retry: false,
  });

  const persistConversationHistory = (items: ConversationSnapshot[]) => {
    window.localStorage.setItem(
      CONVERSATION_HISTORY_STORAGE_KEY,
      JSON.stringify(items)
    );
    setConversationHistory(items);
  };

  const normalizeConversationPayload = useCallback(
    (payload: ConversationSnapshotPayload): ConversationSnapshot => ({
      frontendSessionId: payload.frontend_session_id,
      title: payload.title || "Untitled conversation",
      targetDatabaseId: payload.target_database_id ?? null,
      conversationId: payload.conversation_id ?? null,
      sessionSummary: payload.session_summary ?? null,
      sessionState: payload.session_state || null,
      updatedAt: payload.updated_at || new Date().toISOString(),
      createdAt: payload.created_at || null,
      messages: (payload.messages || [])
        .filter((item) => item && typeof item === "object")
        .map((item) => item as SerializedMessage),
    }),
    []
  );

  const toConversationUpsertPayload = useCallback(
    (snapshot: ConversationSnapshot) => ({
      title: snapshot.title,
      target_database_id: snapshot.targetDatabaseId,
      conversation_id: snapshot.conversationId,
      session_summary: snapshot.sessionSummary,
      session_state: snapshot.sessionState || {},
      messages: snapshot.messages as Array<Record<string, unknown>>,
      updated_at: snapshot.updatedAt,
    }),
    []
  );

  const mergeConversationSnapshots = useCallback(
    (
      incoming: ConversationSnapshot[],
      existing: ConversationSnapshot[]
    ): ConversationSnapshot[] => {
      const bySession = new Map<string, ConversationSnapshot>();
      for (const item of [...incoming, ...existing]) {
        const previous = bySession.get(item.frontendSessionId);
        if (!previous) {
          bySession.set(item.frontendSessionId, item);
          continue;
        }
        const previousTime = new Date(previous.updatedAt).getTime();
        const nextTime = new Date(item.updatedAt).getTime();
        const nextItem =
          !item.createdAt && previous.createdAt
            ? { ...item, createdAt: previous.createdAt }
            : item;
        if (Number.isNaN(previousTime) || nextTime > previousTime) {
          bySession.set(nextItem.frontendSessionId, nextItem);
        }
      }
      return Array.from(bySession.values())
        .sort(
          (a, b) =>
            new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
        )
        .slice(0, MAX_CONVERSATION_HISTORY);
    },
    []
  );

  const upsertConversationSnapshot = useCallback(
    (
      override: {
        frontendSessionId?: string;
        messages?: ChatStoreMessage[];
        conversationId?: string | null;
        sessionSummary?: string | null;
        sessionState?: Record<string, unknown> | null;
        targetDatabaseId?: string | null;
      } = {}
    ) => {
      const snapshotMessages = override.messages || messages;
      if (!snapshotMessages.some((message) => message.role === "user")) {
        return;
      }
      const nowIso = new Date().toISOString();
      const snapshot: ConversationSnapshot = {
        frontendSessionId: override.frontendSessionId || frontendSessionId,
        title: buildConversationTitle(snapshotMessages),
        targetDatabaseId:
          override.targetDatabaseId === undefined
            ? targetDatabaseId
            : override.targetDatabaseId,
        conversationId:
          override.conversationId === undefined
            ? conversationId
            : override.conversationId,
        sessionSummary:
          override.sessionSummary === undefined
            ? sessionSummary
            : override.sessionSummary,
        sessionState:
          override.sessionState === undefined ? sessionState : override.sessionState,
        updatedAt: nowIso,
        createdAt: null,
        messages: serializeMessages(snapshotMessages),
      };

      setConversationHistory((previous) => {
        const merged = mergeConversationSnapshots([snapshot], previous);
        window.localStorage.setItem(
          CONVERSATION_HISTORY_STORAGE_KEY,
          JSON.stringify(merged)
        );
        return merged;
      });

      void apiClient
        .upsertConversation(snapshot.frontendSessionId, toConversationUpsertPayload(snapshot))
        .then((saved) => {
          const normalized = normalizeConversationPayload(saved);
          queryClient.setQueryData(
            ["ui-conversations"],
            (existing: ConversationSnapshotPayload[] | undefined) => {
              const mergedPayload = [
                saved,
                ...(existing || []).filter(
                  (item) => item.frontend_session_id !== saved.frontend_session_id
                ),
              ].slice(0, MAX_CONVERSATION_HISTORY);
              return mergedPayload;
            }
          );
          setConversationHistory((previous) => {
            const merged = mergeConversationSnapshots([normalized], previous);
            window.localStorage.setItem(
              CONVERSATION_HISTORY_STORAGE_KEY,
              JSON.stringify(merged)
            );
            return merged;
          });
        })
        .catch(() => {
          // Local storage fallback stays active when backend persistence is unavailable.
        });
    },
    [
      conversationId,
      frontendSessionId,
      mergeConversationSnapshots,
      messages,
      normalizeConversationPayload,
      queryClient,
      sessionState,
      sessionSummary,
      targetDatabaseId,
      toConversationUpsertPayload,
    ]
  );

  const categorizeError = (errorMessage: string): "network" | "timeout" | "validation" | "database" | "unknown" => {
    const lower = errorMessage.toLowerCase();
    if (
      lower.includes("network") ||
      lower.includes("connection") ||
      lower.includes("econnrefused") ||
      lower.includes("enotfound") ||
      lower.includes("fetch failed") ||
      lower.includes("websocket")
    ) {
      return "network";
    }
    if (
      lower.includes("timeout") ||
      lower.includes("timed out") ||
      lower.includes("deadline exceeded")
    ) {
      return "timeout";
    }
    if (
      lower.includes("validation") ||
      lower.includes("invalid") ||
      lower.includes("syntax") ||
      lower.includes("required")
    ) {
      return "validation";
    }
    if (
      lower.includes("database") ||
      lower.includes("sql") ||
      lower.includes("table") ||
      lower.includes("column") ||
      lower.includes("schema") ||
      lower.includes("query")
    ) {
      return "database";
    }
    return "unknown";
  };

  const getErrorIcon = (category: "network" | "timeout" | "validation" | "database" | "unknown") => {
    switch (category) {
      case "network":
        return Wifi;
      case "timeout":
        return Clock;
      case "database":
        return Database;
      case "validation":
        return AlertTriangle;
      default:
        return AlertCircle;
    }
  };

  const getErrorSuggestion = (category: "network" | "timeout" | "validation" | "database" | "unknown"): string => {
    switch (category) {
      case "network":
        return "Check your internet connection and try again.";
      case "timeout":
        return "The request took too long. Try simplifying your query.";
      case "validation":
        return "Please check your input and try again.";
      case "database":
        return "There was an issue with the database. Try rephrasing your query.";
      default:
        return "An unexpected error occurred. Please try again.";
    }
  };

  const trackSchemaDiscovery = useCallback(
    (action: string, metadata?: Record<string, unknown>) => {
      if (schemaDiscoveryTrackedRef.current) {
        return;
      }
      schemaDiscoveryTrackedRef.current = true;
      const elapsedMs = Math.max(0, Date.now() - schemaDiscoveryStartRef.current);
      void apiClient.emitEntryEvent({
        flow: "schema_discovery",
        step: "first_schema_interaction",
        status: "completed",
        source: "ui",
        metadata: {
          action,
          elapsed_ms: elapsedMs,
          target_database_id: targetDatabaseId,
          ...(metadata || {}),
        },
      });
    },
    [targetDatabaseId]
  );

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    const raw = window.localStorage.getItem(CONVERSATION_HISTORY_STORAGE_KEY);
    if (!raw) {
      setConversationHistory([]);
      return;
    }
    try {
      const parsed = JSON.parse(raw) as ConversationSnapshot[];
      if (Array.isArray(parsed)) {
        setConversationHistory(
          parsed
            .filter((entry) => entry && Array.isArray(entry.messages))
            .map((entry) => ({
              ...entry,
              createdAt: entry.createdAt ?? null,
            }))
            .slice(0, MAX_CONVERSATION_HISTORY)
        );
      }
    } catch {
      setConversationHistory([]);
    }
  }, []);

  useEffect(() => {
    if (!conversationHistoryQuery.data) {
      return;
    }
    const normalizedRemote = conversationHistoryQuery.data.map(
      normalizeConversationPayload
    );
    setConversationHistory((previous) => {
      const merged = mergeConversationSnapshots(normalizedRemote, previous);
      window.localStorage.setItem(
        CONVERSATION_HISTORY_STORAGE_KEY,
        JSON.stringify(merged)
      );
      return merged;
    });
  }, [
    conversationHistoryQuery.data,
    mergeConversationSnapshots,
    normalizeConversationPayload,
  ]);

  useEffect(() => {
    if (!bootstrapQuery.data) {
      if (bootstrapQuery.isError) {
        setIsBackendReachable(false);
      }
      return;
    }
    const { status, dbs } = bootstrapQuery.data;
    setIsBackendReachable(true);
    setIsInitialized(status.is_initialized);
    setSetupSteps(status.setup_required || []);
    setConnections(dbs);

    setTargetDatabaseId((current) => {
      if (current && dbs.some((db) => db.connection_id === current)) {
        return current;
      }
      const storedId = window.localStorage.getItem(ACTIVE_DATABASE_STORAGE_KEY);
      const selected =
        dbs.find((db) => db.connection_id === storedId) ||
        dbs.find((db) => db.is_default) ||
        dbs[0] ||
        null;
      return selected?.connection_id ?? null;
    });
  }, [bootstrapQuery.data, bootstrapQuery.isError]);

  useEffect(() => {
    if (!targetDatabaseId) {
      window.localStorage.removeItem(ACTIVE_DATABASE_STORAGE_KEY);
      return;
    }
    window.localStorage.setItem(ACTIVE_DATABASE_STORAGE_KEY, targetDatabaseId);
  }, [targetDatabaseId]);

  useEffect(() => {
    const shareToken = searchParams.get("share");
    if (!shareToken || loadedShareTokenRef.current === shareToken) {
      return;
    }
    loadedShareTokenRef.current = shareToken;
    const shared = decodeShareToken(shareToken);
    if (!shared) {
      return;
    }

    clearMessages();
    setConversationDatabaseId(null);
    setConversationId(null);
    setSessionMemory(null, null);
    setInput("");
    setSqlDraft("");
    setComposerMode("nl");
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
    setRetryCount(0);
    resetAgentStatus();

    addMessage({
      role: "assistant",
      content: shared.answer,
      sql: shared.sql,
      data: shared.data,
      visualization_hint: shared.visualization_hint,
      visualization_metadata: shared.visualization_metadata || undefined,
      sources: shared.sources || [],
      answer_source: shared.answer_source,
      answer_confidence: shared.answer_confidence,
    });
  }, [
    addMessage,
    clearMessages,
    resetAgentStatus,
    searchParams,
    setConversationId,
    setSessionMemory,
  ]);

  useEffect(() => {
    if (!conversationId || !targetDatabaseId || conversationDatabaseId) {
      return;
    }
    setConversationDatabaseId(targetDatabaseId);
  }, [conversationId, targetDatabaseId, conversationDatabaseId]);

  useEffect(() => {
    schemaDiscoveryStartRef.current = Date.now();
    schemaDiscoveryTrackedRef.current = false;
    schemaLoadedTrackedRef.current = false;
  }, [targetDatabaseId]);

  useEffect(() => {
    if (!targetDatabaseId) {
      setSchemaLoading(false);
      setSchemaTables([]);
      setSchemaError(null);
      setSelectedSchemaTable(null);
      return;
    }
    setSchemaLoading(schemaQuery.isLoading || schemaQuery.isFetching);
    if (schemaQuery.data) {
      if (!schemaLoadedTrackedRef.current) {
        schemaLoadedTrackedRef.current = true;
        void apiClient.emitEntryEvent({
          flow: "schema_discovery",
          step: "schema_loaded",
          status: "completed",
          source: "ui",
          metadata: {
            target_database_id: targetDatabaseId,
            table_count: (schemaQuery.data.tables || []).length,
            elapsed_ms: Math.max(0, Date.now() - schemaDiscoveryStartRef.current),
          },
        });
      }
      setSchemaError(null);
      setSchemaTables(schemaQuery.data.tables || []);
      setSelectedSchemaTable((prev) => {
        if (!prev) return null;
        const stillExists = (schemaQuery.data?.tables || []).some(
          (table) => `${table.schema_name}.${table.table_name}` === prev
        );
        return stillExists ? prev : null;
      });
      return;
    }
    if (schemaQuery.error) {
      const message =
        schemaQuery.error instanceof Error ? schemaQuery.error.message : "Failed to load schema";
      setSchemaTables([]);
      setSchemaError(message);
    }
  }, [
    schemaQuery.data,
    schemaQuery.error,
    schemaQuery.isFetching,
    schemaQuery.isLoading,
    targetDatabaseId,
  ]);

  useEffect(() => {
    setResultLayoutMode(getResultLayoutMode());
    setShowAgentTimingBreakdown(getShowAgentTimingBreakdown());
    setSynthesizeSimpleSql(getSynthesizeSimpleSql());
    setShowLiveReasoning(getShowLiveReasoning());
    const handleStorage = () => {
      setResultLayoutMode(getResultLayoutMode());
      setShowAgentTimingBreakdown(getShowAgentTimingBreakdown());
      setSynthesizeSimpleSql(getSynthesizeSimpleSql());
      setShowLiveReasoning(getShowLiveReasoning());
    };
    window.addEventListener("storage", handleStorage);
    return () => {
      window.removeEventListener("storage", handleStorage);
    };
  }, []);

  useEffect(() => {
    if (!isLoading) {
      setLoadingElapsedSeconds(0);
      return;
    }

    const startedAt = Date.now();
    setLoadingElapsedSeconds(0);
    const interval = window.setInterval(() => {
      setLoadingElapsedSeconds(Math.max(1, Math.floor((Date.now() - startedAt) / 1000)));
    }, 500);

    return () => {
      window.clearInterval(interval);
    };
  }, [isLoading]);

  useEffect(() => {
    if (!isLoading) return;
    if (agentStatus === "idle") return;
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [agentHistory.length, agentStatus, isLoading]);

  const filteredSchemaTables = useMemo(() => {
    const search = schemaSearch.trim().toLowerCase();
    if (!search) {
      return schemaTables;
    }
    return schemaTables.filter((table) => {
      const fullName = `${table.schema_name}.${table.table_name}`.toLowerCase();
      if (fullName.includes(search)) {
        return true;
      }
      return table.columns.some((column) => column.name.toLowerCase().includes(search));
    });
  }, [schemaSearch, schemaTables]);

  const pendingMetadataItems = useMemo(() => {
    const items = (pendingMetadataQuery.data || []).map((item) =>
      normalizeMetadataItem(item as unknown as Record<string, unknown>, "pending")
    );
    return filterMetadataItems(dedupeMetadataItems(items), metadataSearch);
  }, [metadataSearch, pendingMetadataQuery.data]);
  const managedMetadataInContextItems = useMemo(() => {
    const items = (managedMetadataQuery.data || [])
      .map((item) => ({
        id: item.datapoint_id,
        name: item.name || item.datapoint_id,
        type: item.type || "Unknown",
        status: "managed" as const,
        connectionId: item.connection_id || null,
        scope: item.scope || null,
        description: null,
        businessPurpose: null,
        sqlTemplate: null,
        tableName: null,
        relatedTables: [],
        sourceTier: item.source_tier || null,
        sourcePath: item.source_path || null,
        lifecycleVersion: item.lifecycle_version || null,
        lifecycleChangedAt: item.lifecycle_changed_at || null,
        lifecycleChangedBy: item.lifecycle_changed_by || null,
        payload: null,
      }))
      .filter((item) => {
        if (!metadataConnectionId) {
          return true;
        }
        if (!item.connectionId && !item.scope) {
          return true;
        }
        return (
          item.connectionId === metadataConnectionId ||
          item.scope === "global" ||
          item.scope === "shared"
        );
      });
    return dedupeMetadataItems(items);
  }, [managedMetadataQuery.data, metadataConnectionId]);

  const managedMetadataIds = useMemo(
    () => new Set(managedMetadataInContextItems.map((item) => item.id)),
    [managedMetadataInContextItems]
  );

  const managedMetadataItems = useMemo(() => {
    const items = managedMetadataInContextItems.filter((item) => {
      if (includeExampleMetadata) {
        return true;
      }
      const tier = item.sourceTier?.toLowerCase();
      return tier !== "example" && tier !== "demo";
    });
    return filterMetadataItems(items, metadataSearch);
  }, [managedMetadataInContextItems, metadataSearch, includeExampleMetadata]);

  const trainManagedQueryOptions = useMemo<TrainManagedQueryOption[]>(() => {
    return managedMetadataInContextItems
      .filter((item) => {
        const tier = item.sourceTier?.toLowerCase();
        return tier === "user" && item.type.toLowerCase() === "query";
      })
      .map((item) => ({
        datapointId: item.id,
        name: item.name,
        connectionId: item.connectionId || null,
        scope: item.scope || null,
        lifecycleVersion: item.lifecycleVersion || null,
        lifecycleChangedAt: item.lifecycleChangedAt || null,
        lifecycleChangedBy: item.lifecycleChangedBy || null,
      }))
      .sort((a, b) => {
        const aTime = a.lifecycleChangedAt ? Date.parse(a.lifecycleChangedAt) : 0;
        const bTime = b.lifecycleChangedAt ? Date.parse(b.lifecycleChangedAt) : 0;
        if (aTime !== bTime) {
          return bTime - aTime;
        }
        return a.name.localeCompare(b.name);
      });
  }, [managedMetadataInContextItems]);

  const filteredTrainManagedQueryOptions = useMemo(() => {
    const query = trainHistorySearch.trim().toLowerCase();
    if (!query) {
      return trainManagedQueryOptions;
    }
    return trainManagedQueryOptions.filter((item) => {
      const haystack = `${item.name} ${item.datapointId} ${item.connectionId || ""} ${
        item.scope || ""
      } ${item.lifecycleChangedBy || ""}`.toLowerCase();
      return haystack.includes(query);
    });
  }, [trainHistorySearch, trainManagedQueryOptions]);

  const selectedTrainManagedOption = useMemo(
    () =>
      trainManagedQueryOptions.find(
        (item) => item.datapointId === trainTargetDatapointId
      ) || null,
    [trainManagedQueryOptions, trainTargetDatapointId]
  );

  const approvedMetadataItems = useMemo(() => {
    const items = (approvedMetadataQuery.data || [])
      .map((item) =>
        normalizeMetadataItem(item as unknown as Record<string, unknown>, "approved")
      )
      .filter((item) => !managedMetadataIds.has(item.id));
    return filterMetadataItems(dedupeMetadataItems(items), metadataSearch);
  }, [approvedMetadataQuery.data, managedMetadataIds, metadataSearch]);

  const allMetadataItems = useMemo(
    () => [...pendingMetadataItems, ...approvedMetadataItems, ...managedMetadataItems],
    [approvedMetadataItems, managedMetadataItems, pendingMetadataItems]
  );

  const selectedMetadataItem = useMemo(
    () =>
      allMetadataItems.find(
        (item) => `${item.status}:${item.id}` === selectedMetadataKey
      ) || null,
    [allMetadataItems, selectedMetadataKey]
  );

  const selectedMetadataDetail = useMemo(() => {
    if (!selectedMetadataKey) {
      return null;
    }
    return (
      metadataDetailCache[selectedMetadataKey] ||
      selectedMetadataItem?.payload ||
      null
    );
  }, [metadataDetailCache, selectedMetadataItem, selectedMetadataKey]);

  const metadataLoading =
    pendingMetadataQuery.isLoading ||
    approvedMetadataQuery.isLoading ||
    managedMetadataQuery.isLoading ||
    pendingMetadataQuery.isFetching ||
    approvedMetadataQuery.isFetching ||
    managedMetadataQuery.isFetching;

  const metadataError = useMemo(() => {
    const firstError =
      pendingMetadataQuery.error ||
      approvedMetadataQuery.error ||
      managedMetadataQuery.error;
    if (!firstError) {
      return null;
    }
    return firstError instanceof Error
      ? firstError.message
      : "Failed to load metadata explorer data.";
  }, [
    approvedMetadataQuery.error,
    managedMetadataQuery.error,
    pendingMetadataQuery.error,
  ]);

  useEffect(() => {
    if (
      selectedMetadataKey &&
      !allMetadataItems.some((item) => `${item.status}:${item.id}` === selectedMetadataKey)
    ) {
      setSelectedMetadataKey(null);
    }
  }, [allMetadataItems, selectedMetadataKey]);

  const handleSelectMetadataItem = useCallback(
    async (item: MetadataExplorerItem) => {
      const key = `${item.status}:${item.id}`;
      setSelectedMetadataKey(key);
      setMetadataDetailError(null);

      if (item.payload) {
        setMetadataDetailCache((current) =>
          current[key] ? current : { ...current, [key]: item.payload as Record<string, unknown> }
        );
        return;
      }

      const sourceTier = item.sourceTier?.toLowerCase();
      if (sourceTier === "example" || sourceTier === "demo") {
        setMetadataDetailCache((current) =>
          current[key]
            ? current
            : {
                ...current,
                [key]: {
                  datapoint_id: item.id,
                  type: item.type,
                  name: item.name,
                  source_tier: item.sourceTier,
                  source_path: item.sourcePath,
                  note: "This is a bundled reference datapoint and not an editable managed file.",
                },
              }
        );
        return;
      }

      if (metadataDetailCache[key]) {
        return;
      }

      setMetadataDetailLoadingKey(key);
      try {
        const datapoint = await apiClient.getDatapoint(item.id);
        setMetadataDetailCache((current) => ({ ...current, [key]: datapoint }));
      } catch (err) {
        setMetadataDetailError(
          err instanceof Error ? err.message : "Failed to load metadata detail."
        );
      } finally {
        setMetadataDetailLoadingKey((current) => (current === key ? null : current));
      }
    },
    [metadataDetailCache]
  );

  const sortedConversationHistory = useMemo(
    () =>
      [...conversationHistory]
        .sort(
          (a, b) =>
            new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
        )
        .filter((snapshot) => {
          const query = conversationSearch.trim().toLowerCase();
          if (!query) {
            return true;
          }
          const haystack = `${snapshot.title} ${snapshot.targetDatabaseId || ""}`.toLowerCase();
          return haystack.includes(query);
        }),
    [conversationHistory, conversationSearch]
  );

  const formatSnapshotTime = (value: string) => {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return "";
    }
    return date.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  };

  // Handle send message
  const handleSend = async (override?: { mode?: "nl" | "sql"; message?: string }) => {
    const requestMode = override?.mode || composerMode;
    const naturalLanguageQuery =
      requestMode === "nl" ? (override?.message ?? input).trim() : input.trim();
    const sqlQuery = requestMode === "sql" ? (override?.message ?? sqlDraft).trim() : sqlDraft.trim();
    if (isLoading || !canRunQueries) return;
    if (requestMode === "nl" && !naturalLanguageQuery) return;
    if (requestMode === "sql" && !sqlQuery) return;

    const userVisibleQuery =
      requestMode === "sql"
        ? sqlQuery
        : naturalLanguageQuery;
    const requestMessage =
      requestMode === "sql"
        ? sqlQuery
        : naturalLanguageQuery;
    const requestDatabaseId = targetDatabaseId || null;
    const canReuseConversation =
      !!conversationId &&
      !!conversationDatabaseId &&
      conversationDatabaseId === requestDatabaseId;
    const conversationHistory = canReuseConversation
      ? messages.map((m) => ({
          role: m.role,
          content: m.content,
        }))
      : [];

    setInput("");
    if (requestMode === "sql") {
      setSqlDraft("");
    }
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
    setLoading(true);
    setThinkingNotes([]);
    resetAgentStatus();
    if (!canReuseConversation) {
      setConversationId(null);
      setSessionMemory(null, null);
    }

    addMessage({
      role: "user",
      content: userVisibleQuery,
    });

    addMessage({
      role: "assistant",
      content: "",
    });

    try {
      wsClient.streamChat(
        {
          message: requestMessage,
          conversation_id: canReuseConversation ? conversationId || undefined : undefined,
          target_database: requestDatabaseId || undefined,
          conversation_history: conversationHistory,
          session_summary: canReuseConversation ? sessionSummary : undefined,
          session_state: canReuseConversation ? sessionState : undefined,
          synthesize_simple_sql: synthesizeSimpleSql,
          workflow_mode: "auto",
          ...(requestMode === "sql"
            ? {
                execution_mode: "direct_sql" as const,
                sql: sqlQuery,
              }
            : {}),
        },
        {
          onOpen: () => {
            setConnected(true);
          },
          onClose: () => {
            setConnected(false);
            setLoading(false);
            setThinkingNotes([]);
            restoreInputFocus();
          },
          onAgentUpdate: (update) => {
            setAgentUpdate(update);
          },
          onThinking: (note) => {
            if (!showLiveReasoning) return;
            setThinkingNotes((prev) => {
              if (!note.trim()) return prev;
              if (prev[prev.length - 1] === note) return prev;
              return [...prev.slice(-7), note];
            });
          },
          onAnswerChunk: (chunk) => {
            appendToLastMessage(chunk);
          },
          onComplete: (response) => {
            const nextConversationId = response.conversation_id || conversationId || null;
            const nextSummary = response.session_summary || null;
            const nextState = response.session_state || null;
            updateLastMessage({
              content: response.answer,
              clarifying_questions: response.clarifying_questions,
              sub_answers: response.sub_answers,
              sql: response.sql,
              data: response.data,
              visualization_hint: response.visualization_hint,
              visualization_metadata: response.visualization_metadata,
              sources: response.sources,
              answer_source: response.answer_source,
              answer_confidence: response.answer_confidence,
              evidence: response.evidence,
              metrics: response.metrics,
              tool_approval_required: response.tool_approval_required,
              tool_approval_message: response.tool_approval_message,
              tool_approval_calls: response.tool_approval_calls,
              workflow_artifacts: response.workflow_artifacts,
              decision_trace: response.decision_trace,
              action_trace: response.action_trace,
              loop_terminal_state: response.loop_terminal_state,
              loop_stop_reason: response.loop_stop_reason,
              loop_shadow_decisions: response.loop_shadow_decisions,
            });
            if (response.conversation_id) {
              setConversationId(response.conversation_id);
            }
            setSessionMemory(nextSummary, nextState);
            setConversationDatabaseId(requestDatabaseId);
            const currentMessages = useChatStore.getState().messages;
            upsertConversationSnapshot({
              messages: currentMessages,
              conversationId: nextConversationId,
              sessionSummary: nextSummary,
              sessionState: nextState,
              targetDatabaseId: requestDatabaseId,
            });
            if (response.tool_approval_required && response.tool_approval_calls?.length) {
              setToolApprovalCalls(response.tool_approval_calls);
              setToolApprovalMessage(
                response.tool_approval_message ||
                  "Approval required to run the requested tool."
              );
              setToolApprovalOpen(true);
            }
            setLoading(false);
            setThinkingNotes([]);
            resetAgentStatus();
            restoreInputFocus();
          },
          onError: (message) => {
            setError(message);
            setErrorCategory(categorizeError(message));
            setLastFailedQuery(
              requestMode === "sql" ? `__sql__${sqlQuery}` : naturalLanguageQuery
            );
            setRetryCount((c) => c + 1);
            setLoading(false);
            setThinkingNotes([]);
            resetAgentStatus();
            restoreInputFocus();
          },
          onSystemNotInitialized: (steps, message) => {
            const setupMessage =
              message ||
              "Not initialized yet. Complete onboarding to connect a target database, then ask your first question.";
            updateLastMessage({
              content: setupMessage,
            });
            setIsInitialized(false);
            setSetupSteps(steps);
            setSetupError(setupMessage);
            setLoading(false);
            setThinkingNotes([]);
            resetAgentStatus();
            restoreInputFocus();
          },
        }
      );
    } catch (err) {
      console.error("Chat error:", err);
      const errorMessage = err instanceof Error ? err.message : "Failed to send message";
      setError(errorMessage);
      setErrorCategory(categorizeError(errorMessage));
      setLastFailedQuery(
        requestMode === "sql" ? `__sql__${sqlQuery}` : naturalLanguageQuery
      );
      setRetryCount((c) => c + 1);
      setLoading(false);
      resetAgentStatus();
      restoreInputFocus();
    }
  };

  const handleRetry = () => {
    if (!lastFailedQuery || isLoading) return;
    if (lastFailedQuery.startsWith("__sql__")) {
      setComposerMode("sql");
      setSqlDraft(lastFailedQuery.replace("__sql__", ""));
      restoreInputFocus("sql");
    } else {
      setInput(lastFailedQuery);
      setComposerMode("nl");
      restoreInputFocus("nl");
    }
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
  };

  const handleApplyTemplate = (templateId: string) => {
    const template = QUERY_TEMPLATES.find((item) => item.id === templateId);
    if (!template) {
      return;
    }
    setComposerMode("nl");
    setInput(template.build(selectedSchemaTable));
    restoreInputFocus("nl");
  };

  const handleOpenSqlEditor = (sql: string) => {
    setComposerMode("sql");
    setSqlDraft(sql);
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
    restoreInputFocus("sql");
  };

  const findSourceQuestionForMessage = useCallback(
    (messageId: string): string => {
      const index = messages.findIndex((item) => item.id === messageId);
      if (index < 0) {
        return "";
      }
      for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
        if (messages[cursor]?.role === "user") {
          return messages[cursor]?.content?.trim() || "";
        }
      }
      return "";
    },
    [messages]
  );

  const handleOpenTrainModal = (payload: MessageTrainPayload) => {
    const fallbackQuestion = findSourceQuestionForMessage(payload.message_id);
    const nextQuestion = (payload.question || fallbackQuestion || "").trim();
    const related = extractRelatedTablesFromSql(payload.sql || "");
    setTrainMode("create");
    setTrainTargetDatapointId("");
    setTrainHistorySearch("");
    setTrainQuestion(nextQuestion);
    setTrainSql((payload.sql || "").trim());
    setTrainName(
      nextQuestion
        ? `User-trained: ${nextQuestion.slice(0, 80)}`
        : "User-trained query datapoint"
    );
    setTrainNotes("");
    setTrainRelatedTables(related.join(", "));
    setTrainLoadingExisting(false);
    setTrainSyncing(false);
    setTrainError(null);
    setTrainNotice(null);
    setTrainModalOpen(true);
  };

  useEffect(() => {
    if (!trainModalOpen || trainMode !== "update") {
      return;
    }
    if (!trainManagedQueryOptions.length) {
      setTrainError("No existing user-trained query datapoints found yet. Use Create New.");
      return;
    }

    const nextDatapointId = trainTargetDatapointId || trainManagedQueryOptions[0].datapointId;
    if (!trainTargetDatapointId) {
      setTrainTargetDatapointId(nextDatapointId);
      return;
    }

    let cancelled = false;
    const loadExistingDatapoint = async () => {
      setTrainLoadingExisting(true);
      setTrainError(null);
      try {
        const datapoint = await apiClient.getDatapoint(nextDatapointId);
        if (cancelled) {
          return;
        }
        const metadata =
          datapoint.metadata && typeof datapoint.metadata === "object"
            ? (datapoint.metadata as Record<string, unknown>)
            : {};
        const loadedQuestion =
          typeof metadata.trained_from_question === "string"
            ? metadata.trained_from_question.trim()
            : "";
        const loadedSql =
          typeof datapoint.sql_template === "string" ? datapoint.sql_template.trim() : "";
        const loadedName = typeof datapoint.name === "string" ? datapoint.name.trim() : "";
        const loadedNotes =
          typeof metadata.training_note === "string"
            ? metadata.training_note
            : typeof datapoint.description === "string"
              ? datapoint.description
              : "";
        const loadedRelated = Array.isArray(datapoint.related_tables)
          ? datapoint.related_tables
              .filter((entry): entry is string => typeof entry === "string")
              .join(", ")
          : "";

        setTrainQuestion((current) => loadedQuestion || current);
        setTrainSql((current) => loadedSql || current);
        setTrainName((current) => loadedName || current);
        setTrainNotes((current) => loadedNotes || current);
        setTrainRelatedTables((current) =>
          loadedRelated || extractRelatedTablesFromSql(loadedSql || current).join(", ")
        );
      } catch (err) {
        if (cancelled) {
          return;
        }
        setTrainError(
          err instanceof Error ? err.message : "Failed to load selected datapoint."
        );
      } finally {
        if (!cancelled) {
          setTrainLoadingExisting(false);
        }
      }
    };

    void loadExistingDatapoint();

    return () => {
      cancelled = true;
    };
  }, [trainManagedQueryOptions, trainModalOpen, trainMode, trainTargetDatapointId]);

  const waitForSyncCompletion = useCallback(async (jobId: string): Promise<void> => {
    const timeoutMs = 60_000;
    const pollIntervalMs = 1_000;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const status = await apiClient.getSyncStatus();
      if (status.job_id === jobId) {
        if (status.status === "completed") {
          return;
        }
        if (status.status === "failed") {
          throw new Error(status.error || "Sync failed while indexing training datapoint.");
        }
      }
      await sleep(pollIntervalMs);
    }
    throw new Error("Sync is still running. Retry may not use the latest training yet.");
  }, []);

  const handleSubmitTrainDatapoint = async () => {
    const sql = trainSql.trim();
    const question = trainQuestion.trim();
    const isUpdateMode = trainMode === "update";
    if (!sql) {
      setTrainError("SQL is required to train a query datapoint.");
      return;
    }
    if (!question) {
      setTrainError("Question context is required.");
      return;
    }
    if (isUpdateMode && !trainTargetDatapointId) {
      setTrainError("Select an existing datapoint to update.");
      return;
    }

    const relatedFromText = trainRelatedTables
      .split(",")
      .map((entry) => entry.trim().toLowerCase())
      .filter(Boolean);
    const relatedTables = Array.from(
      new Set([...relatedFromText, ...extractRelatedTablesFromSql(sql)])
    ).slice(0, 20);

    const connectionId =
      (isUpdateMode ? selectedTrainManagedOption?.connectionId || null : null) ||
      (metadataConnectionId &&
      metadataConnectionId !== ENV_DATABASE_CONNECTION_ID
        ? metadataConnectionId
        : null);

    const datapointId = isUpdateMode
      ? trainTargetDatapointId
      : `query_user_trained_${slugifyDatapointToken(question)}_${String(Date.now()).slice(-6)}`;
    const trainingNote = trainNotes.trim();
    const payload: Record<string, unknown> = {
      datapoint_id: datapointId,
      type: "Query",
      name: trainName.trim() || `User-trained query: ${question.slice(0, 80)}`,
      owner: "trainer@datachat.local",
      tags: ["user", "training", "query-template"],
      metadata: {
        source: "ui_train_datapoint",
        source_tier: "user",
        scope: connectionId ? "database" : "global",
        connection_id: connectionId,
        grain: "product_store_latest_snapshot",
        exclusions:
          "Anchored to latest available snapshot week; does not include forward demand forecasting or supplier lead-time risk.",
        confidence_notes:
          "User-trained datapoint from chat correction loop. Validate against business policy before external reporting.",
        trained_from_question: question,
        training_note: trainingNote || undefined,
      },
      description:
        trainingNote ||
        `User-trained query datapoint for: ${question}. Added from chat feedback loop.`,
      sql_template: sql,
      parameters: {},
      validation: {
        max_rows: 1000,
      },
      related_tables: relatedTables,
    };

    try {
      setTrainSubmitting(true);
      setTrainSyncing(false);
      setTrainError(null);
      if (isUpdateMode) {
        await apiClient.updateDatapoint(datapointId, payload);
      } else {
        await apiClient.createDatapoint(payload);
      }

      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["metadata-managed"] }),
        queryClient.invalidateQueries({ queryKey: ["metadata-approved"] }),
        queryClient.invalidateQueries({ queryKey: ["metadata-pending"] }),
      ]);

      let syncWarning: string | null = null;
      try {
        setTrainSyncing(true);
        const syncJob = await apiClient.triggerSync({
          ...(connectionId
            ? { scope: "database" as const, connection_id: connectionId }
            : { scope: "auto" as const }),
          conflict_mode: "prefer_latest",
        });
        await waitForSyncCompletion(syncJob.job_id);
      } catch (syncErr) {
        syncWarning =
          syncErr instanceof Error
            ? syncErr.message
            : "Sync is still in progress. Retry may not use the latest training yet.";
      } finally {
        setTrainSyncing(false);
      }

      setTrainNotice(
        syncWarning
          ? `${isUpdateMode ? "Datapoint updated" : "Datapoint created"}, but sync is still catching up.`
          : `${isUpdateMode ? "Datapoint updated" : "Datapoint created"} and indexed. Re-running your question...`
      );
      setTrainModalOpen(false);
      setTrainMode("create");
      setTrainTargetDatapointId("");
      setComposerMode("nl");
      await handleSend({ mode: "nl", message: question });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to save training datapoint.";
      setTrainError(message);
    } finally {
      setTrainSyncing(false);
      setTrainSubmitting(false);
    }
  };

  const handleStartNewConversation = () => {
    upsertConversationSnapshot();
    clearMessages();
    setConversationDatabaseId(null);
    setConversationId(null);
    setSessionMemory(null, null);
    setInput("");
    setSqlDraft("");
    setComposerMode("nl");
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
    setRetryCount(0);
    restoreInputFocus("nl");
  };

  const handleLoadConversation = (snapshot: ConversationSnapshot) => {
    const restoredMessages = deserializeMessages(snapshot.messages);
    loadSession({
      frontendSessionId: snapshot.frontendSessionId,
      messages: restoredMessages,
      conversationId: snapshot.conversationId,
      sessionSummary: snapshot.sessionSummary,
      sessionState: snapshot.sessionState,
    });
    setConversationDatabaseId(snapshot.targetDatabaseId);
    setTargetDatabaseId(snapshot.targetDatabaseId);
    setInput("");
    setSqlDraft("");
    setComposerMode("nl");
    setError(null);
    setErrorCategory(null);
    setLastFailedQuery(null);
    setRetryCount(0);
    restoreInputFocus("nl");
  };

  const handleDeleteConversation = (sessionId: string) => {
    const deletingActiveConversation = sessionId === frontendSessionId;
    const remaining = conversationHistory.filter(
      (entry) => entry.frontendSessionId !== sessionId
    );
    persistConversationHistory(remaining);
    void apiClient.deleteConversation(sessionId).catch(() => {
      // Local storage fallback stays active when backend persistence is unavailable.
    });
    queryClient.setQueryData(
      ["ui-conversations"],
      (existing: ConversationSnapshotPayload[] | undefined) =>
        (existing || []).filter((item) => item.frontend_session_id !== sessionId)
    );
    if (deletingActiveConversation) {
      clearMessages();
      setConversationDatabaseId(null);
      setInput("");
      setSqlDraft("");
      setComposerMode("nl");
      setError(null);
      setErrorCategory(null);
      setLastFailedQuery(null);
      setRetryCount(0);
      restoreInputFocus("nl");
    }
  };

  const handleSchemaSearchChange = (value: string) => {
    setSchemaSearch(value);
    if (value.trim()) {
      trackSchemaDiscovery("schema_search", { search_length: value.trim().length });
    }
  };

  const handleSchemaSelectTable = (fullName: string) => {
    setSelectedSchemaTable(fullName);
    trackSchemaDiscovery("schema_table_open", { table: fullName });
  };

  const handleSchemaUseTable = (fullName: string) => {
    setSelectedSchemaTable(fullName);
    trackSchemaDiscovery("schema_use_in_query", { table: fullName, composer_mode: composerMode });
    if (composerMode === "sql") {
      setSqlDraft(`SELECT *\nFROM ${fullName}\nLIMIT 100;`);
      restoreInputFocus("sql");
      return;
    }
    setInput(`Show first 100 rows from ${fullName}.`);
    restoreInputFocus("nl");
  };

  const handleSubmitFeedback = async (payload: MessageFeedbackPayload) => {
    await apiClient.submitFeedback({
      ...payload,
      conversation_id: conversationId,
      target_database_id: conversationDatabaseId || targetDatabaseId,
      metadata: {
        frontend_session_id: frontendSessionId,
        display_mode: resultLayoutMode,
      },
    });
  };

  const handleApproveTools = async () => {
    setToolApprovalError(null);
    setToolApprovalRunning(true);
    try {
      for (const call of toolApprovalCalls) {
        const result = await apiClient.executeTool({
          name: call.name,
          arguments: call.arguments || {},
          approved: true,
        });
        const payload = result.result || {};
        const summary =
          (payload as Record<string, unknown>).message ||
          (payload as Record<string, unknown>).answer ||
          `Tool ${call.name} completed.`;
        addMessage({
          role: "assistant",
          content: String(summary),
        });
      }
      setToolApprovalOpen(false);
      setToolApprovalCalls([]);
      setToolApprovalMessage(null);
    } catch (err) {
      setToolApprovalError((err as Error).message);
    } finally {
      setToolApprovalRunning(false);
    }
  };

  const toolCostEstimate = () => {
    if (!toolApprovalCalls.length) {
      return null;
    }
    const estimates = toolApprovalCalls.map((call) => {
      if (call.name === "profile_and_generate_datapoints") {
        const args = call.arguments || {};
        const batchSize = Number(args.batch_size || 10);
        const maxTables = args.max_tables ? Number(args.max_tables) : null;
        const tableCount = maxTables || "unknown";
        const batches =
          typeof tableCount === "number"
            ? Math.ceil(tableCount / batchSize)
            : "unknown";
        return {
          tool: call.name,
          tables: tableCount,
          batchSize,
          batches,
          llmCalls: typeof batches === "number" ? batches : "unknown",
        };
      }
      return { tool: call.name };
    });
    return estimates;
  };

  // Handle key press (Enter to send)
  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleSqlEditorKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      handleSend();
    }
  };

  useEffect(() => {
    if (toolApprovalOpen) {
      window.requestAnimationFrame(() => {
        toolApprovalApproveButtonRef.current?.focus();
      });
    }
  }, [toolApprovalOpen]);

  useEffect(() => {
    if (shortcutsOpen) {
      window.requestAnimationFrame(() => {
        shortcutsCloseButtonRef.current?.focus();
      });
    }
  }, [shortcutsOpen]);

  useEffect(() => {
    if (!trainNotice) {
      return;
    }
    const timeout = window.setTimeout(() => setTrainNotice(null), 6000);
    return () => window.clearTimeout(timeout);
  }, [trainNotice]);

  useEffect(() => {
    if (!trainModalOpen) {
      return;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [trainModalOpen]);

  useEffect(() => {
    const handleGlobalShortcuts = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const tagName = target?.tagName?.toLowerCase();
      const isEditable =
        !!target &&
        (target.isContentEditable ||
          tagName === "input" ||
          tagName === "textarea" ||
          tagName === "select");
      const hasModifier = event.metaKey || event.ctrlKey;
      const key = event.key.toLowerCase();

      if (event.key === "Escape") {
        if (trainModalOpen) {
          event.preventDefault();
          if (!trainSubmitting) {
            setTrainModalOpen(false);
            setTrainError(null);
          }
          restoreInputFocus();
          return;
        }
        if (toolApprovalOpen) {
          event.preventDefault();
          setToolApprovalOpen(false);
          restoreInputFocus();
          return;
        }
        if (shortcutsOpen) {
          event.preventDefault();
          setShortcutsOpen(false);
          restoreInputFocus();
          return;
        }
      }

      if (hasModifier && key === "k") {
        event.preventDefault();
        restoreInputFocus();
        return;
      }

      if (trainModalOpen) {
        return;
      }

      if (hasModifier && key === "h") {
        event.preventDefault();
        setIsHistorySidebarOpen((prev) => !prev);
        return;
      }

      if (hasModifier && key === "e") {
        event.preventDefault();
        setIsSchemaSidebarOpen((prev) => !prev);
        return;
      }

      if (hasModifier && key === "/") {
        event.preventDefault();
        setShortcutsOpen((prev) => !prev);
        return;
      }

      if (!hasModifier && !isEditable && event.key === "/") {
        event.preventDefault();
        restoreInputFocus();
      }
    };

    window.addEventListener("keydown", handleGlobalShortcuts);
    return () => {
      window.removeEventListener("keydown", handleGlobalShortcuts);
    };
  }, [restoreInputFocus, shortcutsOpen, toolApprovalOpen, trainModalOpen, trainSubmitting]);

  const handleInitialize = async (
    databaseUrl: string,
    autoProfile: boolean,
    systemDatabaseUrl?: string
  ) => {
    setSetupError(null);
    setSetupNotice(null);
    setIsInitializing(true);
    try {
      const response = await apiClient.systemInitialize({
        database_url: databaseUrl,
        system_database_url: systemDatabaseUrl,
        auto_profile: autoProfile,
      });
      setIsInitialized(response.is_initialized);
      setSetupSteps(response.setup_required || []);
      if (response.message) {
        setSetupNotice(response.message);
        if (response.message.toLowerCase().includes("initialization completed")) {
          setSetupCompleted(true);
          router.push("/databases");
        }
      }
    } catch (err) {
      console.error("Initialization error:", err);
      setSetupError(
        err instanceof Error ? err.message : "Initialization failed"
      );
    } finally {
      setIsInitializing(false);
    }
  };

  return (
    <div
      className="flex h-full min-h-0 bg-gradient-to-b from-background via-background to-muted/20"
      role="main"
      aria-label="DataChat workspace"
    >
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex-shrink-0 border-b border-border/70 bg-background/90 p-4 backdrop-blur" role="banner">
          <div className="flex items-center justify-between gap-3">
            <div className="flex min-w-0 items-center gap-2">
              <div className="min-w-0">
                <h1 className="text-2xl font-bold">DataChat</h1>
                <p className="truncate text-sm text-muted-foreground">
                  Decision workflows for business decision makers
                </p>
              </div>
            </div>
            <div className="flex min-w-0 flex-1 items-center justify-end gap-2 overflow-x-auto py-1 whitespace-nowrap [&>*]:shrink-0">
              {connections.length > 0 && (
                <select
                  value={targetDatabaseId ?? ""}
                  onChange={(event) => {
                    const nextId = event.target.value || null;
                    if (nextId !== targetDatabaseId) {
                      setConversationId(null);
                      setConversationDatabaseId(null);
                      resetAgentStatus();
                    }
                    setTargetDatabaseId(nextId);
                  }}
                  className="h-8 rounded-md border border-input bg-background px-2 text-xs"
                  disabled={isLoading}
                  aria-label="Target database"
                >
                  {connections.map((connection) => (
                    <option key={connection.connection_id} value={connection.connection_id}>
                      {connection.name}
                      {` (${connection.database_type})`}
                      {connection.tags?.includes("env") ? " (env)" : ""}
                      {connection.is_default ? " (default)" : ""}
                    </option>
                  ))}
                </select>
              )}
              <Button
                variant="outline"
                size="icon"
                onClick={handleStartNewConversation}
                disabled={isLoading}
                aria-label="Start new conversation"
                title="Start new conversation"
              >
                <Plus size={14} />
              </Button>
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setShortcutsOpen(true)}
                aria-label="Open keyboard shortcuts"
                title="Keyboard shortcuts"
              >
                <Keyboard size={14} />
              </Button>
              <Button asChild variant="ghost" size="sm">
                <Link href="/settings">Settings</Link>
              </Button>
              <Button asChild variant="secondary" size="sm">
                <Link href="/databases">Manage DataPoints</Link>
              </Button>
              <div className="flex items-center gap-2 text-xs">
                <div
                  className={`h-2 w-2 rounded-full ${
                    isConnected || isBackendReachable ? "bg-green-500" : "bg-red-500"
                  }`}
                />
                {isLoading && <Loader2 className="h-3 w-3 animate-spin text-primary" />}
                <span className="text-muted-foreground" role="status" aria-live="polite">
                  {isLoading
                    ? formatWaitingChipLabel(loadingElapsedSeconds)
                    : isConnected
                      ? "Streaming"
                      : isBackendReachable
                        ? "Ready"
                        : "Disconnected"}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className="flex min-h-0 flex-1">
          <ConversationHistorySidebar
            isOpen={isHistorySidebarOpen}
            frontendSessionId={frontendSessionId}
            conversationSearch={conversationSearch}
            sortedConversationHistory={sortedConversationHistory}
            formatSnapshotTime={formatSnapshotTime}
            onToggle={() => setIsHistorySidebarOpen((prev) => !prev)}
            onStartNewConversation={handleStartNewConversation}
            onSearchChange={setConversationSearch}
            onLoadConversation={handleLoadConversation}
            onDeleteConversation={handleDeleteConversation}
          />

          <div className="flex min-w-0 flex-1 flex-col">
            <div className="flex-1 overflow-y-auto px-4 py-5">
              <section
                role="log"
                aria-live="polite"
                aria-relevant="additions text"
                aria-label="Chat message stream"
                className="mx-auto w-full max-w-5xl"
              >
              {!canRunQueries && !setupCompleted && (
                <SystemSetup
                  steps={setupSteps}
                  onInitialize={handleInitialize}
                  isSubmitting={isInitializing}
                  error={setupError}
                  notice={setupNotice}
                />
              )}
              {!canRunQueries && setupCompleted && (
                <div className="mb-4 rounded-md border border-muted bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                  Setup saved. Add DataPoints from{" "}
                  <Link href="/databases" className="underline">
                    Database Manager
                  </Link>{" "}
                  (or run <strong>datachat demo</strong>) to enable chat.
                </div>
              )}
              {trainNotice && (
                <div className="mb-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
                  {trainNotice}
                </div>
              )}
              {messages.length === 0 && (
                <div className="flex h-full items-center justify-center text-muted-foreground">
                  <div className="text-center">
                    <p className="mb-2 text-lg">Welcome to DataChat!</p>
                    <p className="text-sm">
                      Ask a question about your data to get started.
                    </p>
                    <p className="mt-2 text-xs text-muted-foreground">
                      New here? Run <strong>datachat demo</strong> to load sample data.
                    </p>
                  </div>
                </div>
              )}

              {messages.map((message, index) => {
                let sourceQuestion = "";
                for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
                  if (messages[cursor]?.role === "user") {
                    sourceQuestion = messages[cursor]?.content || "";
                    break;
                  }
                }
                return (
                  <Message
                    key={message.id}
                    message={message}
                    sourceQuestion={sourceQuestion}
                    displayMode={resultLayoutMode}
                    showAgentTimingBreakdown={showAgentTimingBreakdown}
                    onEditSqlDraft={handleOpenSqlEditor}
                    onTrainDatapoint={handleOpenTrainModal}
                    onSubmitFeedback={handleSubmitFeedback}
                    onClarifyingAnswer={(question) => {
                      setComposerMode("nl");
                      setInput(`Regarding "${question}": `);
                      restoreInputFocus("nl");
                    }}
                  />
                );
              })}

              {isLoading && showLiveReasoning && (
                <Card className="mb-4 border-primary/20 bg-primary/5">
                  <div className="p-3">
                    <div className="mb-2 text-xs font-medium text-primary">Working...</div>
                    <ul className="space-y-1 text-xs text-muted-foreground">
                      {(thinkingNotes.length
                        ? thinkingNotes
                        : ["Understanding your request..."]).map((note, idx) => (
                        <li key={`${idx}-${note}`} className="flex items-start gap-2">
                          <span className="mt-1 inline-block h-1.5 w-1.5 rounded-full bg-primary/70" />
                          <span>{note}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </Card>
              )}

              <AgentStatus />
              {isLoading && (
                <div className="mb-4 flex items-center justify-center">
                  <div className="inline-flex items-center gap-2 rounded-full border border-primary/25 bg-primary/5 px-3 py-1 text-xs text-primary">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    <span>{formatWaitingChipLabel(loadingElapsedSeconds)}</span>
                  </div>
                </div>
              )}

              {error && errorCategory && (
                <Card className="mb-4 border-destructive bg-destructive/10">
                  <div className="p-4">
                    <div className="flex items-start gap-3">
                      {(() => {
                        const Icon = getErrorIcon(errorCategory);
                        return (
                          <Icon className="mt-0.5 h-5 w-5 flex-shrink-0 text-destructive" />
                        );
                      })()}
                      <div className="flex-1">
                        <div className="mb-1 flex items-center gap-2">
                          <p className="text-sm font-medium text-destructive">
                            {errorCategory.charAt(0).toUpperCase() + errorCategory.slice(1)} Error
                          </p>
                          {retryCount > 0 && (
                            <span className="text-xs text-muted-foreground">
                              (attempt {retryCount})
                            </span>
                          )}
                        </div>
                        <p className="text-sm text-muted-foreground">{error}</p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {getErrorSuggestion(errorCategory)}
                        </p>
                      </div>
                    </div>
                    <div className="mt-3 flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={handleRetry}
                        disabled={isLoading || !lastFailedQuery}
                        className="text-xs"
                      >
                        <RefreshCw size={14} className="mr-1" />
                        Retry Query
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => {
                          setError(null);
                          setErrorCategory(null);
                          setLastFailedQuery(null);
                          setRetryCount(0);
                        }}
                        className="text-xs text-muted-foreground"
                      >
                        Dismiss
                      </Button>
                    </div>
                  </div>
                </Card>
              )}
              <div ref={messagesEndRef} />
              </section>
            </div>

            <div className="flex-shrink-0 border-t border-border/70 bg-background/90 p-4 backdrop-blur">
              <div className="mb-3 flex flex-wrap gap-2">
                {QUERY_TEMPLATES.map((template) => (
                  <button
                    key={template.id}
                    type="button"
                    onClick={() => handleApplyTemplate(template.id)}
                    className="rounded-full border border-border/70 bg-background/80 px-3 py-1 text-xs text-foreground/90 shadow-sm transition hover:bg-muted"
                    disabled={isLoading || !canRunQueries}
                  >
                    {template.label}
                  </button>
                ))}
                {selectedSchemaTable && (
                  <span className="inline-flex items-center rounded-full bg-primary/10 px-3 py-1 text-xs text-primary">
                    Table: {selectedSchemaTable}
                  </span>
                )}
              </div>
              <div className="mb-2 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => setComposerMode("nl")}
                  className={`inline-flex items-center gap-1 rounded-md border px-2.5 py-1 text-xs transition ${
                    composerMode === "nl"
                      ? "border-primary/40 bg-primary/10 text-primary"
                      : "border-border/70 bg-background/80 text-muted-foreground hover:bg-muted"
                  }`}
                >
                  Ask
                </button>
                <button
                  type="button"
                  onClick={() => setComposerMode("sql")}
                  className={`inline-flex items-center gap-1 rounded-md border px-2.5 py-1 text-xs transition ${
                    composerMode === "sql"
                      ? "border-primary/40 bg-primary/10 text-primary"
                      : "border-border/70 bg-background/80 text-muted-foreground hover:bg-muted"
                  }`}
                >
                  <FileCode2 size={12} />
                  SQL Editor
                </button>
              </div>
              {composerMode === "nl" ? (
                <div className="flex gap-2">
                  <Input
                    ref={inputRef}
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyPress}
                    placeholder="Ask a question about your data..."
                    disabled={isLoading || !canRunQueries}
                    className="flex-1"
                    aria-label="Chat query input"
                  />
                  <Button
                    onClick={handleSend}
                    disabled={!input.trim() || isLoading || !canRunQueries}
                    size="icon"
                    aria-label="Send chat message"
                  >
                    {isLoading ? (
                      <Loader2 size={18} className="animate-spin" />
                    ) : (
                      <Send size={18} />
                    )}
                  </Button>
                </div>
              ) : (
                <div className="space-y-2">
                  <textarea
                    ref={sqlEditorRef}
                    value={sqlDraft}
                    onChange={(event) => setSqlDraft(event.target.value)}
                    onKeyDown={handleSqlEditorKeyPress}
                    placeholder="SELECT * FROM your_table LIMIT 10;"
                    disabled={isLoading || !canRunQueries}
                    className="min-h-[120px] w-full resize-y rounded-md border border-input bg-background px-3 py-2 font-mono text-xs leading-relaxed outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                    aria-label="SQL editor input"
                  />
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-xs text-muted-foreground">
                      Press Ctrl/Cmd + Enter to run SQL
                    </p>
                    <Button
                      onClick={handleSend}
                      disabled={!sqlDraft.trim() || isLoading || !canRunQueries}
                      size="sm"
                      aria-label="Run SQL draft"
                    >
                      {isLoading ? (
                        <Loader2 size={14} className="animate-spin" />
                      ) : (
                        <Send size={14} />
                      )}
                      Run SQL
                    </Button>
                  </div>
                </div>
              )}
              <p className="mt-2 text-xs text-muted-foreground">
                {composerMode === "nl"
                  ? "Press Enter to send"
                  : "SQL editor runs your SQL directly (read-only)."}
              </p>
              {conversationId &&
                conversationDatabaseId &&
                targetDatabaseId &&
                conversationDatabaseId !== targetDatabaseId && (
                  <p className="mt-1 text-xs text-amber-700">
                    Data source changed. Next query starts a fresh conversation context.
                  </p>
                )}
            </div>
          </div>

          <SchemaExplorerSidebar
            isOpen={isSchemaSidebarOpen}
            explorerMode={explorerMode}
            schemaSearch={schemaSearch}
            metadataSearch={metadataSearch}
            schemaLoading={schemaLoading}
            metadataLoading={metadataLoading}
            schemaError={schemaError}
            metadataError={metadataError}
            filteredSchemaTables={filteredSchemaTables}
            pendingMetadataItems={pendingMetadataItems}
            approvedMetadataItems={approvedMetadataItems}
            managedMetadataItems={managedMetadataItems}
            selectedMetadataKey={selectedMetadataKey}
            metadataDetail={selectedMetadataDetail}
            metadataDetailLoading={Boolean(
              selectedMetadataKey && metadataDetailLoadingKey === selectedMetadataKey
            )}
            metadataDetailError={metadataDetailError}
            metadataContextNote={metadataContextNote}
            selectedSchemaTable={selectedSchemaTable}
            onToggle={() => setIsSchemaSidebarOpen((prev) => !prev)}
            onExplorerModeChange={setExplorerMode}
            onSearchChange={handleSchemaSearchChange}
            onMetadataSearchChange={setMetadataSearch}
            includeExampleMetadata={includeExampleMetadata}
            onIncludeExampleMetadataChange={setIncludeExampleMetadata}
            onSelectMetadataItem={handleSelectMetadataItem}
            onSelectTable={handleSchemaSelectTable}
            onUseTable={handleSchemaUseTable}
          />
        </div>
      </div>
      {isClientMounted &&
        trainModalOpen &&
        createPortal(
          <div
            className="fixed inset-0 z-[100] overflow-y-auto bg-black/50 p-4"
            role="dialog"
            aria-modal="true"
            aria-label="Train DataChat"
            onClick={() => {
              if (!trainSubmitting) {
                setTrainModalOpen(false);
                setTrainError(null);
              }
            }}
          >
            <div className="flex min-h-full items-start justify-center py-6">
              <div
                className="w-full max-w-2xl rounded-lg bg-background p-6 shadow-xl"
                onClick={(event) => event.stopPropagation()}
              >
                <h3 className="text-base font-semibold">Train DataChat</h3>
                <p className="mt-1 text-xs text-muted-foreground">
                  Create or update a managed query datapoint, sync it, then retry the question.
                </p>

                <div className="mt-4 flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setTrainMode("create");
                      setTrainTargetDatapointId("");
                      setTrainError(null);
                    }}
                    className={`rounded-md border px-3 py-1.5 text-xs font-medium ${
                      trainMode === "create"
                        ? "border-primary/50 bg-primary/10 text-primary"
                        : "border-border text-muted-foreground hover:bg-muted"
                    }`}
                    disabled={trainSubmitting}
                  >
                    Create New
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setTrainMode("update");
                      setTrainHistorySearch("");
                      setTrainError(null);
                      if (!trainTargetDatapointId && trainManagedQueryOptions.length > 0) {
                        setTrainTargetDatapointId(trainManagedQueryOptions[0].datapointId);
                      }
                    }}
                    className={`rounded-md border px-3 py-1.5 text-xs font-medium ${
                      trainMode === "update"
                        ? "border-primary/50 bg-primary/10 text-primary"
                        : "border-border text-muted-foreground hover:bg-muted"
                    }`}
                    disabled={trainSubmitting}
                  >
                    Update Existing
                  </button>
                </div>

                {trainMode === "update" && (
                  <div className="mt-3 space-y-2">
                    <div className="text-xs font-medium text-foreground">Training history</div>
                    <Input
                      value={trainHistorySearch}
                      onChange={(event) => setTrainHistorySearch(event.target.value)}
                      placeholder="Search by name, id, scope, or updater"
                      className="h-8 text-xs"
                      disabled={trainSubmitting || trainLoadingExisting}
                    />
                    <div className="max-h-40 space-y-2 overflow-y-auto rounded-md border border-border/70 bg-muted/20 p-2">
                      {!filteredTrainManagedQueryOptions.length ? (
                        <div className="px-2 py-1 text-xs text-muted-foreground">
                          No matching user-trained query datapoints.
                        </div>
                      ) : (
                        filteredTrainManagedQueryOptions.map((item) => (
                          <button
                            key={item.datapointId}
                            type="button"
                            onClick={() => {
                              setTrainTargetDatapointId(item.datapointId);
                              setTrainError(null);
                            }}
                            disabled={trainSubmitting || trainLoadingExisting}
                            className={`w-full rounded-md border px-2 py-2 text-left transition ${
                              item.datapointId === trainTargetDatapointId
                                ? "border-primary/50 bg-primary/10"
                                : "border-border/60 bg-background hover:bg-muted/60"
                            }`}
                          >
                            <div className="truncate text-xs font-medium text-foreground">
                              {item.name}
                            </div>
                            <div className="truncate text-[11px] text-muted-foreground">
                              {item.datapointId}
                            </div>
                            <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-muted-foreground">
                              <span>Updated: {formatTrainHistoryTimestamp(item.lifecycleChangedAt)}</span>
                              {item.lifecycleVersion ? <span>v{item.lifecycleVersion}</span> : null}
                              {item.scope ? <span>scope:{item.scope}</span> : null}
                              {item.lifecycleChangedBy ? <span>by:{item.lifecycleChangedBy}</span> : null}
                            </div>
                          </button>
                        ))
                      )}
                    </div>
                  </div>
                )}

                <div className="mt-4 max-h-[62vh] space-y-3 overflow-y-auto pr-1">
                  <label className="block text-xs font-medium text-foreground">
                    Question context
                    <Input
                      value={trainQuestion}
                      onChange={(event) => setTrainQuestion(event.target.value)}
                      placeholder="Which question should this datapoint answer?"
                      className="mt-1"
                      disabled={trainSubmitting || trainLoadingExisting}
                    />
                  </label>
                  <label className="block text-xs font-medium text-foreground">
                    SQL template
                    <textarea
                      value={trainSql}
                      onChange={(event) => setTrainSql(event.target.value)}
                      className="mt-1 min-h-[140px] w-full resize-y rounded-md border border-input bg-background px-3 py-2 font-mono text-xs leading-relaxed outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                      placeholder="SELECT ..."
                      disabled={trainSubmitting || trainLoadingExisting}
                    />
                  </label>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="block text-xs font-medium text-foreground">
                      Datapoint name
                      <Input
                        value={trainName}
                        onChange={(event) => setTrainName(event.target.value)}
                        placeholder="User-trained query datapoint"
                        className="mt-1"
                        disabled={trainSubmitting || trainLoadingExisting}
                      />
                    </label>
                    <label className="block text-xs font-medium text-foreground">
                      Related tables
                      <Input
                        value={trainRelatedTables}
                        onChange={(event) => setTrainRelatedTables(event.target.value)}
                        placeholder="public.table_a, public.table_b"
                        className="mt-1"
                        disabled={trainSubmitting || trainLoadingExisting}
                      />
                    </label>
                  </div>
                  <label className="block text-xs font-medium text-foreground">
                    Notes (optional)
                    <textarea
                      value={trainNotes}
                      onChange={(event) => setTrainNotes(event.target.value)}
                      className="mt-1 min-h-[90px] w-full resize-y rounded-md border border-input bg-background px-3 py-2 text-xs outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                      placeholder="What was wrong and how should DataChat answer this next time?"
                      disabled={trainSubmitting || trainLoadingExisting}
                    />
                  </label>
                </div>

                {(trainLoadingExisting || trainSyncing) && (
                  <div className="mt-3 inline-flex items-center gap-2 rounded-md border border-border/80 bg-muted/40 px-2.5 py-1 text-xs text-muted-foreground">
                    <Loader2 size={12} className="animate-spin" />
                    {trainLoadingExisting ? "Loading existing training..." : "Syncing training datapoint..."}
                  </div>
                )}

                {trainError && (
                  <div className="mt-3 rounded border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                    {trainError}
                  </div>
                )}

                <div className="mt-5 flex justify-end gap-2">
                  <Button
                    variant="outline"
                    onClick={() => {
                      if (trainSubmitting) {
                        return;
                      }
                      setTrainModalOpen(false);
                      setTrainError(null);
                    }}
                    disabled={trainSubmitting}
                  >
                    Cancel
                  </Button>
                  <Button onClick={handleSubmitTrainDatapoint} disabled={trainSubmitting}>
                    {trainSubmitting ? <Loader2 size={14} className="mr-2 animate-spin" /> : null}
                    {trainMode === "update" ? "Update, Sync, and Retry" : "Save, Sync, and Retry"}
                  </Button>
                </div>
              </div>
            </div>
          </div>,
          document.body
        )}
      {shortcutsOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
          role="dialog"
          aria-modal="true"
          aria-label="Keyboard shortcuts"
        >
          <div className="w-full max-w-md rounded-lg bg-background p-6 shadow-lg">
            <h3 className="text-base font-semibold">Keyboard shortcuts</h3>
            <div className="mt-4 space-y-2 text-sm">
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Focus query input</span>
                <code className="text-xs">Ctrl/Cmd + K</code>
              </div>
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Toggle conversation history</span>
                <code className="text-xs">Ctrl/Cmd + H</code>
              </div>
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Toggle schema explorer</span>
                <code className="text-xs">Ctrl/Cmd + E</code>
              </div>
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Open shortcuts modal</span>
                <code className="text-xs">Ctrl/Cmd + /</code>
              </div>
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Focus query input</span>
                <code className="text-xs">/</code>
              </div>
              <div className="flex items-center justify-between rounded border border-border px-3 py-2">
                <span>Close open modal</span>
                <code className="text-xs">Esc</code>
              </div>
            </div>
            <div className="mt-4 flex justify-end">
              <button
                ref={shortcutsCloseButtonRef}
                className="rounded-md border border-border px-3 py-2 text-xs"
                onClick={() => {
                  setShortcutsOpen(false);
                  restoreInputFocus();
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
      {toolApprovalOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
          role="dialog"
          aria-modal="true"
          aria-label="Tool approval modal"
        >
          <div className="w-full max-w-lg rounded-lg bg-background p-6 shadow-lg">
            <h3 className="text-base font-semibold">Tool Approval Required</h3>
            <p className="mt-2 text-sm text-muted-foreground">
              {toolApprovalMessage}
            </p>
            <div className="mt-4 space-y-2 text-xs text-muted-foreground">
              {toolApprovalCalls.map((call) => (
                <div key={call.name} className="rounded-md border border-border p-2">
                  <div className="font-medium text-foreground">{call.name}</div>
                  <pre className="mt-1 whitespace-pre-wrap">
                    {JSON.stringify(call.arguments || {}, null, 2)}
                  </pre>
                </div>
              ))}
            </div>
            <div className="mt-4 rounded-md border border-muted bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
              Cost hint:
              {toolCostEstimate() ? (
                <div className="mt-2 space-y-1">
                  {toolCostEstimate()?.map((item, index) => (
                    <div key={`${item.tool}-${index}`}>
                      {item.tool}: tables={item.tables ?? "unknown"} · batch size=
                      {"batchSize" in item ? item.batchSize : "n/a"} · batches=
                      {"batches" in item ? item.batches : "unknown"} · LLM calls≈
                      {"llmCalls" in item ? item.llmCalls : "unknown"}
                    </div>
                  ))}
                </div>
              ) : (
                <span> this may run LLM calls or database profiling.</span>
              )}
            </div>
            {toolApprovalError && (
              <div className="mt-2 text-xs text-destructive">{toolApprovalError}</div>
            )}
            <div className="mt-4 flex gap-2">
              <button
                ref={toolApprovalApproveButtonRef}
                className="rounded-md bg-primary px-3 py-2 text-xs text-primary-foreground"
                onClick={handleApproveTools}
                disabled={toolApprovalRunning}
                aria-label="Approve tool calls"
              >
                {toolApprovalRunning ? "Approving..." : "Approve"}
              </button>
              <button
                className="rounded-md border border-border px-3 py-2 text-xs"
                onClick={() => setToolApprovalOpen(false)}
                disabled={toolApprovalRunning}
                aria-label="Cancel tool approval"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
