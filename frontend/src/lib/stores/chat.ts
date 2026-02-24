/**
 * Chat Store - Zustand
 *
 * Manages chat state including messages, agent updates, and WebSocket connection.
 */

import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import type { ChatMessage, ChatResponse, AgentUpdate } from "../api";

export interface Message extends ChatMessage {
  id: string;
  timestamp: Date;
  sub_answers?: Array<{
    index: number;
    query: string;
    answer: string;
    answer_source?: string | null;
    answer_confidence?: number | null;
    sql?: string | null;
    data?: Record<string, unknown[]> | null;
    visualization_hint?: string | null;
    visualization_metadata?: Record<string, unknown> | null;
    clarifying_questions?: string[];
    error?: string | null;
  }>;
  sql?: string | null;
  data?: Record<string, unknown[]> | null;
  visualization_hint?: string | null;
  visualization_metadata?: Record<string, unknown> | null;
  clarifying_questions?: string[];
  sources?: Array<{
    datapoint_id: string;
    type: string;
    name: string;
    relevance_score: number;
  }>;
  answer_source?: string | null;
  answer_confidence?: number | null;
  evidence?: Array<{
    datapoint_id: string;
    name?: string | null;
    type?: string | null;
    reason?: string | null;
  }>;
  tool_approval_required?: boolean;
  tool_approval_message?: string | null;
  tool_approval_calls?: Array<{
    name: string;
    arguments?: Record<string, unknown>;
  }>;
  metrics?: {
    total_latency_ms: number;
    agent_timings: Record<string, number>;
    llm_calls: number;
    retry_count: number;
    sql_formatter_fallback_calls?: number;
    sql_formatter_fallback_successes?: number;
    query_compiler_llm_calls?: number;
    query_compiler_llm_refinements?: number;
    query_compiler_latency_ms?: number;
  };
  workflow_artifacts?: {
    package_version: string;
    domain: string;
    summary: string;
    metrics: Array<{ label: string; value: string }>;
    drivers: Array<{ dimension: string; value: string; contribution: string }>;
    caveats: string[];
    sources: Array<{ datapoint_id: string; name: string; source_type: string }>;
    follow_ups: string[];
  } | null;
  decision_trace?: Array<{
    stage: string;
    decision: string;
    reason: string;
    details?: Record<string, unknown>;
  }>;
  action_trace?: Array<Record<string, unknown>>;
  loop_terminal_state?: string | null;
  loop_stop_reason?: string | null;
  loop_shadow_decisions?: Array<Record<string, unknown>>;
}

type PersistedMessage = Pick<
  Message,
  | "id"
  | "role"
  | "content"
  | "clarifying_questions"
  | "answer_source"
  | "answer_confidence"
  | "tool_approval_required"
  | "tool_approval_message"
  | "sql"
  | "visualization_hint"
  | "visualization_metadata"
  | "sources"
  | "evidence"
  | "metrics"
  | "sub_answers"
  | "workflow_artifacts"
  | "decision_trace"
  | "action_trace"
  | "loop_terminal_state"
  | "loop_stop_reason"
  | "loop_shadow_decisions"
> & {
  timestamp: string | Date;
  data?: Record<string, unknown[]> | null;
};

const MAX_PERSISTED_MESSAGES = 60;
const MAX_PERSISTED_CONTENT_CHARS = 4000;
const MAX_PERSISTED_DATA_ROWS = 50;

const createSessionId = () => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `session_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
};

const createMessageId = () => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `msg_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
};

const noopStorage = {
  getItem: () => null,
  setItem: () => undefined,
  removeItem: () => undefined,
};

const reviveMessage = (message: PersistedMessage): Message => ({
  ...message,
  timestamp: message.timestamp instanceof Date ? message.timestamp : new Date(message.timestamp),
});

const compactMessageForPersistence = (message: Message): PersistedMessage => {
  const compactedData: Record<string, unknown[]> | null = message.data
    ? Object.fromEntries(
        Object.entries(message.data).map(([key, values]) => [
          key,
          Array.isArray(values) ? values.slice(0, MAX_PERSISTED_DATA_ROWS) : values,
        ])
      )
    : null;

  return {
    id: message.id,
    role: message.role,
    content:
      message.content.length > MAX_PERSISTED_CONTENT_CHARS
        ? `${message.content.slice(0, MAX_PERSISTED_CONTENT_CHARS)}...`
        : message.content,
    clarifying_questions: message.clarifying_questions,
    answer_source: message.answer_source,
    answer_confidence: message.answer_confidence,
    tool_approval_required: message.tool_approval_required,
    tool_approval_message: message.tool_approval_message,
    sql: message.sql,
    visualization_hint: message.visualization_hint,
    visualization_metadata: message.visualization_metadata,
    sources: message.sources?.slice(0, 10),
    evidence: message.evidence?.slice(0, 10),
    metrics: message.metrics
      ? {
          total_latency_ms: message.metrics.total_latency_ms,
          agent_timings: message.metrics.agent_timings,
          llm_calls: message.metrics.llm_calls,
          retry_count: message.metrics.retry_count,
        }
      : undefined,
    sub_answers: message.sub_answers?.slice(0, 5).map((sub) => ({
      index: sub.index,
      query: sub.query,
      answer: sub.answer?.slice(0, MAX_PERSISTED_CONTENT_CHARS),
      answer_source: sub.answer_source,
      answer_confidence: sub.answer_confidence,
      sql: sub.sql,
      visualization_hint: sub.visualization_hint,
    })),
    workflow_artifacts: message.workflow_artifacts
      ? {
          package_version: message.workflow_artifacts.package_version,
          domain: message.workflow_artifacts.domain,
          summary: message.workflow_artifacts.summary.slice(0, 500),
          metrics: message.workflow_artifacts.metrics.slice(0, 6),
          drivers: message.workflow_artifacts.drivers.slice(0, 5),
          caveats: message.workflow_artifacts.caveats.slice(0, 6),
          sources: message.workflow_artifacts.sources.slice(0, 6),
          follow_ups: message.workflow_artifacts.follow_ups.slice(0, 6),
        }
      : null,
    decision_trace: message.decision_trace?.slice(0, 30),
    action_trace: message.action_trace?.slice(0, 20),
    loop_terminal_state: message.loop_terminal_state ?? null,
    loop_stop_reason: message.loop_stop_reason ?? null,
    loop_shadow_decisions: message.loop_shadow_decisions?.slice(0, 30),
    data: compactedData,
    timestamp: message.timestamp,
  };
};

interface ChatState {
  // Messages
  messages: Message[];
  conversationId: string | null;
  sessionSummary: string | null;
  sessionState: Record<string, unknown> | null;
  frontendSessionId: string;

  // Agent status
  currentAgent: string | null;
  agentStatus: "idle" | "running" | "completed" | "error";
  agentMessage: string | null;
  agentError: string | null;
  agentHistory: AgentUpdate[];

  // Loading state
  isLoading: boolean;

  // WebSocket connection
  isConnected: boolean;

  // Actions
  addMessage: (message: Omit<Message, "id" | "timestamp">) => void;
  updateLastMessage: (
    updates: Partial<Omit<Message, "id" | "timestamp">>
  ) => void;
  setConversationId: (id: string | null) => void;
  setSessionMemory: (
    summary: string | null,
    state: Record<string, unknown> | null
  ) => void;
  setAgentUpdate: (update: AgentUpdate) => void;
  resetAgentStatus: () => void;
  setLoading: (loading: boolean) => void;
  setConnected: (connected: boolean) => void;
  clearMessages: () => void;
  loadSession: (session: {
    frontendSessionId: string;
    messages: Message[];
    conversationId: string | null;
    sessionSummary: string | null;
    sessionState: Record<string, unknown> | null;
  }) => void;
  addChatResponse: (query: string, response: ChatResponse) => void;
  appendToLastMessage: (content: string) => void;
}

export const useChatStore = create<ChatState>()(
  persist(
    (set, get) => ({
  // Initial state
  messages: [],
  conversationId: null,
  sessionSummary: null,
  sessionState: null,
  frontendSessionId: createSessionId(),
  currentAgent: null,
  agentStatus: "idle",
  agentMessage: null,
  agentError: null,
  agentHistory: [],
  isLoading: false,
  isConnected: false,

  // Actions
  addMessage: (message) =>
    set((state) => ({
      messages: [
        ...state.messages,
        {
          ...message,
          id: createMessageId(),
          timestamp: new Date(),
        },
      ],
    })),

  updateLastMessage: (updates) =>
    set((state) => {
      const messages = [...state.messages];
      const lastMessage = messages[messages.length - 1];
      if (lastMessage) {
        messages[messages.length - 1] = {
          ...lastMessage,
          ...updates,
        };
      }
      return { messages };
    }),

  setConversationId: (id) => set({ conversationId: id }),
  setSessionMemory: (summary, state) =>
    set({
      sessionSummary: summary,
      sessionState: state,
    }),

  setAgentUpdate: (update) =>
    set((state) => ({
      currentAgent: update.current_agent,
      agentStatus: update.status,
      agentMessage: update.message || null,
      agentError: update.error || null,
      agentHistory: [...state.agentHistory, update],
    })),

  resetAgentStatus: () =>
    set({
      currentAgent: null,
      agentStatus: "idle",
      agentMessage: null,
      agentError: null,
      agentHistory: [],
    }),

  setLoading: (loading) => set({ isLoading: loading }),

  setConnected: (connected) => set({ isConnected: connected }),

  clearMessages: () =>
    set({
        messages: [],
        conversationId: null,
        sessionSummary: null,
        sessionState: null,
        frontendSessionId: createSessionId(),
      currentAgent: null,
      agentStatus: "idle",
      agentMessage: null,
      agentError: null,
      agentHistory: [],
    }),

  loadSession: (session) =>
    set({
      messages: session.messages,
      conversationId: session.conversationId,
      sessionSummary: session.sessionSummary,
      sessionState: session.sessionState,
      frontendSessionId: session.frontendSessionId,
      currentAgent: null,
      agentStatus: "idle",
      agentMessage: null,
      agentError: null,
      agentHistory: [],
      isLoading: false,
      isConnected: false,
    }),

  addChatResponse: (query, response) =>
    set((state) => {
      const userMessage: Message = {
        id: createMessageId(),
        role: "user",
        content: query,
        timestamp: new Date(),
      };

      const assistantMessage: Message = {
        id: createMessageId(),
        role: "assistant",
        content: response.answer,
        timestamp: new Date(),
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
      };

      return {
        messages: [...state.messages, userMessage, assistantMessage],
        conversationId: response.conversation_id,
        sessionSummary: response.session_summary || null,
        sessionState: response.session_state || null,
      };
    }),

  appendToLastMessage: (content) =>
    set(() => {
      const messages = [...get().messages];
      const lastMessage = messages[messages.length - 1];
      if (!lastMessage) {
        return { messages };
      }
      messages[messages.length - 1] = {
        ...lastMessage,
        content: `${lastMessage.content}${content}`,
      };
      return { messages };
    }),
    }),
    {
      name: "datachat.chat.session.v1",
      storage: createJSONStorage(() =>
        typeof window === "undefined" ? noopStorage : window.localStorage
      ),
      partialize: (state) => ({
        messages: (
          state.isLoading &&
          state.messages.length > 0 &&
          state.messages[state.messages.length - 1]?.role === "assistant"
            ? state.messages.slice(0, -1)
            : state.messages
        )
          .slice(-MAX_PERSISTED_MESSAGES)
          .map((message) => compactMessageForPersistence(message)),
        conversationId: state.conversationId,
        sessionSummary: state.sessionSummary,
        sessionState: state.sessionState,
        frontendSessionId: state.frontendSessionId,
      }),
      onRehydrateStorage: () => (state) => {
        if (!state) return;
        state.messages = state.messages.map((message) => reviveMessage(message as PersistedMessage));
        if (!state.frontendSessionId) {
          state.frontendSessionId = createSessionId();
        }
        state.sessionSummary = state.sessionSummary || null;
        state.sessionState = state.sessionState || null;
      },
    }
  )
);
