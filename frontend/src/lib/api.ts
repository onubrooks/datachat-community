/**
 * API Client for DataChat Backend
 *
 * Provides REST API client and WebSocket connection for real-time updates.
 */

// Type definitions matching backend API models
export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

export interface ChatRequest {
  message: string;
  execution_mode?: "natural_language" | "direct_sql";
  sql?: string;
  workflow_mode?: "auto" | "finance_variance_v1";
  conversation_id?: string;
  target_database?: string;
  conversation_history?: ChatMessage[];
  session_summary?: string | null;
  session_state?: Record<string, unknown> | null;
  synthesize_simple_sql?: boolean;
}

export interface DataSource {
  datapoint_id: string;
  type: string;
  name: string;
  relevance_score: number;
}

export interface ChatMetrics {
  total_latency_ms: number;
  agent_timings: Record<string, number>;
  llm_calls: number;
  retry_count: number;
  sql_formatter_fallback_calls?: number;
  sql_formatter_fallback_successes?: number;
  query_compiler_llm_calls?: number;
  query_compiler_llm_refinements?: number;
  query_compiler_latency_ms?: number;
}

export interface SQLValidationError {
  error_type: "syntax" | "security" | "schema" | "other";
  message: string;
  location?: string | null;
  severity: "critical" | "high" | "medium" | "low";
}

export interface ValidationWarning {
  warning_type: "performance" | "style" | "compatibility" | "other";
  message: string;
  suggestion?: string | null;
}

export interface ChatResponse {
  answer: string;
  clarifying_questions?: string[];
  sub_answers?: {
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
  }[];
  sql: string | null;
  data: Record<string, unknown[]> | null;
  visualization_hint: string | null;
  visualization_metadata?: Record<string, unknown> | null;
  sources: DataSource[];
  answer_source?: string | null;
  answer_confidence?: number | null;
  evidence?: {
    datapoint_id: string;
    name?: string | null;
    type?: string | null;
    reason?: string | null;
  }[];
  validation_errors?: SQLValidationError[];
  validation_warnings?: ValidationWarning[];
  tool_approval_required?: boolean;
  tool_approval_message?: string | null;
  tool_approval_calls?: {
    name: string;
    arguments?: Record<string, unknown>;
  }[];
  metrics: ChatMetrics;
  conversation_id: string;
  session_summary?: string | null;
  session_state?: Record<string, unknown> | null;
  decision_trace?: Array<{
    stage: string;
    decision: string;
    reason: string;
    details?: Record<string, unknown>;
  }>;
  action_trace?: Array<{
    version?: string;
    step?: number;
    stage?: string;
    selected_action?: string;
    inputs?: Record<string, unknown>;
    outputs?: Record<string, unknown>;
    verification?: {
      status?: string;
      reason?: string | null;
      details?: Record<string, unknown>;
    };
    error_class?: string | null;
    stop_reason?: string | null;
    terminal_state?: string | null;
  }>;
  loop_terminal_state?: string | null;
  loop_stop_reason?: string | null;
  loop_shadow_decisions?: Array<Record<string, unknown>>;
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
}

export interface AgentUpdate {
  current_agent: string;
  status: "running" | "completed" | "error";
  message?: string;
  error?: string;
}

export interface SetupStep {
  step: string;
  title: string;
  description: string;
  action: string;
}

export interface SystemStatusResponse {
  is_initialized: boolean;
  has_databases: boolean;
  has_system_database: boolean;
  has_datapoints: boolean;
  setup_required: SetupStep[];
}

export interface SystemInitializeRequest {
  database_url?: string;
  system_database_url?: string;
  auto_profile: boolean;
}

export interface SystemInitializeResponse {
  message: string;
  is_initialized: boolean;
  has_databases: boolean;
  has_system_database: boolean;
  has_datapoints: boolean;
  setup_required: SetupStep[];
}

export interface ToolInfo {
  name: string;
  description: string;
  category: string;
  requires_approval: boolean;
  enabled: boolean;
  parameters_schema: Record<string, unknown>;
}

export interface ToolExecuteRequest {
  name: string;
  arguments?: Record<string, unknown>;
  approved?: boolean;
  user_id?: string;
  correlation_id?: string;
}

export interface ToolExecuteResponse {
  tool: string;
  success: boolean;
  result?: Record<string, unknown> | null;
  error?: string | null;
}

export interface FeedbackSubmitRequest {
  category: "answer_feedback" | "issue_report" | "improvement_suggestion";
  sentiment?: "up" | "down" | null;
  message?: string | null;
  conversation_id?: string | null;
  message_id?: string | null;
  target_database_id?: string | null;
  answer_source?: string | null;
  answer_confidence?: number | null;
  query?: string | null;
  answer?: string | null;
  sql?: string | null;
  sources?: Array<Record<string, unknown>>;
  metadata?: Record<string, unknown> | null;
}

export interface FeedbackSubmitResponse {
  ok: boolean;
  feedback_id: string;
  saved_to: "system_database" | "logs_only";
  created_at: string;
}

export interface FeedbackSummaryResponse {
  window_days: number;
  totals: Array<{
    category: string;
    sentiment: string | null;
    count: number;
  }>;
}

function extractApiErrorMessage(error: unknown, fallback: string): string {
  if (error && typeof error === "object") {
    const payload = error as Record<string, unknown>;
    const direct =
      (typeof payload.detail === "string" && payload.detail) ||
      (typeof payload.message === "string" && payload.message);
    if (direct) return direct;

    if (payload.detail && typeof payload.detail === "object") {
      const detail = payload.detail as Record<string, unknown>;
      const detailMessage =
        (typeof detail.message === "string" && detail.message) ||
        (typeof detail.detail === "string" && detail.detail);
      const contractErrors = Array.isArray(detail.contract_errors)
        ? detail.contract_errors
            .map((item) => {
              if (!item || typeof item !== "object") return null;
              const issue = item as Record<string, unknown>;
              const code = typeof issue.code === "string" ? issue.code : "contract_error";
              const message =
                typeof issue.message === "string"
                  ? issue.message
                  : "DataPoint contract validation failed.";
              return `${code}: ${message}`;
            })
            .filter(Boolean)
            .join("; ")
        : "";
      if (detailMessage && contractErrors) {
        return `${detailMessage} ${contractErrors}`;
      }
      if (detailMessage) return detailMessage;
      if (contractErrors) return contractErrors;
    }
  }
  return fallback;
}

export interface StreamChatHandlers {
  onOpen?: () => void;
  onClose?: () => void;
  onAgentUpdate?: (update: AgentUpdate) => void;
  onThinking?: (note: string) => void;
  onAnswerChunk?: (chunk: string) => void;
  onComplete?: (response: ChatResponse) => void;
  onError?: (message: string) => void;
  onSystemNotInitialized?: (steps: SetupStep[], message: string) => void;
}

export interface HealthResponse {
  status: string;
  version: string;
  timestamp: string;
}

export interface DatabaseConnection {
  connection_id: string;
  name: string;
  database_url: string;
  database_type: string;
  is_active: boolean;
  is_default: boolean;
  tags: string[];
  description?: string | null;
  created_at: string;
  last_profiled?: string | null;
  datapoint_count: number;
}

export interface ConversationSnapshotPayload {
  frontend_session_id: string;
  title: string;
  target_database_id: string | null;
  conversation_id: string | null;
  session_summary: string | null;
  session_state: Record<string, unknown>;
  messages: Array<Record<string, unknown>>;
  created_at: string | null;
  updated_at: string | null;
}

export interface ConversationUpsertRequest {
  title: string;
  target_database_id: string | null;
  conversation_id: string | null;
  session_summary: string | null;
  session_state: Record<string, unknown>;
  messages: Array<Record<string, unknown>>;
  updated_at?: string;
}

export interface DatabaseConnectionCreate {
  name: string;
  database_url: string;
  database_type: string;
  tags: string[];
  description?: string;
  is_default?: boolean;
}

export interface DatabaseConnectionUpdate {
  name?: string;
  database_url?: string;
  database_type?: string;
  description?: string | null;
}

export interface DatabaseSchemaColumn {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  is_foreign_key: boolean;
  foreign_table?: string | null;
  foreign_column?: string | null;
}

export interface DatabaseSchemaTable {
  schema_name: string;
  table_name: string;
  row_count?: number | null;
  table_type: string;
  columns: DatabaseSchemaColumn[];
}

export interface DatabaseSchemaResponse {
  connection_id: string;
  database_type: "postgresql" | "clickhouse" | "mysql";
  fetched_at: string;
  tables: DatabaseSchemaTable[];
}

export interface ProfilingProgress {
  total_tables: number;
  tables_completed: number;
}

export interface ProfilingJob {
  job_id: string;
  connection_id: string;
  status: string;
  progress?: ProfilingProgress | null;
  error?: string | null;
  profile_id?: string | null;
}

export interface GenerationProgress {
  total_tables: number;
  tables_completed: number;
  batch_size: number;
}

export interface GenerationJob {
  job_id: string;
  profile_id: string;
  status: string;
  progress?: GenerationProgress | null;
  error?: string | null;
}

export interface PendingDataPoint {
  pending_id: string;
  profile_id: string;
  datapoint: Record<string, unknown>;
  confidence: number;
  status: string;
  review_note?: string | null;
}

export interface DataPointSummary {
  datapoint_id: string;
  type: string;
  name?: string | null;
  connection_id?: string | null;
  scope?: string | null;
  source_tier?: string | null;
  source_path?: string | null;
  lifecycle_version?: string | null;
  lifecycle_reviewer?: string | null;
  lifecycle_changed_by?: string | null;
  lifecycle_changed_reason?: string | null;
  lifecycle_changed_at?: string | null;
}

export interface RuntimeSettingsResponse {
  target_database_url: string | null;
  system_database_url: string | null;
  llm_default_provider: string;
  llm_openai_model: string | null;
  llm_openai_model_mini: string | null;
  llm_anthropic_model: string | null;
  llm_anthropic_model_mini: string | null;
  llm_google_model: string | null;
  llm_google_model_mini: string | null;
  llm_local_model: string | null;
  llm_temperature: string | null;
  database_credentials_key_present: boolean;
  llm_openai_api_key_present: boolean;
  llm_anthropic_api_key_present: boolean;
  llm_google_api_key_present: boolean;
  database_credentials_key_preview: string | null;
  llm_openai_api_key_preview: string | null;
  llm_anthropic_api_key_preview: string | null;
  llm_google_api_key_preview: string | null;
  source: Record<string, string>;
  runtime_valid: boolean;
  runtime_error: string | null;
}

export interface RuntimeSettingsUpdateRequest {
  target_database_url?: string | null;
  system_database_url?: string | null;
  llm_default_provider?: string | null;
  llm_openai_model?: string | null;
  llm_openai_model_mini?: string | null;
  llm_anthropic_model?: string | null;
  llm_anthropic_model_mini?: string | null;
  llm_google_model?: string | null;
  llm_google_model_mini?: string | null;
  llm_local_model?: string | null;
  llm_temperature?: string | null;
  database_credentials_key?: string | null;
  llm_openai_api_key?: string | null;
  llm_anthropic_api_key?: string | null;
  llm_google_api_key?: string | null;
  generate_database_credentials_key?: boolean;
}

export interface SyncStatusResponse {
  status: string;
  job_id: string | null;
  sync_type: string | null;
  started_at: string | null;
  finished_at: string | null;
  total_datapoints: number;
  processed_datapoints: number;
  error: string | null;
}

/**
 * API Client Configuration
 */
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
const WS_BASE_URL =
  process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000";

/**
 * REST API Client
 */
export class DataChatAPI {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  /**
   * Send a chat message and get response
   */
  async chat(request: ChatRequest): Promise<ChatResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/chat`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({
        error: "Unknown error",
        message: response.statusText,
      }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }

    return response.json();
  }

  /**
   * Check API health
   */
  async health(): Promise<HealthResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/health`);

    if (!response.ok) {
      throw new Error(`Health check failed: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Check API readiness
   */
  async ready(): Promise<HealthResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/ready`);

    if (!response.ok) {
      throw new Error(`Readiness check failed: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Check system initialization status
   */
  async systemStatus(): Promise<SystemStatusResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/status`);

    if (!response.ok) {
      throw new Error(`System status failed: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * Initialize system with database connection
   */
  async systemInitialize(
    payload: SystemInitializeRequest
  ): Promise<SystemInitializeResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/initialize`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }

    return response.json();
  }

  async systemReset(): Promise<SystemStatusResponse & { message: string }> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/reset`, {
      method: "POST",
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }

    return response.json();
  }

  async getSystemSettings(): Promise<RuntimeSettingsResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/settings`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async updateSystemSettings(
    payload: RuntimeSettingsUpdateRequest
  ): Promise<RuntimeSettingsResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/settings`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async listDatabases(): Promise<DatabaseConnection[]> {
    const response = await fetch(`${this.baseUrl}/api/v1/databases`);
    if (!response.ok) {
      throw new Error(`List databases failed: ${response.statusText}`);
    }
    return response.json();
  }

  async listConversations(limit = 20): Promise<ConversationSnapshotPayload[]> {
    const response = await fetch(`${this.baseUrl}/api/v1/conversations?limit=${limit}`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async upsertConversation(
    frontendSessionId: string,
    payload: ConversationUpsertRequest
  ): Promise<ConversationSnapshotPayload> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/conversations/${encodeURIComponent(frontendSessionId)}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async deleteConversation(frontendSessionId: string): Promise<void> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/conversations/${encodeURIComponent(frontendSessionId)}`,
      { method: "DELETE" }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
  }

  async createDatabase(
    payload: DatabaseConnectionCreate
  ): Promise<DatabaseConnection> {
    const response = await fetch(`${this.baseUrl}/api/v1/databases`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async updateDatabase(
    connectionId: string,
    payload: DatabaseConnectionUpdate
  ): Promise<DatabaseConnection> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/databases/${connectionId}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async setDefaultDatabase(connectionId: string): Promise<void> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/databases/${connectionId}/default`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ is_default: true }),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
  }

  async deleteDatabase(connectionId: string): Promise<void> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/databases/${connectionId}`,
      { method: "DELETE" }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
  }

  async getDatabaseSchema(connectionId: string): Promise<DatabaseSchemaResponse> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/databases/${connectionId}/schema`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      const message =
        error.message || error.detail || response.statusText || `HTTP ${response.status}`;
      throw new Error(message);
    }
    return response.json();
  }

  async startProfiling(
    connectionId: string,
    payload: { sample_size: number; tables?: string[] }
  ): Promise<ProfilingJob> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/databases/${connectionId}/profile`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getProfilingJob(jobId: string): Promise<ProfilingJob> {
    const response = await fetch(`${this.baseUrl}/api/v1/profiling/jobs/${jobId}`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getLatestProfilingJob(connectionId: string): Promise<ProfilingJob | null> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/profiling/jobs/connection/${connectionId}/latest`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async generateDatapoints(profileId: string): Promise<GenerationJob> {
    return this.startDatapointGeneration({ profile_id: profileId });
  }

  async startDatapointGeneration(payload: {
    profile_id: string;
    tables?: string[];
    depth?: string;
    batch_size?: number;
    max_tables?: number | null;
    max_metrics_per_table?: number;
    replace_existing?: boolean;
  }): Promise<GenerationJob> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints/generate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getGenerationJob(jobId: string): Promise<GenerationJob> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/generate/jobs/${jobId}`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getLatestGenerationJob(profileId: string): Promise<GenerationJob | null> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/generate/profiles/${profileId}`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async listProfileTables(profileId: string): Promise<string[]> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/profiling/profiles/${profileId}/tables`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    const data = await response.json();
    return data.tables || [];
  }

  async listPendingDatapoints(options?: {
    statusFilter?: "pending" | "approved" | "rejected" | "all";
    connectionId?: string | null;
  }): Promise<PendingDataPoint[]> {
    const params = new URLSearchParams();
    if (options?.statusFilter) {
      params.set("status_filter", options.statusFilter);
    }
    if (options?.connectionId) {
      params.set("connection_id", options.connectionId);
    }
    const query = params.toString();
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/pending${query ? `?${query}` : ""}`
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    const data = await response.json();
    return data.pending || [];
  }

  async listDatapoints(): Promise<DataPointSummary[]> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    const data = await response.json();
    return data.datapoints || [];
  }

  async getDatapoint(datapointId: string): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints/${datapointId}`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async createDatapoint(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async updateDatapoint(
    datapointId: string,
    payload: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints/${datapointId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async deleteDatapoint(datapointId: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/datapoints/${datapointId}`, {
      method: "DELETE",
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
  }

  async approvePendingDatapoint(
    pendingId: string,
    datapoint?: Record<string, unknown>
  ): Promise<PendingDataPoint> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/pending/${pendingId}/approve`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ datapoint }),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(
        extractApiErrorMessage(error, response.statusText || `HTTP ${response.status}`)
      );
    }
    return response.json();
  }

  async rejectPendingDatapoint(pendingId: string, reviewNote?: string): Promise<PendingDataPoint> {
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/pending/${pendingId}/reject`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ review_note: reviewNote || null }),
      }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(
        extractApiErrorMessage(error, response.statusText || `HTTP ${response.status}`)
      );
    }
    return response.json();
  }

  async bulkApproveDatapoints(connectionId?: string | null): Promise<PendingDataPoint[]> {
    const params = new URLSearchParams();
    if (connectionId) {
      params.set("connection_id", connectionId);
    }
    const query = params.toString();
    const response = await fetch(
      `${this.baseUrl}/api/v1/datapoints/pending/bulk-approve${query ? `?${query}` : ""}`,
      { method: "POST" }
    );
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(
        extractApiErrorMessage(error, response.statusText || `HTTP ${response.status}`)
      );
    }
    const data = await response.json();
    return data.pending || [];
  }

  async triggerSync(payload?: {
    scope?: "auto" | "global" | "database";
    connection_id?: string | null;
    conflict_mode?: "error" | "prefer_user" | "prefer_managed" | "prefer_latest";
  }): Promise<{ job_id: string }> {
    const body = payload
      ? {
          ...(payload.scope ? { scope: payload.scope } : {}),
          ...(payload.connection_id ? { connection_id: payload.connection_id } : {}),
          ...(payload.conflict_mode ? { conflict_mode: payload.conflict_mode } : {}),
        }
      : undefined;
    const response = await fetch(`${this.baseUrl}/api/v1/sync`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getSyncStatus(): Promise<SyncStatusResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/sync/status`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async listTools(): Promise<ToolInfo[]> {
    const response = await fetch(`${this.baseUrl}/api/v1/tools`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async executeTool(payload: ToolExecuteRequest): Promise<ToolExecuteResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/tools/execute`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async submitFeedback(payload: FeedbackSubmitRequest): Promise<FeedbackSubmitResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/feedback`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async getFeedbackSummary(days = 30): Promise<FeedbackSummaryResponse> {
    const response = await fetch(`${this.baseUrl}/api/v1/feedback/summary?days=${days}`);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.detail || error.message || `HTTP ${response.status}`);
    }
    return response.json();
  }

  async emitEntryEvent(payload: {
    flow: string;
    step: string;
    status: string;
    source?: string;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/v1/system/entry-event`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(`Event emission failed: ${response.statusText}`);
    }
  }
}

/**
 * WebSocket Client for Real-Time Agent Updates
 */
export class DataChatWebSocket {
  private ws: WebSocket | null = null;
  private baseUrl: string;

  constructor(baseUrl: string = WS_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  /**
   * Start a streaming chat session over WebSocket.
   */
  streamChat(request: ChatRequest, handlers: StreamChatHandlers): void {
    try {
      this.ws = new WebSocket(`${this.baseUrl}/ws/chat`);

      this.ws.onopen = () => {
        this.ws?.send(JSON.stringify(request));
        handlers.onOpen?.();
      };

      this.ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as {
            event?: string;
            agent?: string;
            message?: string;
            error?: string;
            chunk?: string;
            note?: string;
          };

          if (payload.event === "agent_start" && payload.agent) {
            handlers.onAgentUpdate?.({
              current_agent: payload.agent,
              status: "running",
              message: payload.message,
            });
            return;
          }

          if (payload.event === "agent_complete" && payload.agent) {
            handlers.onAgentUpdate?.({
              current_agent: payload.agent,
              status: "completed",
              message: payload.message,
            });
            return;
          }

          if (payload.event === "answer_chunk" && payload.chunk) {
            handlers.onAnswerChunk?.(payload.chunk);
            return;
          }

          if (payload.event === "thinking" && payload.note) {
            handlers.onThinking?.(payload.note);
            return;
          }

          if (payload.event === "complete") {
            handlers.onComplete?.(payload as ChatResponse);
            this.disconnect();
            return;
          }

          if (payload.event === "error") {
            if (payload.error === "system_not_initialized") {
              handlers.onSystemNotInitialized?.(
                (payload as { setup_steps?: SetupStep[] }).setup_steps || [],
                payload.message || "DataChat requires setup."
              );
            } else {
              handlers.onError?.(payload.message || "WebSocket error");
            }
            this.disconnect();
          }
        } catch (error) {
          console.error("Failed to parse WebSocket message:", error);
        }
      };

      this.ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        handlers.onError?.("WebSocket connection error");
      };

      this.ws.onclose = () => {
        handlers.onClose?.();
      };
    } catch (error) {
      console.error("Failed to create WebSocket:", error);
      handlers.onError?.("Failed to create WebSocket");
    }
  }

  /**
   * Send a message through WebSocket
   */
  send(data: unknown): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    } else {
      console.error("WebSocket is not connected");
    }
  }

  /**
   * Disconnect WebSocket
   */
  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  /**
   * Get connection status
   */
  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }
}

// Export singleton instances
export const apiClient = new DataChatAPI();
export const wsClient = new DataChatWebSocket();
