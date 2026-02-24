import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement } from "react";

import { ChatInterface } from "@/components/chat/ChatInterface";
import { buildShareUrl } from "@/lib/share";
import { useChatStore } from "@/lib/stores/chat";

const {
  mockSystemStatus,
  mockListDatabases,
  mockListConversations,
  mockUpsertConversation,
  mockDeleteConversation,
  mockEmitEntryEvent,
  mockGetDatabaseSchema,
  mockStreamChat,
  mockSearchParamGet,
} = vi.hoisted(() => ({
  mockSystemStatus: vi.fn(),
  mockListDatabases: vi.fn(),
  mockListConversations: vi.fn(),
  mockUpsertConversation: vi.fn(),
  mockDeleteConversation: vi.fn(),
  mockEmitEntryEvent: vi.fn(),
  mockGetDatabaseSchema: vi.fn(),
  mockStreamChat: vi.fn(),
  mockSearchParamGet: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  useSearchParams: () => ({ get: mockSearchParamGet }),
}));

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return {
    ...actual,
    apiClient: {
      ...actual.apiClient,
      systemStatus: mockSystemStatus,
      listDatabases: mockListDatabases,
      listConversations: mockListConversations,
      upsertConversation: mockUpsertConversation,
      deleteConversation: mockDeleteConversation,
      emitEntryEvent: mockEmitEntryEvent,
      getDatabaseSchema: mockGetDatabaseSchema,
    },
    wsClient: {
      ...actual.wsClient,
      streamChat: mockStreamChat,
    },
  };
});

const renderWithProviders = (ui: ReactElement) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, staleTime: 0 },
    },
  });
  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
};

describe("ChatInterface target database", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSearchParamGet.mockReturnValue(null);
    window.localStorage.clear();
    Element.prototype.scrollIntoView = vi.fn();

    useChatStore.getState().clearMessages();
    useChatStore.getState().setLoading(false);
    useChatStore.getState().setConnected(false);
    useChatStore.getState().resetAgentStatus();

    mockSystemStatus.mockResolvedValue({
      is_initialized: true,
      has_databases: true,
      has_system_database: true,
      has_datapoints: false,
      setup_required: [],
    });

    mockListDatabases.mockResolvedValue([
      {
        connection_id: "db_pg",
        name: "Postgres Main",
        database_url: "postgresql://postgres@localhost:5432/app",
        database_type: "postgresql",
        is_active: true,
        is_default: false,
        tags: [],
        created_at: new Date().toISOString(),
        datapoint_count: 0,
      },
      {
        connection_id: "db_mysql",
        name: "MySQL Demo",
        database_url: "mysql://root:root@localhost:3306/demo",
        database_type: "mysql",
        is_active: true,
        is_default: true,
        tags: [],
        created_at: new Date().toISOString(),
        datapoint_count: 0,
      },
    ]);
    mockGetDatabaseSchema.mockResolvedValue({
      connection_id: "db_mysql",
      database_type: "mysql",
      fetched_at: new Date().toISOString(),
      tables: [],
    });
    mockListConversations.mockResolvedValue([]);
    mockUpsertConversation.mockResolvedValue({
      frontend_session_id: "session-test",
      title: "Test conversation",
      target_database_id: "db_mysql",
      conversation_id: null,
      session_summary: null,
      session_state: {},
      messages: [],
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    mockDeleteConversation.mockResolvedValue(undefined);
    mockEmitEntryEvent.mockResolvedValue(undefined);
  });

  it("sends the default selected connection as target_database", async () => {
    renderWithProviders(<ChatInterface />);

    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    const input = screen.getByPlaceholderText("Ask a question about your data...");
    fireEvent.change(input, { target: { value: "list all available tables" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    const request = mockStreamChat.mock.calls[0][0] as Record<string, unknown>;
    expect(request.target_database).toBe("db_mysql");
  });

  it("uses the user-selected connection id for chat", async () => {
    renderWithProviders(<ChatInterface />);

    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    const select = await screen.findByLabelText("Target database");
    fireEvent.change(select, { target: { value: "db_pg" } });

    const input = screen.getByPlaceholderText("Ask a question about your data...");
    fireEvent.change(input, { target: { value: "show columns in customers" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    const request = mockStreamChat.mock.calls[0][0] as Record<string, unknown>;
    expect(request.target_database).toBe("db_pg");
  });

  it("sends selected workflow mode in chat requests", async () => {
    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    fireEvent.change(screen.getByLabelText("Workflow mode"), {
      target: { value: "finance_variance_v1" },
    });

    const input = screen.getByPlaceholderText("Ask a question about your data...");
    fireEvent.change(input, { target: { value: "show liquidity risk signals" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    const request = mockStreamChat.mock.calls[0][0] as Record<string, unknown>;
    expect(request.workflow_mode).toBe("finance_variance_v1");
  });

  it("restores input focus after response completes", async () => {
    let handlers:
      | {
          onComplete?: (response: { answer: string }) => void;
        }
      | undefined;

    mockStreamChat.mockImplementation((_request, callbacks) => {
      handlers = callbacks;
    });

    renderWithProviders(<ChatInterface />);

    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    const input = screen.getByPlaceholderText(
      "Ask a question about your data..."
    ) as HTMLInputElement;

    fireEvent.change(input, { target: { value: "list all available tables" } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    expect(handlers).toBeDefined();

    input.blur();
    expect(document.activeElement).not.toBe(input);

    await act(async () => {
      handlers?.onComplete?.({ answer: "Found tables." });
    });

    await waitFor(() => {
      expect(document.activeElement).toBe(input);
    });
  });

  it("preserves sub-answers from websocket completion for multi-question rendering", async () => {
    let handlers:
      | {
          onComplete?: (response: { answer: string; sub_answers?: Array<Record<string, unknown>> }) => void;
        }
      | undefined;

    mockStreamChat.mockImplementation((_request, callbacks) => {
      handlers = callbacks;
    });

    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    const input = screen.getByPlaceholderText(
      "Ask a question about your data..."
    ) as HTMLInputElement;
    fireEvent.change(input, {
      target: { value: "when was last sale and last inventory update" },
    });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));

    await act(async () => {
      handlers?.onComplete?.({
        answer: "I handled your request as multiple questions.",
        sub_answers: [
          {
            index: 1,
            query: "when was the last sale made",
            answer: "Last sale was 2026-02-23.",
            sql: "SELECT MAX(sold_at) FROM public.grocery_sales_transactions",
            data: { max: ["2026-02-23"] },
          },
          {
            index: 2,
            query: "when was inventory last updated",
            answer: "Last inventory update was 2026-02-24.",
            sql: "SELECT MAX(snapshot_date) FROM public.grocery_inventory_snapshots",
            data: { max: ["2026-02-24"] },
          },
        ],
      });
    });

    await waitFor(() => {
      expect(screen.getByText("Generated SQL by sub-question")).toBeInTheDocument();
    });
    expect(screen.getByText(/grocery_sales_transactions/i)).toBeInTheDocument();
    expect(screen.getByText(/grocery_inventory_snapshots/i)).toBeInTheDocument();
  });

  it("applies quick query templates into the input", async () => {
    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(mockGetDatabaseSchema).toHaveBeenCalled());

    fireEvent.click(screen.getByRole("button", { name: "List Tables" }));
    expect(
      screen.getByDisplayValue("List all available tables.")
    ).toBeInTheDocument();
  });

  it("opens and closes keyboard shortcuts modal from keyboard shortcut", async () => {
    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    fireEvent.keyDown(window, { key: "/", ctrlKey: true });
    expect(
      screen.getByRole("dialog", { name: "Keyboard shortcuts" })
    ).toBeInTheDocument();

    fireEvent.keyDown(window, { key: "Escape" });
    await waitFor(() =>
      expect(
        screen.queryByRole("dialog", { name: "Keyboard shortcuts" })
      ).not.toBeInTheDocument()
    );
  });

  it("filters conversation history from sidebar search", async () => {
    window.localStorage.setItem(
      "datachat.conversation.history.v1",
      JSON.stringify([
        {
          frontendSessionId: "session-a",
          title: "Sales trend review",
          targetDatabaseId: "db_mysql",
          conversationId: "conv_a",
          sessionSummary: null,
          sessionState: null,
          updatedAt: new Date().toISOString(),
          messages: [],
        },
        {
          frontendSessionId: "session-b",
          title: "Inventory checks",
          targetDatabaseId: "db_pg",
          conversationId: "conv_b",
          sessionSummary: null,
          sessionState: null,
          updatedAt: new Date().toISOString(),
          messages: [],
        },
      ])
    );

    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(mockGetDatabaseSchema).toHaveBeenCalled());
    fireEvent.click(screen.getByLabelText("Toggle conversation history sidebar"));

    expect(screen.getByText("Sales trend review")).toBeInTheDocument();
    expect(screen.getByText("Inventory checks")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Search saved conversations"), {
      target: { value: "sales" },
    });

    expect(screen.getByText("Sales trend review")).toBeInTheDocument();
    expect(screen.queryByText("Inventory checks")).not.toBeInTheDocument();
  });

  it("deleting the active conversation removes it and starts a fresh session", async () => {
    const currentSessionId = useChatStore.getState().frontendSessionId;
    window.localStorage.setItem(
      "datachat.conversation.history.v1",
      JSON.stringify([
        {
          frontendSessionId: currentSessionId,
          title: "Active conversation",
          targetDatabaseId: "db_mysql",
          conversationId: "conv_active",
          sessionSummary: null,
          sessionState: null,
          updatedAt: new Date().toISOString(),
          messages: [],
        },
      ])
    );

    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(mockGetDatabaseSchema).toHaveBeenCalled());
    fireEvent.click(screen.getByLabelText("Toggle conversation history sidebar"));

    expect(screen.getByText("Active conversation")).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("Delete conversation Active conversation"));

    await waitFor(() => {
      expect(screen.queryByText("Active conversation")).not.toBeInTheDocument();
    });
    expect(mockDeleteConversation).toHaveBeenCalledWith(currentSessionId);
    expect(useChatStore.getState().frontendSessionId).not.toBe(currentSessionId);
  });

  it("sends SQL editor content as direct SQL execution request", async () => {
    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: /SQL Editor/i }));
    fireEvent.change(screen.getByLabelText("SQL editor input"), {
      target: { value: "SELECT * FROM users LIMIT 10;" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run SQL draft" }));

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    const request = mockStreamChat.mock.calls[0][0] as Record<string, unknown>;
    expect(request.message).toBe("SELECT * FROM users LIMIT 10;");
    expect(request.execution_mode).toBe("direct_sql");
    expect(request.sql).toBe("SELECT * FROM users LIMIT 10;");
  });

  it("keeps natural-language retry payload unchanged when it contains SQL fences", async () => {
    let handlers:
      | {
          onError?: (message: string) => void;
        }
      | undefined;

    mockStreamChat.mockImplementation((_request, callbacks) => {
      handlers = callbacks;
    });

    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    const prompt =
      "Explain why this query fails: ```sql SELECT * FROM users LIMIT 10 ```";
    const input = screen.getByPlaceholderText(
      "Ask a question about your data..."
    ) as HTMLInputElement;
    fireEvent.change(input, { target: { value: prompt } });
    fireEvent.keyDown(input, { key: "Enter", code: "Enter" });

    await waitFor(() => expect(mockStreamChat).toHaveBeenCalledTimes(1));
    await act(async () => {
      handlers?.onError?.("request failed");
    });

    fireEvent.click(screen.getByRole("button", { name: "Retry Query" }));

    const restoredInput = screen.getByPlaceholderText(
      "Ask a question about your data..."
    ) as HTMLInputElement;
    expect(restoredInput.value).toBe(prompt);
    expect(screen.queryByLabelText("SQL editor input")).not.toBeInTheDocument();
  });

  it("restores focus to natural-language input after switching from SQL mode via template", async () => {
    renderWithProviders(<ChatInterface />);
    await waitFor(() => expect(mockListDatabases).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: /SQL Editor/i }));
    const sqlEditor = screen.getByLabelText("SQL editor input");
    expect(sqlEditor).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "List Tables" }));

    const input = screen.getByPlaceholderText(
      "Ask a question about your data..."
    ) as HTMLInputElement;
    await waitFor(() => {
      expect(document.activeElement).toBe(input);
    });
  });

  it("loads a shared result payload from the URL", async () => {
    const sharedUrl = buildShareUrl(
      {
        created_at: new Date().toISOString(),
        answer: "Shared result answer",
        sql: "SELECT business_date, revenue FROM public.daily_revenue",
        data: {
          business_date: ["2026-01-01", "2026-01-02"],
          revenue: [100, 125],
        },
        visualization_hint: "line_chart",
        visualization_metadata: { deterministic: "line_chart" },
        sources: [],
        answer_source: "sql",
        answer_confidence: 1,
      },
      "http://localhost:3000/"
    );
    const token = new URL(sharedUrl).searchParams.get("share");
    mockSearchParamGet.mockImplementation((key: string) => (key === "share" ? token : null));

    renderWithProviders(<ChatInterface />);

    await waitFor(() => {
      expect(screen.getByText("Shared result answer")).toBeInTheDocument();
    });
  });
});
