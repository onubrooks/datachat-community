import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, vi } from "vitest";

import { Message } from "@/components/chat/Message";

describe("Message", () => {
  const clipboardWriteText = vi.fn();

  beforeEach(() => {
    clipboardWriteText.mockReset();
    Object.defineProperty(window.navigator, "clipboard", {
      configurable: true,
      value: { writeText: clipboardWriteText },
    });
  });

  it("renders simple markdown bullets and bold text for assistant messages", () => {
    render(
      <Message
        message={{
          id: "msg-1",
          role: "assistant",
          content:
            "Here are the grocery stores:\n\n* **Downtown Fresh** in Austin\n* **Midtown Market** in Austin",
          timestamp: new Date(),
        }}
      />
    );

    expect(screen.getByText("Here are the grocery stores:")).toBeInTheDocument();
    const list = screen.getByRole("list");
    const items = within(list).getAllByRole("listitem");
    expect(items).toHaveLength(2);
    expect(within(items[0]).getByText("Downtown Fresh", { selector: "strong" })).toBeInTheDocument();
    expect(within(items[1]).getByText("Midtown Market", { selector: "strong" })).toBeInTheDocument();
  });

  it("renders tabbed layout with visualization tab for assistant results", () => {
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-2",
          role: "assistant",
          content: "Sales by region",
          sql: "SELECT region, total FROM sales_by_region",
          data: {
            region: ["South", "North", "East"],
            total: [120, 90, 45],
          },
          sources: [
            {
              datapoint_id: "dp_1",
              type: "Schema",
              name: "sales_by_region",
              relevance_score: 0.9,
            },
          ],
          visualization_hint: "bar_chart",
          timestamp: new Date(),
        }}
      />
    );

    expect(screen.getByRole("tab", { name: "Answer" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "SQL" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Table" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Visualization" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Evidence" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "Visualization" }));
    expect(screen.getByText("Bar Chart")).toBeInTheDocument();
  });

  it("opens SQL draft editor callback from message actions", () => {
    const onEditSqlDraft = vi.fn();
    render(
      <Message
        message={{
          id: "msg-edit-sql",
          role: "assistant",
          content: "SQL is ready.",
          sql: "SELECT id, name FROM users LIMIT 5",
          timestamp: new Date(),
        }}
        onEditSqlDraft={onEditSqlDraft}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Edit SQL draft" }));
    expect(onEditSqlDraft).toHaveBeenCalledWith("SELECT id, name FROM users LIMIT 5");
  });

  it("renders axis and legend metadata for line visualization", () => {
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-2b",
          role: "assistant",
          content: "Revenue trend",
          data: {
            business_date: ["2026-01-01", "2026-01-02", "2026-01-03"],
            revenue: [100, 130, 120],
          },
          visualization_hint: "line_chart",
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByRole("tab", { name: "Visualization" }));
    expect(screen.getByText("Line Chart")).toBeInTheDocument();
    expect(screen.getByText(/X axis:/)).toBeInTheDocument();
    expect(screen.getByText(/Y axis:/)).toBeInTheDocument();
    expect(screen.getByText(/Legend:/)).toBeInTheDocument();
  });

  it("shows line chart settings controls when toggled", () => {
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-2c",
          role: "assistant",
          content: "Revenue trend",
          data: {
            business_date: ["2026-01-01", "2026-01-02", "2026-01-03"],
            revenue: [100, 130, 120],
          },
          visualization_hint: "line_chart",
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByRole("tab", { name: "Visualization" }));
    fireEvent.click(screen.getByRole("button", { name: "Toggle line chart settings" }));

    expect(screen.getByLabelText("Line chart X axis column")).toBeInTheDocument();
    expect(screen.getByLabelText("Line chart Y axis column")).toBeInTheDocument();
    expect(screen.getByLabelText("Line chart zoom")).toBeInTheDocument();
  });

  it("collapses results and evidence sections by default in stacked mode", () => {
    const { container } = render(
      <Message
        message={{
          id: "msg-3",
          role: "assistant",
          content: "Summary",
          sql: "SELECT name, total FROM revenue",
          data: {
            name: ["A", "B"],
            total: [10, 20],
          },
          sources: [
            {
              datapoint_id: "dp_2",
              type: "Business",
              name: "Revenue Metric",
              relevance_score: 0.95,
            },
          ],
          evidence: [
            {
              datapoint_id: "dp_2",
              type: "Business",
              name: "Revenue Metric",
              reason: "Used to answer the query",
            },
          ],
          timestamp: new Date(),
        }}
      />
    );

    const details = Array.from(container.querySelectorAll("details"));
    expect(details.length).toBeGreaterThanOrEqual(2);
    for (const section of details) {
      expect(section.open).toBe(false);
    }
  });

  it("can hide agent timing breakdown while keeping summary metrics", () => {
    render(
      <Message
        showAgentTimingBreakdown={false}
        message={{
          id: "msg-4",
          role: "assistant",
          content: "Done",
          metrics: {
            total_latency_ms: 1250,
            agent_timings: {
              classifier: 120,
              context: 330,
            },
            llm_calls: 1,
            retry_count: 0,
          },
          timestamp: new Date(),
        }}
      />
    );

    expect(screen.queryByText("Classifier")).not.toBeInTheDocument();
    expect(screen.getByText("1.25s")).toBeInTheDocument();
    expect(screen.getByText("LLM calls: 1")).toBeInTheDocument();
  });

  it("does not duplicate finance brief summary when it matches the main answer", () => {
    const duplicatedText = "Net flow decreased week over week across segments.";

    render(
      <Message
        message={{
          id: "msg-finance-duplicate",
          role: "assistant",
          content: duplicatedText,
          workflow_artifacts: {
            package_version: "v1",
            domain: "finance",
            summary: duplicatedText,
            metrics: [],
            drivers: [],
            caveats: ["Review source assumptions before sharing externally."],
            sources: [],
            follow_ups: ["Compare this result against the previous equivalent period."],
          },
          timestamp: new Date(),
        }}
      />
    );

    expect(screen.getAllByText(duplicatedText)).toHaveLength(1);
    expect(screen.getByText("Finance Brief")).toBeInTheDocument();
    expect(screen.getByText("Caveats")).toBeInTheDocument();
  });

  it("renders finance brief duplicate metrics without React key warnings", () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    render(
      <Message
        message={{
          id: "msg-finance-keys",
          role: "assistant",
          content: "Finance summary",
          workflow_artifacts: {
            package_version: "v1",
            domain: "finance",
            summary: "Finance summary",
            metrics: [
              { label: "Loan Type", value: "auto" },
              { label: "Loan Type", value: "auto" },
            ],
            drivers: [],
            caveats: [],
            sources: [],
            follow_ups: [],
          },
          timestamp: new Date(),
        }}
      />
    );

    const duplicateKeyWarnings = errorSpy.mock.calls.filter((call) =>
      String(call[0]).includes("Encountered two children with the same key")
    );
    expect(duplicateKeyWarnings).toHaveLength(0);
    errorSpy.mockRestore();
  });

  it("shows agent timing breakdown in a timing tab sorted longest to shortest", () => {
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-5",
          role: "assistant",
          content: "Done",
          metrics: {
            total_latency_ms: 3250,
            agent_timings: {
              classifier: 1000,
              context: 2100,
              sql: 150,
            },
            llm_calls: 2,
            retry_count: 0,
          },
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByRole("tab", { name: "Timing" }));

    const rows = screen.getAllByText(/s$/);
    expect(rows[0]).toHaveTextContent("2.1s");
    expect(rows[1]).toHaveTextContent("1s");
    expect(rows[2]).toHaveTextContent("0.15s");
  });

  it("supports toggling between multi-question sub-answers for SQL and table views", () => {
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-6",
          role: "assistant",
          content: "I handled your request as multiple questions.",
          answer_source: "multi",
          sub_answers: [
            {
              index: 1,
              query: "Which suppliers have highest late-delivery rate?",
              answer: "Supplier A leads.",
              sql: "SELECT supplier_id, late_rate FROM supplier_late_rates LIMIT 10",
              data: {
                supplier_id: ["SUP1"],
                late_rate: [0.31],
              },
              visualization_hint: "bar_chart",
            },
            {
              index: 2,
              query: "What is the average delay in days?",
              answer: "Average delay is 2.4 days.",
              sql: "SELECT AVG(delay_days) AS avg_delay_days FROM supplier_delays",
              data: {
                avg_delay_days: [2.4],
              },
              visualization_hint: "none",
            },
          ],
          timestamp: new Date(),
        }}
      />
    );

    expect(screen.getByRole("button", { name: "Focus sub-question 1" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Focus sub-question 2" })).toBeInTheDocument();
    expect(screen.getByText("Which suppliers have highest late-delivery rate?")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "SQL" }));
    expect(screen.getByText("Generated SQL by sub-question")).toBeInTheDocument();
    expect(screen.getByText(/supplier_late_rates/)).toBeInTheDocument();
    expect(screen.getByText(/avg_delay_days/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Focus sub-question 2" }));
    expect(screen.getAllByText(/average delay in days/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/supplier_delays/)).toBeInTheDocument();
  });

  it("resets table pagination when switching to a different sub-answer", () => {
    const manyRows = Array.from({ length: 120 }, (_, index) => `row-${index + 1}`);

    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-7",
          role: "assistant",
          content: "Pagination test",
          sub_answers: [
            {
              index: 1,
              query: "First query",
              answer: "Lots of rows",
              data: {
                item: manyRows,
              },
            },
            {
              index: 2,
              query: "Second query",
              answer: "One row",
              data: {
                item: ["only-row"],
              },
            },
          ],
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByRole("tab", { name: "Table" }));
    fireEvent.click(screen.getByText(/Q1: First query/i));

    fireEvent.click(screen.getByRole("button", { name: /Next/i }));
    fireEvent.click(screen.getByRole("button", { name: /Next/i }));
    expect(screen.getByText("Page 3 of 12")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Focus sub-question 2" }));
    fireEvent.click(screen.getByText(/Q2: Second query/i));

    expect(screen.getByText("only-row")).toBeInTheDocument();
  });

  it("allows changing result table rows per page", () => {
    const manyRows = Array.from({ length: 120 }, (_, index) => `row-${index + 1}`);

    render(
      <Message
        message={{
          id: "msg-8",
          role: "assistant",
          content: "Pagination size test",
          data: {
            item: manyRows,
          },
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByText("Results (120 rows)"));
    expect(screen.getByText("Page 1 of 12")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Rows per page"), {
      target: { value: "25" },
    });

    expect(screen.getByText("Page 1 of 5")).toBeInTheDocument();
    expect(screen.getByText("Showing 1-25 of 120 rows")).toBeInTheDocument();
  });

  it("uses active sub-answer source metadata when submitting feedback", async () => {
    const onSubmitFeedback = vi.fn().mockResolvedValue(undefined);
    render(
      <Message
        displayMode="tabbed"
        message={{
          id: "msg-feedback-sub-answer",
          role: "assistant",
          content: "Composite answer",
          answer_source: "multi",
          answer_confidence: 0.22,
          sub_answers: [
            {
              index: 1,
              query: "Q1",
              answer: "A1",
              answer_source: "context",
              answer_confidence: 0.45,
            },
            {
              index: 2,
              query: "Q2",
              answer: "A2",
              answer_source: "semantic_sql",
              answer_confidence: 0.91,
            },
          ],
          timestamp: new Date(),
        }}
        onSubmitFeedback={onSubmitFeedback}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Focus sub-question 2" }));
    fireEvent.click(screen.getByRole("button", { name: "Rate answer helpful" }));

    await waitFor(() => expect(onSubmitFeedback).toHaveBeenCalledTimes(1));
    expect(onSubmitFeedback.mock.calls[0][0].answer_source).toBe("semantic_sql");
    expect(onSubmitFeedback.mock.calls[0][0].answer_confidence).toBe(0.91);
  });

  it("supports markdown export and share link actions for tabular results", async () => {
    clipboardWriteText.mockResolvedValue(undefined);
    render(
      <Message
        message={{
          id: "msg-share",
          role: "assistant",
          content: "Revenue by region",
          data: {
            region: ["South", "North"],
            revenue: [120, 90],
          },
          timestamp: new Date(),
        }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Copy query results as markdown table" }));
    await waitFor(() => expect(clipboardWriteText).toHaveBeenCalledTimes(1));
    expect(clipboardWriteText.mock.calls[0][0]).toContain("| region | revenue |");

    fireEvent.click(screen.getByRole("button", { name: "Copy share link for this result" }));
    await waitFor(() => expect(clipboardWriteText).toHaveBeenCalledTimes(2));
    expect(clipboardWriteText.mock.calls[1][0]).toContain("share=");
  });

  it("submits answer feedback and issue reports when callback is provided", async () => {
    const onSubmitFeedback = vi.fn().mockResolvedValue(undefined);
    render(
      <Message
        message={{
          id: "msg-feedback",
          role: "assistant",
          content: "Response content",
          sql: "SELECT 1",
          timestamp: new Date(),
        }}
        onSubmitFeedback={onSubmitFeedback}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Rate answer helpful" }));
    await waitFor(() => expect(onSubmitFeedback).toHaveBeenCalledTimes(1));
    expect(onSubmitFeedback.mock.calls[0][0].category).toBe("answer_feedback");
    expect(onSubmitFeedback.mock.calls[0][0].sentiment).toBe("up");

    fireEvent.click(screen.getByRole("button", { name: "Report issue with this answer" }));
    fireEvent.change(screen.getByLabelText("Issue report details"), {
      target: { value: "This used the wrong table." },
    });
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    await waitFor(() => expect(onSubmitFeedback).toHaveBeenCalledTimes(2));
    expect(onSubmitFeedback.mock.calls[1][0].category).toBe("issue_report");
  });
});
