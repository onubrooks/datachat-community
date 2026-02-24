/**
 * Settings Page
 *
 * UI preferences and behavior settings.
 */

"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  applyThemeMode,
  getSynthesizeSimpleSql,
  getShowLiveReasoning,
  getThemeMode,
  getResultLayoutMode,
  getShowAgentTimingBreakdown,
  setResultLayoutMode,
  setShowLiveReasoning,
  setShowAgentTimingBreakdown,
  setSynthesizeSimpleSql,
  setThemeMode,
  type ResultLayoutMode,
  type ThemeMode,
} from "@/lib/settings";
import { useChatStore } from "@/lib/stores/chat";

type SettingOption = {
  label: string;
  selected: boolean;
  onClick: () => void;
  ariaLabel: string;
};

function SettingRow({
  title,
  description,
  options,
}: {
  title: string;
  description: string;
  options: SettingOption[];
}) {
  return (
    <div className="flex flex-col gap-3 border-b border-border/70 py-4 last:border-b-0">
      <div>
        <h2 className="text-sm font-semibold text-foreground">{title}</h2>
        <p className="mt-1 text-xs text-muted-foreground">{description}</p>
      </div>
      <div className="inline-flex w-full max-w-full flex-wrap items-center gap-2 rounded-md border border-border/80 bg-muted/30 p-1 sm:w-auto">
        {options.map((option) => (
          <button
            key={option.label}
            type="button"
            onClick={option.onClick}
            aria-label={option.ariaLabel}
            className={`min-w-[78px] rounded-md px-3 py-1.5 text-xs font-medium transition ${
              option.selected
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:bg-background/70 hover:text-foreground"
            }`}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export default function SettingsPage() {
  const [resultLayout, setResultLayout] = useState<ResultLayoutMode>("stacked");
  const [showAgentTimings, setShowAgentTimings] = useState(true);
  const [synthesizeSimpleSql, setSynthesizeSimpleSqlState] = useState(true);
  const [showLiveReasoning, setShowLiveReasoningState] = useState(true);
  const [themeMode, setThemeModeState] = useState<ThemeMode>("system");
  const [clearedAt, setClearedAt] = useState<Date | null>(null);
  const clearMessages = useChatStore((state) => state.clearMessages);

  useEffect(() => {
    setResultLayout(getResultLayoutMode());
    setShowAgentTimings(getShowAgentTimingBreakdown());
    setSynthesizeSimpleSqlState(getSynthesizeSimpleSql());
    setShowLiveReasoningState(getShowLiveReasoning());
    setThemeModeState(getThemeMode());
  }, []);

  const handleLayoutChange = (value: ResultLayoutMode) => {
    setResultLayout(value);
    setResultLayoutMode(value);
  };

  const handleAgentTimingsChange = (value: boolean) => {
    setShowAgentTimings(value);
    setShowAgentTimingBreakdown(value);
  };

  const handleSynthesizeSimpleSqlChange = (value: boolean) => {
    setSynthesizeSimpleSqlState(value);
    setSynthesizeSimpleSql(value);
  };

  const handleShowLiveReasoningChange = (value: boolean) => {
    setShowLiveReasoningState(value);
    setShowLiveReasoning(value);
  };

  const handleThemeModeChange = (value: ThemeMode) => {
    setThemeModeState(value);
    setThemeMode(value);
    applyThemeMode(value);
  };

  const handleClearChatHistory = () => {
    clearMessages();
    setClearedAt(new Date());
  };

  return (
    <main className="h-screen overflow-auto p-4 sm:p-6">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
        <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Settings</h1>
          <p className="text-sm text-muted-foreground">
            Configure display and interaction preferences.
          </p>
        </div>
        <Button asChild variant="secondary" size="sm">
          <Link href="/">Back to Chat</Link>
        </Button>
      </div>

        <Card className="p-4 sm:p-5">
          <div className="mb-1 text-sm font-semibold">Display & Behavior</div>
          <p className="text-xs text-muted-foreground">
            Keep these defaults unless you have a specific reason to tune the interface behavior.
          </p>
          <div className="mt-2">
            <SettingRow
              title="Theme"
              description="Choose light, dark, or follow your system preference."
              options={[
                {
                  label: "Light",
                  selected: themeMode === "light",
                  onClick: () => handleThemeModeChange("light"),
                  ariaLabel: "Set light theme",
                },
                {
                  label: "Dark",
                  selected: themeMode === "dark",
                  onClick: () => handleThemeModeChange("dark"),
                  ariaLabel: "Set dark theme",
                },
                {
                  label: "System",
                  selected: themeMode === "system",
                  onClick: () => handleThemeModeChange("system"),
                  ariaLabel: "Use system theme",
                },
              ]}
            />
            <SettingRow
              title="Result Layout"
              description="Choose whether result sections appear in one flow or as tabbed panels."
              options={[
                {
                  label: "Stacked",
                  selected: resultLayout === "stacked",
                  onClick: () => handleLayoutChange("stacked"),
                  ariaLabel: "Use stacked result layout",
                },
                {
                  label: "Tabbed",
                  selected: resultLayout === "tabbed",
                  onClick: () => handleLayoutChange("tabbed"),
                  ariaLabel: "Use tabbed result layout",
                },
              ]}
            />
            <SettingRow
              title="Agent Timing Breakdown"
              description="Show or hide per-agent runtime details in response timing."
              options={[
                {
                  label: "Show",
                  selected: showAgentTimings,
                  onClick: () => handleAgentTimingsChange(true),
                  ariaLabel: "Show agent timing breakdown",
                },
                {
                  label: "Hide",
                  selected: !showAgentTimings,
                  onClick: () => handleAgentTimingsChange(false),
                  ariaLabel: "Hide agent timing breakdown",
                },
              ]}
            />
            <SettingRow
              title="Live Reasoning Stream"
              description="Control whether temporary in-progress reasoning notes appear while generating."
              options={[
                {
                  label: "Show",
                  selected: showLiveReasoning,
                  onClick: () => handleShowLiveReasoningChange(true),
                  ariaLabel: "Show live reasoning stream",
                },
                {
                  label: "Hide",
                  selected: !showLiveReasoning,
                  onClick: () => handleShowLiveReasoningChange(false),
                  ariaLabel: "Hide live reasoning stream",
                },
              ]}
            />
            <SettingRow
              title="Simple SQL Response Synthesis"
              description="Enable richer wording for simple SQL responses, or disable for lower latency."
              options={[
                {
                  label: "On",
                  selected: synthesizeSimpleSql,
                  onClick: () => handleSynthesizeSimpleSqlChange(true),
                  ariaLabel: "Enable simple SQL synthesis",
                },
                {
                  label: "Off",
                  selected: !synthesizeSimpleSql,
                  onClick: () => handleSynthesizeSimpleSqlChange(false),
                  ariaLabel: "Disable simple SQL synthesis",
                },
              ]}
            />
          </div>
        </Card>

        <Card className="border-destructive/20 p-4 sm:p-5">
          <div className="text-sm font-semibold">Chat History</div>
          <p className="mt-1 text-xs text-muted-foreground">
            Clears the locally saved chat session and starts a new frontend session id.
          </p>
          <div className="mt-3 flex items-center gap-3">
            <Button type="button" variant="destructive" size="sm" onClick={handleClearChatHistory}>
              Clear Chat History
            </Button>
            {clearedAt && (
              <span className="text-xs text-muted-foreground">
                Cleared at {clearedAt.toLocaleTimeString()}
              </span>
            )}
          </div>
        </Card>
      </div>
    </main>
  );
}
