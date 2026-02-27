/**
 * Settings Page
 *
 * UI preferences + runtime configuration settings.
 */

"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";

import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  DataChatAPI,
  type RuntimeSettingsResponse,
  type RuntimeSettingsUpdateRequest,
} from "@/lib/api";
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

const api = new DataChatAPI();

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

function SourcePill({ source }: { source?: string }) {
  const label = source || "unknown";
  const color =
    label === "env"
      ? "bg-blue-100 text-blue-900"
      : label === "config"
        ? "bg-emerald-100 text-emerald-900"
        : "bg-muted text-muted-foreground";
  return <span className={`rounded px-1.5 py-0.5 text-[10px] ${color}`}>{label}</span>;
}

export default function SettingsPage() {
  const [resultLayout, setResultLayout] = useState<ResultLayoutMode>("stacked");
  const [showAgentTimings, setShowAgentTimings] = useState(true);
  const [synthesizeSimpleSql, setSynthesizeSimpleSqlState] = useState(true);
  const [showLiveReasoning, setShowLiveReasoningState] = useState(true);
  const [themeMode, setThemeModeState] = useState<ThemeMode>("system");
  const [clearedAt, setClearedAt] = useState<Date | null>(null);
  const clearMessages = useChatStore((state) => state.clearMessages);

  const [runtimeLoading, setRuntimeLoading] = useState(true);
  const [runtimeSaving, setRuntimeSaving] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [runtimeNotice, setRuntimeNotice] = useState<string | null>(null);
  const [settingsSnapshot, setSettingsSnapshot] = useState<RuntimeSettingsResponse | null>(null);

  const [targetDatabaseUrl, setTargetDatabaseUrl] = useState("");
  const [systemDatabaseUrl, setSystemDatabaseUrl] = useState("");
  const [llmDefaultProvider, setLlmDefaultProvider] = useState("openai");
  const [llmOpenaiModel, setLlmOpenaiModel] = useState("");
  const [llmOpenaiModelMini, setLlmOpenaiModelMini] = useState("");
  const [llmAnthropicModel, setLlmAnthropicModel] = useState("");
  const [llmAnthropicModelMini, setLlmAnthropicModelMini] = useState("");
  const [llmGoogleModel, setLlmGoogleModel] = useState("");
  const [llmGoogleModelMini, setLlmGoogleModelMini] = useState("");
  const [llmLocalModel, setLlmLocalModel] = useState("");
  const [llmTemperature, setLlmTemperature] = useState("");

  const [databaseCredentialsKeyInput, setDatabaseCredentialsKeyInput] = useState("");
  const [llmOpenaiApiKeyInput, setLlmOpenaiApiKeyInput] = useState("");
  const [llmAnthropicApiKeyInput, setLlmAnthropicApiKeyInput] = useState("");
  const [llmGoogleApiKeyInput, setLlmGoogleApiKeyInput] = useState("");

  useEffect(() => {
    setResultLayout(getResultLayoutMode());
    setShowAgentTimings(getShowAgentTimingBreakdown());
    setSynthesizeSimpleSqlState(getSynthesizeSimpleSql());
    setShowLiveReasoningState(getShowLiveReasoning());
    setThemeModeState(getThemeMode());
  }, []);

  const loadRuntimeSettings = async () => {
    setRuntimeLoading(true);
    setRuntimeError(null);
    try {
      const settings = await api.getSystemSettings();
      setSettingsSnapshot(settings);
      setTargetDatabaseUrl(settings.target_database_url || "");
      setSystemDatabaseUrl(settings.system_database_url || "");
      setLlmDefaultProvider(settings.llm_default_provider || "openai");
      setLlmOpenaiModel(settings.llm_openai_model || "");
      setLlmOpenaiModelMini(settings.llm_openai_model_mini || "");
      setLlmAnthropicModel(settings.llm_anthropic_model || "");
      setLlmAnthropicModelMini(settings.llm_anthropic_model_mini || "");
      setLlmGoogleModel(settings.llm_google_model || "");
      setLlmGoogleModelMini(settings.llm_google_model_mini || "");
      setLlmLocalModel(settings.llm_local_model || "");
      setLlmTemperature(settings.llm_temperature || "");
      setDatabaseCredentialsKeyInput("");
      setLlmOpenaiApiKeyInput("");
      setLlmAnthropicApiKeyInput("");
      setLlmGoogleApiKeyInput("");
    } catch (err) {
      setRuntimeError(err instanceof Error ? err.message : "Failed to load runtime settings.");
    } finally {
      setRuntimeLoading(false);
    }
  };

  useEffect(() => {
    void loadRuntimeSettings();
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

  const saveRuntimeSettings = async (
    payload: RuntimeSettingsUpdateRequest,
    notice: string
  ) => {
    setRuntimeSaving(true);
    setRuntimeError(null);
    setRuntimeNotice(null);
    try {
      const updated = await api.updateSystemSettings(payload);
      setSettingsSnapshot(updated);
      setRuntimeNotice(notice);
      await loadRuntimeSettings();
    } catch (err) {
      setRuntimeError(err instanceof Error ? err.message : "Failed to save runtime settings.");
    } finally {
      setRuntimeSaving(false);
    }
  };

  const saveCoreRuntime = async () => {
    await saveRuntimeSettings(
      {
        target_database_url: targetDatabaseUrl || null,
        system_database_url: systemDatabaseUrl || null,
        llm_default_provider: llmDefaultProvider || null,
        llm_openai_model: llmOpenaiModel || null,
        llm_openai_model_mini: llmOpenaiModelMini || null,
        llm_anthropic_model: llmAnthropicModel || null,
        llm_anthropic_model_mini: llmAnthropicModelMini || null,
        llm_google_model: llmGoogleModel || null,
        llm_google_model_mini: llmGoogleModelMini || null,
        llm_local_model: llmLocalModel || null,
        llm_temperature: llmTemperature || null,
      },
      "Runtime configuration saved."
    );
  };

  const saveSecrets = async () => {
    await saveRuntimeSettings(
      {
        database_credentials_key: databaseCredentialsKeyInput || undefined,
        llm_openai_api_key: llmOpenaiApiKeyInput || undefined,
        llm_anthropic_api_key: llmAnthropicApiKeyInput || undefined,
        llm_google_api_key: llmGoogleApiKeyInput || undefined,
      },
      "Secrets updated."
    );
  };

  const generateCredentialsKey = async () => {
    await saveRuntimeSettings(
      { generate_database_credentials_key: true },
      "Generated and saved a new DATABASE_CREDENTIALS_KEY."
    );
  };

  const runtimeStatus = useMemo(() => {
    if (!settingsSnapshot) {
      return null;
    }
    if (settingsSnapshot.runtime_valid) {
      return (
        <div className="rounded-md border border-emerald-300 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
          Runtime validation: healthy
        </div>
      );
    }
    return (
      <div className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900">
        Runtime validation issue: {settingsSnapshot.runtime_error || "Unknown error"}
      </div>
    );
  }, [settingsSnapshot]);

  return (
    <main className="h-screen overflow-auto p-4 sm:p-6">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Settings</h1>
            <p className="text-sm text-muted-foreground">
              Configure UI behavior and runtime setup without editing .env.
            </p>
          </div>
          <Button asChild variant="secondary" size="sm">
            <Link href="/">Back to Chat</Link>
          </Button>
        </div>

        <div className="grid gap-4 xl:grid-cols-2">
          <Card className="p-4 sm:p-5">
            <div className="mb-1 text-sm font-semibold">Display & Behavior</div>
            <p className="text-xs text-muted-foreground">
              Local UI preferences for this browser session.
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
                description="Control temporary in-progress reasoning notes while generating."
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

          <Card className="p-4 sm:p-5">
            <div className="mb-1 text-sm font-semibold">Runtime Configuration</div>
            <p className="text-xs text-muted-foreground">
              Persist backend connection/provider settings in DataChat config.
            </p>

            <div className="mt-3 space-y-3">
              {runtimeLoading && (
                <div className="text-xs text-muted-foreground">Loading runtime settings...</div>
              )}
              {runtimeStatus}
              {runtimeError && (
                <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                  {runtimeError}
                </div>
              )}
              {runtimeNotice && (
                <div className="rounded-md border border-emerald-300 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
                  {runtimeNotice}
                </div>
              )}
            </div>

            <div className="mt-4 space-y-3">
              <div className="text-xs font-semibold text-muted-foreground">Database</div>
              <div className="space-y-2">
                <label className="text-xs text-muted-foreground">Target Database URL</label>
                <Input
                  value={targetDatabaseUrl}
                  onChange={(event) => setTargetDatabaseUrl(event.target.value)}
                  placeholder="postgresql://user:pass@host:5432/database"
                />
                <SourcePill source={settingsSnapshot?.source?.target_database_url} />
              </div>
              <div className="space-y-2">
                <label className="text-xs text-muted-foreground">System Database URL</label>
                <Input
                  value={systemDatabaseUrl}
                  onChange={(event) => setSystemDatabaseUrl(event.target.value)}
                  placeholder="postgresql://user:pass@host:5432/datachat"
                />
                <SourcePill source={settingsSnapshot?.source?.system_database_url} />
              </div>
              <div className="space-y-2 rounded-md border border-border/80 bg-muted/20 p-3">
                <div className="flex items-center justify-between">
                  <div className="text-xs font-medium">DATABASE_CREDENTIALS_KEY</div>
                  <SourcePill source={settingsSnapshot?.source?.database_credentials_key} />
                </div>
                <div className="text-[11px] text-muted-foreground">
                  Current:{" "}
                  {settingsSnapshot?.database_credentials_key_preview || "not configured"}
                </div>
                <Input
                  value={databaseCredentialsKeyInput}
                  onChange={(event) => setDatabaseCredentialsKeyInput(event.target.value)}
                  placeholder="Paste a Fernet key or generate one below"
                />
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    onClick={generateCredentialsKey}
                    disabled={runtimeSaving}
                  >
                    Generate Key
                  </Button>
                </div>
              </div>
            </div>

            <div className="mt-4 space-y-3">
              <div className="text-xs font-semibold text-muted-foreground">LLM Runtime</div>
              <div className="space-y-2">
                <label className="text-xs text-muted-foreground">Default Provider</label>
                <select
                  className="h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                  value={llmDefaultProvider}
                  onChange={(event) => setLlmDefaultProvider(event.target.value)}
                >
                  <option value="openai">openai</option>
                  <option value="anthropic">anthropic</option>
                  <option value="google">google</option>
                  <option value="local">local</option>
                </select>
                <SourcePill source={settingsSnapshot?.source?.llm_default_provider} />
              </div>

              <div className="grid gap-2 md:grid-cols-2">
                <Input
                  value={llmOpenaiModel}
                  onChange={(event) => setLlmOpenaiModel(event.target.value)}
                  placeholder="OpenAI model"
                />
                <Input
                  value={llmOpenaiModelMini}
                  onChange={(event) => setLlmOpenaiModelMini(event.target.value)}
                  placeholder="OpenAI mini model"
                />
                <Input
                  value={llmAnthropicModel}
                  onChange={(event) => setLlmAnthropicModel(event.target.value)}
                  placeholder="Anthropic model"
                />
                <Input
                  value={llmAnthropicModelMini}
                  onChange={(event) => setLlmAnthropicModelMini(event.target.value)}
                  placeholder="Anthropic mini model"
                />
                <Input
                  value={llmGoogleModel}
                  onChange={(event) => setLlmGoogleModel(event.target.value)}
                  placeholder="Google model"
                />
                <Input
                  value={llmGoogleModelMini}
                  onChange={(event) => setLlmGoogleModelMini(event.target.value)}
                  placeholder="Google mini model"
                />
                <Input
                  value={llmLocalModel}
                  onChange={(event) => setLlmLocalModel(event.target.value)}
                  placeholder="Local model"
                />
                <Input
                  value={llmTemperature}
                  onChange={(event) => setLlmTemperature(event.target.value)}
                  placeholder="Temperature (e.g. 0.0)"
                />
              </div>

              <div className="space-y-2 rounded-md border border-border/80 bg-muted/20 p-3">
                <div className="text-xs font-medium">LLM API Keys</div>
                <div className="grid gap-2 md:grid-cols-2">
                  <Input
                    value={llmOpenaiApiKeyInput}
                    onChange={(event) => setLlmOpenaiApiKeyInput(event.target.value)}
                    placeholder={`OpenAI key (${settingsSnapshot?.llm_openai_api_key_preview || "not set"})`}
                  />
                  <Input
                    value={llmAnthropicApiKeyInput}
                    onChange={(event) => setLlmAnthropicApiKeyInput(event.target.value)}
                    placeholder={`Anthropic key (${settingsSnapshot?.llm_anthropic_api_key_preview || "not set"})`}
                  />
                  <Input
                    value={llmGoogleApiKeyInput}
                    onChange={(event) => setLlmGoogleApiKeyInput(event.target.value)}
                    placeholder={`Google key (${settingsSnapshot?.llm_google_api_key_preview || "not set"})`}
                  />
                </div>
                <div className="flex flex-wrap gap-2 text-[11px] text-muted-foreground">
                  <div>OpenAI: {settingsSnapshot?.llm_openai_api_key_present ? "set" : "missing"}</div>
                  <div>Anthropic: {settingsSnapshot?.llm_anthropic_api_key_present ? "set" : "missing"}</div>
                  <div>Google: {settingsSnapshot?.llm_google_api_key_present ? "set" : "missing"}</div>
                </div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              <Button type="button" onClick={saveCoreRuntime} disabled={runtimeSaving}>
                {runtimeSaving ? "Saving..." : "Save Runtime Config"}
              </Button>
              <Button
                type="button"
                variant="secondary"
                onClick={saveSecrets}
                disabled={runtimeSaving}
              >
                Save Secrets
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={() => void loadRuntimeSettings()}
                disabled={runtimeSaving}
              >
                Reload
              </Button>
            </div>
          </Card>
        </div>

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
