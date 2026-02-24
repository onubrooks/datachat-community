export type ResultLayoutMode = "stacked" | "tabbed";
export type ThemeMode = "system" | "light" | "dark";

const RESULT_LAYOUT_KEY = "datachat.resultLayoutMode";
const SHOW_AGENT_TIMINGS_KEY = "datachat.showAgentTimingBreakdown";
const SYNTHESIZE_SIMPLE_SQL_KEY = "datachat.synthesizeSimpleSql";
const SHOW_LIVE_REASONING_KEY = "datachat.showLiveReasoning";
const THEME_MODE_KEY = "datachat.themeMode";

export const getResultLayoutMode = (): ResultLayoutMode => {
  if (typeof window === "undefined") {
    return "stacked";
  }
  const value = window.localStorage.getItem(RESULT_LAYOUT_KEY);
  if (value === "stacked" || value === "tabbed") {
    return value;
  }
  return "stacked";
};

export const setResultLayoutMode = (mode: ResultLayoutMode) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(RESULT_LAYOUT_KEY, mode);
};

export const getShowAgentTimingBreakdown = (): boolean => {
  if (typeof window === "undefined") {
    return true;
  }
  const value = window.localStorage.getItem(SHOW_AGENT_TIMINGS_KEY);
  if (value === null) {
    return true;
  }
  return value === "true";
};

export const setShowAgentTimingBreakdown = (enabled: boolean) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SHOW_AGENT_TIMINGS_KEY, String(enabled));
};

export const getSynthesizeSimpleSql = (): boolean => {
  if (typeof window === "undefined") {
    return true;
  }
  const value = window.localStorage.getItem(SYNTHESIZE_SIMPLE_SQL_KEY);
  if (value === null) {
    return true;
  }
  return value === "true";
};

export const setSynthesizeSimpleSql = (enabled: boolean) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SYNTHESIZE_SIMPLE_SQL_KEY, String(enabled));
};

export const getShowLiveReasoning = (): boolean => {
  if (typeof window === "undefined") {
    return true;
  }
  const value = window.localStorage.getItem(SHOW_LIVE_REASONING_KEY);
  if (value === null) {
    return true;
  }
  return value === "true";
};

export const setShowLiveReasoning = (enabled: boolean) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SHOW_LIVE_REASONING_KEY, String(enabled));
};

export const getThemeMode = (): ThemeMode => {
  if (typeof window === "undefined") {
    return "system";
  }
  const value = window.localStorage.getItem(THEME_MODE_KEY);
  if (value === "light" || value === "dark" || value === "system") {
    return value;
  }
  return "system";
};

export const setThemeMode = (mode: ThemeMode) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(THEME_MODE_KEY, mode);
};

export const applyThemeMode = (mode: ThemeMode) => {
  if (typeof window === "undefined") return;
  const root = window.document.documentElement;
  const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
  const shouldUseDark = mode === "dark" || (mode === "system" && prefersDark);
  root.classList.toggle("dark", shouldUseDark);
};
