export interface SharedResultPayload {
  version: 1;
  created_at: string;
  answer: string;
  sql?: string | null;
  data?: Record<string, unknown[]> | null;
  visualization_hint?: string | null;
  visualization_metadata?: Record<string, unknown> | null;
  sources?: Array<{
    datapoint_id: string;
    type: string;
    name: string;
    relevance_score: number;
  }>;
  answer_source?: string | null;
  answer_confidence?: number | null;
}

const SHARE_PARAM = "share";
const MAX_SHARED_DATA_ROWS = 100;

const toBase64Url = (value: string): string => {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
};

const fromBase64Url = (value: string): string => {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padding = "=".repeat((4 - (normalized.length % 4)) % 4);
  const binary = atob(`${normalized}${padding}`);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
};

const clampSharedData = (
  data: Record<string, unknown[]> | null | undefined
): Record<string, unknown[]> | null => {
  if (!data) {
    return null;
  }
  return Object.fromEntries(
    Object.entries(data).map(([column, values]) => [
      column,
      Array.isArray(values) ? values.slice(0, MAX_SHARED_DATA_ROWS) : [],
    ])
  );
};

export const buildShareUrl = (
  payload: Omit<SharedResultPayload, "version">,
  currentHref: string
): string => {
  const serialized: SharedResultPayload = {
    ...payload,
    version: 1,
    data: clampSharedData(payload.data),
  };
  const token = toBase64Url(JSON.stringify(serialized));
  const url = new URL(currentHref);
  url.searchParams.set(SHARE_PARAM, token);
  return url.toString();
};

export const decodeShareToken = (token: string | null | undefined): SharedResultPayload | null => {
  if (!token) {
    return null;
  }
  try {
    const decoded = fromBase64Url(token);
    const parsed = JSON.parse(decoded) as SharedResultPayload;
    if (parsed.version !== 1 || typeof parsed.answer !== "string") {
      return null;
    }
    return {
      ...parsed,
      data: clampSharedData(parsed.data),
      sql: parsed.sql ?? null,
      visualization_hint: parsed.visualization_hint ?? null,
      visualization_metadata: parsed.visualization_metadata ?? null,
      answer_source: parsed.answer_source ?? null,
      answer_confidence:
        typeof parsed.answer_confidence === "number" ? parsed.answer_confidence : null,
      created_at: parsed.created_at || new Date().toISOString(),
    };
  } catch {
    return null;
  }
};
