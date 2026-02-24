import type { Message as ChatStoreMessage } from "@/lib/stores/chat";

export type SerializedMessage = Omit<ChatStoreMessage, "timestamp"> & {
  timestamp: string;
};

export interface ConversationSnapshot {
  frontendSessionId: string;
  title: string;
  targetDatabaseId: string | null;
  conversationId: string | null;
  sessionSummary: string | null;
  sessionState: Record<string, unknown> | null;
  updatedAt: string;
  createdAt: string | null;
  messages: SerializedMessage[];
}
