"use client";

import { History, PanelLeftClose, PanelLeftOpen, Plus, Search, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";

import type { ConversationSnapshot } from "./chatTypes";

interface ConversationHistorySidebarProps {
  isOpen: boolean;
  frontendSessionId: string;
  conversationSearch: string;
  sortedConversationHistory: ConversationSnapshot[];
  formatSnapshotTime: (value: string) => string;
  onToggle: () => void;
  onStartNewConversation: () => void;
  onSearchChange: (value: string) => void;
  onLoadConversation: (snapshot: ConversationSnapshot) => void;
  onDeleteConversation: (sessionId: string) => void;
}

export function ConversationHistorySidebar({
  isOpen,
  frontendSessionId,
  conversationSearch,
  sortedConversationHistory,
  formatSnapshotTime,
  onToggle,
  onStartNewConversation,
  onSearchChange,
  onLoadConversation,
  onDeleteConversation,
}: ConversationHistorySidebarProps) {
  return (
    <aside
      className={`hidden border-r border-border/70 bg-muted/30 transition-all duration-200 lg:flex lg:flex-col ${
        isOpen ? "w-72" : "w-14"
      }`}
      role="complementary"
      aria-label="Conversation history sidebar"
    >
      <div className="flex items-center justify-between border-b px-2 py-2">
        <Button
          variant="ghost"
          size="icon"
          onClick={onToggle}
          aria-label="Toggle conversation history sidebar"
        >
          {isOpen ? <PanelLeftClose size={16} /> : <PanelLeftOpen size={16} />}
        </Button>
        {isOpen && (
          <>
            <div className="text-xs font-medium">Conversations</div>
            <Button
              variant="ghost"
              size="icon"
              onClick={onStartNewConversation}
              aria-label="Start new conversation"
            >
              <Plus size={15} />
            </Button>
          </>
        )}
      </div>
      {isOpen ? (
        <div className="flex min-h-0 flex-1 flex-col p-2">
          <div className="mb-2 flex items-center gap-1 rounded-md border border-border/70 bg-background/80 px-2">
            <Search size={12} className="text-muted-foreground" />
            <input
              type="text"
              value={conversationSearch}
              onChange={(event) => onSearchChange(event.target.value)}
              placeholder="Search conversations..."
              className="h-8 w-full bg-transparent text-xs outline-none"
              aria-label="Search saved conversations"
            />
          </div>
          <div className="mb-2 text-[11px] text-muted-foreground">
            {sortedConversationHistory.length} conversation
            {sortedConversationHistory.length === 1 ? "" : "s"}
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto" role="list" aria-label="Saved conversations">
            {sortedConversationHistory.length === 0 ? (
              <div className="rounded border border-dashed p-3 text-xs text-muted-foreground">
                {conversationSearch.trim()
                  ? "No conversations matched your search."
                  : "No saved conversations yet."}
              </div>
            ) : (
              <div className="space-y-2">
                {sortedConversationHistory.map((snapshot) => {
                  const isActive = snapshot.frontendSessionId === frontendSessionId;
                  return (
                    <div
                      key={snapshot.frontendSessionId}
                      className={`rounded border ${
                        isActive ? "border-primary/40 bg-primary/5" : "border-border/70 bg-background/60"
                      }`}
                    >
                      <button
                        type="button"
                        onClick={() => onLoadConversation(snapshot)}
                        className="w-full px-2 py-2 text-left"
                        aria-label={`Load conversation ${snapshot.title}`}
                        aria-current={isActive ? "true" : undefined}
                      >
                        <p className="truncate text-xs font-medium">{snapshot.title}</p>
                        <p className="mt-1 text-[11px] text-muted-foreground">
                          {formatSnapshotTime(snapshot.updatedAt)}
                        </p>
                        {snapshot.targetDatabaseId && (
                          <p className="mt-1 text-[11px] text-muted-foreground">
                            DB: {snapshot.targetDatabaseId}
                          </p>
                        )}
                      </button>
                      <div className="flex justify-end border-t border-border/60 px-1 py-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 px-2 text-[11px]"
                          aria-label={`Delete conversation ${snapshot.title}`}
                          onClick={(event) => {
                            event.stopPropagation();
                            onDeleteConversation(snapshot.frontendSessionId);
                          }}
                        >
                          <Trash2 size={12} />
                        </Button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="flex flex-1 items-center justify-center text-muted-foreground">
          <History size={16} />
        </div>
      )}
    </aside>
  );
}
