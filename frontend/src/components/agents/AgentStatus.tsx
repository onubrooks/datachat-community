/**
 * Agent Status Component
 *
 * Displays real-time pipeline agent status during query processing:
 * - Current agent being executed
 * - Agent progress indicators
 * - Status messages and errors
 * - Agent execution history
 */

"use client";

import React from "react";
import {
  Loader2,
  CheckCircle2,
  XCircle,
  Search,
  Database,
  Code,
  ShieldCheck,
  Play,
  Sparkles,
} from "lucide-react";
import { Card, CardContent } from "../ui/card";
import { cn } from "@/lib/utils";
import { useChatStore } from "@/lib/stores/chat";

// Agent icons mapping
const AGENT_ICONS: Record<string, React.ElementType> = {
  ClassifierAgent: Search,
  ContextAgent: Database,
  SQLAgent: Code,
  ValidatorAgent: ShieldCheck,
  ExecutorAgent: Play,
  ContextAnswerAgent: Sparkles,
};

// Agent display names
const AGENT_NAMES: Record<string, string> = {
  ClassifierAgent: "Classifier",
  ContextAgent: "Context Retrieval",
  SQLAgent: "SQL Generation",
  ValidatorAgent: "Validation",
  ExecutorAgent: "Execution",
  ContextAnswerAgent: "Context Answer",
};

export function AgentStatus() {
  const { currentAgent, agentStatus, agentMessage, agentError, agentHistory } =
    useChatStore();

  // Don't show if no agent is running
  if (agentStatus === "idle") {
    return null;
  }

  const Icon = currentAgent ? AGENT_ICONS[currentAgent] : Loader2;
  const agentName = currentAgent
    ? AGENT_NAMES[currentAgent] || currentAgent
    : "Processing";

  return (
    <Card
      className={cn(
        "mb-4 border-primary/20 bg-primary/5",
        agentStatus === "running" && "animate-pulse"
      )}
    >
      <CardContent className="p-4">
        {/* Current Agent Status */}
        <div className="flex items-center gap-3 mb-3">
          {agentStatus === "running" && (
            <Loader2 className="w-5 h-5 text-primary animate-spin" />
          )}
          {agentStatus === "completed" && (
            <CheckCircle2 className="w-5 h-5 text-green-500" />
          )}
          {agentStatus === "error" && (
            <XCircle className="w-5 h-5 text-destructive" />
          )}

          <div className="flex-1">
            <div className="font-medium text-sm">{agentName}</div>
            {agentMessage && (
              <div className="text-xs text-muted-foreground">
                {agentMessage}
              </div>
            )}
            {agentError && (
              <div className="text-xs text-destructive">{agentError}</div>
            )}
          </div>
        </div>

        {agentStatus === "running" && (
          <div className="mb-3 text-xs text-muted-foreground">
            Working on it<span className="animate-pulse">...</span>
          </div>
        )}

        {/* Agent Execution History */}
        {agentHistory.length > 0 && (
          <div className="flex items-center gap-2 flex-wrap">
            {agentHistory.map((update, idx) => {
              const AgentIcon =
                AGENT_ICONS[update.current_agent] || Loader2;
              const name =
                AGENT_NAMES[update.current_agent] || update.current_agent;

              return (
                <div
                  key={idx}
                  className={cn(
                    "flex items-center gap-1 px-2 py-1 rounded text-xs",
                    update.status === "completed" &&
                      "bg-green-500/10 text-green-700 dark:text-green-400",
                    update.status === "running" &&
                      "bg-primary/10 text-primary",
                    update.status === "error" &&
                      "bg-destructive/10 text-destructive"
                  )}
                  title={update.message || name}
                >
                  {update.status === "completed" && (
                    <CheckCircle2 size={12} />
                  )}
                  {update.status === "running" && <Loader2 size={12} />}
                  {update.status === "error" && <XCircle size={12} />}
                  <AgentIcon size={12} />
                  <span className="hidden sm:inline">{name}</span>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
