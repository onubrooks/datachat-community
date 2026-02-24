/**
 * Main Chat Page
 *
 * Root page of the DataChat application.
 * Displays the chat interface in a full-height layout.
 */

import { Suspense } from "react";

import { ChatInterface } from "@/components/chat/ChatInterface";

export default function Home() {
  return (
    <main className="h-screen flex flex-col">
      <Suspense
        fallback={
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            Loading chat workspace...
          </div>
        }
      >
        <ChatInterface />
      </Suspense>
    </main>
  );
}
