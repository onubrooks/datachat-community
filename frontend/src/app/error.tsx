"use client";

import Link from "next/link";
import { useEffect } from "react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Unhandled UI error:", error);
  }, [error]);

  return (
    <main className="flex min-h-screen items-center justify-center bg-background px-6 py-10">
      <div className="w-full max-w-xl rounded-lg border border-border bg-card p-6 shadow-sm">
        <h1 className="text-xl font-semibold">Something went wrong</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          An unexpected UI error occurred. You can retry this screen or return to chat.
        </p>
        <div className="mt-4 rounded border border-dashed border-border/80 bg-muted/20 p-3 text-xs text-muted-foreground">
          {error.message || "Unexpected error"}
        </div>
        <div className="mt-5 flex gap-2">
          <button
            type="button"
            onClick={reset}
            className="rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground"
          >
            Retry
          </button>
          <Link
            href="/"
            className="rounded-md border border-border px-3 py-2 text-sm hover:bg-secondary"
          >
            Back to Chat
          </Link>
        </div>
      </div>
    </main>
  );
}

