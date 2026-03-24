/**
 * System Setup Component
 *
 * Guides users through initialization steps when DataChat isn't ready.
 */

"use client";

import React, { useState } from "react";
import Link from "next/link";
import { AlertCircle, Database, ListChecks, WandSparkles } from "lucide-react";
import { Card } from "../ui/card";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import type { SetupStep } from "@/lib/api";

interface SystemSetupProps {
  steps: SetupStep[];
  onInitialize: (
    databaseUrl: string,
    autoProfile: boolean,
    systemDatabaseUrl?: string
  ) => Promise<void>;
  isSubmitting: boolean;
  error: string | null;
  notice?: string | null;
}

export function SystemSetup({
  steps,
  onInitialize,
  isSubmitting,
  error,
  notice,
}: SystemSetupProps) {
  const [databaseUrl, setDatabaseUrl] = useState("");
  const [systemDatabaseUrl, setSystemDatabaseUrl] = useState("");
  const [autoProfile, setAutoProfile] = useState(false);
  const needsSystemDatabase = steps.some((step) => step.step === "system_database");

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!databaseUrl.trim()) {
      return;
    }
    if (needsSystemDatabase && !systemDatabaseUrl.trim()) {
      return;
    }
    await onInitialize(
      databaseUrl.trim(),
      autoProfile,
      systemDatabaseUrl.trim() || undefined
    );
  };

  return (
    <Card className="border-primary/20 bg-primary/5 mb-4">
      <div className="p-4 space-y-4">
        <div className="flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-primary mt-0.5" />
          <div>
            <h2 className="text-sm font-semibold">Connect a database to start</h2>
            <p className="text-xs text-muted-foreground">
              Use the onboarding wizard for the fastest path. It guides connection,
              profiling, metadata generation, approval, and retrieval sync in one flow.
            </p>
          </div>
        </div>

        <div className="rounded-md border border-border bg-background p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-xs text-muted-foreground">
              Primary path: onboarding wizard
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button asChild size="sm">
                <Link href="/databases?tab=quickstart&wizard=1">
                  <WandSparkles className="mr-1 h-3.5 w-3.5" />
                  Start Onboarding Wizard
                </Link>
              </Button>
            </div>
          </div>
          <div className="mt-3 space-y-2">
            <div className="text-xs font-medium text-muted-foreground">Wizard steps</div>
            <ul className="space-y-2">
            {steps.map((step) => (
              <li key={step.step} className="flex items-start gap-2 text-sm">
                <ListChecks className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="font-medium">{step.title}</div>
                  <div className="text-xs text-muted-foreground">
                    {step.description}
                  </div>
                </div>
              </li>
            ))}
            </ul>
          </div>
          <p className="mt-3 text-xs text-muted-foreground">
            Need sample data for quick testing? Run <strong>datachat demo --persona base --reset</strong>.
          </p>
        </div>

        <details className="rounded-md border border-border bg-muted/20 p-3">
          <summary className="cursor-pointer text-xs font-medium text-muted-foreground">
            Advanced manual setup
          </summary>
          <form onSubmit={handleSubmit} className="mt-3 space-y-2">
            <label className="text-xs font-medium text-muted-foreground flex items-center gap-2">
              <Database className="w-4 h-4" />
              Target Database URL
            </label>
            <Input
              value={databaseUrl}
              onChange={(event) => setDatabaseUrl(event.target.value)}
              placeholder="postgresql://user:pass@host:5432/database"
              disabled={isSubmitting}
            />
            {needsSystemDatabase && (
              <>
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-2">
                  <Database className="w-4 h-4" />
                  System Database URL (for demo/registry)
                </label>
                <Input
                  value={systemDatabaseUrl}
                  onChange={(event) => setSystemDatabaseUrl(event.target.value)}
                  placeholder="postgresql://user:pass@host:5432/datachat"
                  disabled={isSubmitting}
                />
              </>
            )}
            <div className="text-xs text-muted-foreground">
              These settings are saved for future sessions.
            </div>
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              <input
                type="checkbox"
                checked={autoProfile}
                onChange={(event) => setAutoProfile(event.target.checked)}
                disabled={isSubmitting}
              />
              Auto-profile the database (generate DataPoints draft)
            </label>
            {error && <div className="text-xs text-destructive">{error}</div>}
            {notice && !error && (
              <div className="text-xs text-muted-foreground">{notice}</div>
            )}
            <Button
              type="submit"
              disabled={
                isSubmitting ||
                !databaseUrl.trim() ||
                (needsSystemDatabase && !systemDatabaseUrl.trim())
              }
            >
              {isSubmitting ? "Initializing..." : "Initialize manually"}
            </Button>
          </form>
        </details>
      </div>
    </Card>
  );
}
