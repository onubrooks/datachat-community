/**
 * System Setup Component
 *
 * Guides users through initialization steps when DataChat isn't ready.
 */

"use client";

import React, { useState } from "react";
import { AlertCircle, Database, ListChecks } from "lucide-react";
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
            <h2 className="text-sm font-semibold">Finish DataChat setup</h2>
            <p className="text-xs text-muted-foreground">
              DataChat needs a target database connection before it can answer queries.
              DataPoints are optional but recommended for higher-quality, business-aware
              responses. The system database is optional unless you want registry/profiling
              or demo data.
            </p>
            <p className="text-xs text-muted-foreground mt-2">
              Need a quick start? Run the demo dataset with <strong>datachat demo</strong>.
            </p>
          </div>
        </div>

        <div className="space-y-2">
          <div className="text-xs font-medium text-muted-foreground">
            Required steps
          </div>
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

        <form onSubmit={handleSubmit} className="space-y-2">
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
          {error && (
            <div className="text-xs text-destructive">{error}</div>
          )}
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
            {isSubmitting ? "Initializing..." : "Initialize"}
          </Button>
        </form>
      </div>
    </Card>
  );
}
