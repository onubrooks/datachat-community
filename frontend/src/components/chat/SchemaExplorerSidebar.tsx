"use client";

import { useEffect, useState } from "react";

import {
  ChevronDown,
  ChevronRight,
  Database,
  FileText,
  Loader2,
  PanelRightClose,
  PanelRightOpen,
  Table2,
  X,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { DatabaseSchemaTable } from "@/lib/api";

interface MetadataExplorerItem {
  id: string;
  name: string;
  type: string;
  status: "pending" | "approved" | "managed";
  connectionId?: string | null;
  scope?: string | null;
  description?: string | null;
  businessPurpose?: string | null;
  sqlTemplate?: string | null;
  tableName?: string | null;
  relatedTables?: string[];
  confidence?: number | null;
  reviewNote?: string | null;
  sourceTier?: string | null;
  sourcePath?: string | null;
  payload?: Record<string, unknown> | null;
}

interface SchemaExplorerSidebarProps {
  isOpen: boolean;
  explorerMode: "schema" | "metadata";
  schemaSearch: string;
  metadataSearch: string;
  schemaLoading: boolean;
  metadataLoading: boolean;
  schemaError: string | null;
  metadataError: string | null;
  filteredSchemaTables: DatabaseSchemaTable[];
  pendingMetadataItems: MetadataExplorerItem[];
  approvedMetadataItems: MetadataExplorerItem[];
  managedMetadataItems: MetadataExplorerItem[];
  selectedMetadataKey: string | null;
  metadataDetail: Record<string, unknown> | null;
  metadataDetailLoading: boolean;
  metadataDetailError: string | null;
  includeExampleMetadata: boolean;
  metadataContextNote?: string | null;
  selectedSchemaTable: string | null;
  onToggle: () => void;
  onExplorerModeChange: (mode: "schema" | "metadata") => void;
  onSearchChange: (value: string) => void;
  onMetadataSearchChange: (value: string) => void;
  onIncludeExampleMetadataChange: (value: boolean) => void;
  onSelectMetadataItem: (item: MetadataExplorerItem) => void;
  onSelectTable: (fullName: string) => void;
  onUseTable: (fullName: string) => void;
}

export function SchemaExplorerSidebar({
  isOpen,
  explorerMode,
  schemaSearch,
  metadataSearch,
  schemaLoading,
  metadataLoading,
  schemaError,
  metadataError,
  filteredSchemaTables,
  pendingMetadataItems,
  approvedMetadataItems,
  managedMetadataItems,
  selectedMetadataKey,
  metadataDetail,
  metadataDetailLoading,
  metadataDetailError,
  includeExampleMetadata,
  metadataContextNote,
  selectedSchemaTable,
  onToggle,
  onExplorerModeChange,
  onSearchChange,
  onMetadataSearchChange,
  onIncludeExampleMetadataChange,
  onSelectMetadataItem,
  onSelectTable,
  onUseTable,
}: SchemaExplorerSidebarProps) {
  const [metadataDetailOpen, setMetadataDetailOpen] = useState(false);

  useEffect(() => {
    if (!isOpen || explorerMode !== "metadata") {
      setMetadataDetailOpen(false);
    }
  }, [explorerMode, isOpen]);

  const statusBadgeClass = (status: MetadataExplorerItem["status"]) => {
    if (status === "pending") {
      return "bg-amber-100 text-amber-900";
    }
    if (status === "approved") {
      return "bg-emerald-100 text-emerald-900";
    }
    return "bg-blue-100 text-blue-900";
  };

  const handleOpenMetadataDetails = (item: MetadataExplorerItem) => {
    void Promise.resolve(onSelectMetadataItem(item)).finally(() =>
      setMetadataDetailOpen(true)
    );
  };

  const renderMetadataSection = (
    title: string,
    items: MetadataExplorerItem[],
    emptyText: string
  ) => (
    <section className="rounded border border-border bg-background p-2">
      <div className="mb-2 text-[11px] font-semibold text-foreground">
        {title} ({items.length})
      </div>
      {items.length === 0 ? (
        <p className="text-[11px] text-muted-foreground">{emptyText}</p>
      ) : (
        <ul className="space-y-2">
          {items.map((item, index) => (
            <li key={`${item.id}-${item.status}-${index}`}>
              {(() => {
                const itemKey = `${item.status}:${item.id}`;
                const isSelected = selectedMetadataKey === itemKey;
                return (
                  <div
                    className={`w-full rounded border p-2 text-left transition ${
                      isSelected
                        ? "border-primary/40 bg-primary/10"
                        : "border-border/70 bg-muted/20 hover:bg-muted/40"
                    }`}
                  >
                    <button
                      type="button"
                      onClick={() => onSelectMetadataItem(item)}
                      className="w-full text-left"
                      aria-label={`Inspect metadata item ${item.name}`}
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <div className="truncate text-[11px] font-medium text-foreground">
                            {item.name}
                          </div>
                          <div className="truncate text-[10px] text-muted-foreground">
                            {item.type} · {item.id}
                          </div>
                        </div>
                        <span
                          className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${statusBadgeClass(
                            item.status
                          )}`}
                        >
                          {item.status}
                        </span>
                      </div>
                      <div className="mt-1 space-y-0.5 text-[10px] text-muted-foreground">
                        {item.description && (
                          <div className="truncate" title={item.description}>
                            Description: {item.description}
                          </div>
                        )}
                        {item.tableName && <div>Table: {item.tableName}</div>}
                        {item.scope && <div>Scope: {item.scope}</div>}
                        {item.connectionId && <div>Connection: {item.connectionId}</div>}
                        {typeof item.confidence === "number" && (
                          <div>Confidence: {item.confidence.toFixed(2)}</div>
                        )}
                        {item.sourceTier && <div>Source tier: {item.sourceTier}</div>}
                        {item.sourcePath && (
                          <div className="truncate" title={item.sourcePath}>
                            Source path: {item.sourcePath}
                          </div>
                        )}
                        {item.reviewNote && (
                          <div className="truncate" title={item.reviewNote}>
                            Review: {item.reviewNote}
                          </div>
                        )}
                      </div>
                    </button>
                    <div className="mt-2 flex justify-end">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-6 px-2 text-[10px]"
                        onClick={() => handleOpenMetadataDetails(item)}
                      >
                        Show details
                      </Button>
                    </div>
                  </div>
                );
              })()}
            </li>
          ))}
        </ul>
      )}
    </section>
  );

  return (
    <aside
      className={`hidden border-l border-border/70 bg-muted/30 transition-all duration-200 xl:flex xl:flex-col ${
        isOpen ? "w-80" : "w-14"
      }`}
      role="complementary"
      aria-label="Schema explorer sidebar"
    >
      <div className="flex items-center justify-between border-b px-2 py-2">
        <Button
          variant="ghost"
          size="icon"
          onClick={onToggle}
          aria-label="Toggle schema sidebar"
        >
          {isOpen ? <PanelRightClose size={16} /> : <PanelRightOpen size={16} />}
        </Button>
        {isOpen && <div className="text-xs font-medium">Explorer</div>}
      </div>
      {isOpen ? (
        <div className="flex min-h-0 flex-1 flex-col">
          <div className="border-b px-2 py-2">
            <div className="grid grid-cols-2 gap-1 rounded-md bg-muted p-1">
              <button
                type="button"
                onClick={() => onExplorerModeChange("schema")}
                className={`rounded px-2 py-1 text-xs font-medium transition ${
                  explorerMode === "schema"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:bg-background/70"
                }`}
                aria-label="Show schema explorer"
              >
                Schema
              </button>
              <button
                type="button"
                onClick={() => onExplorerModeChange("metadata")}
                className={`rounded px-2 py-1 text-xs font-medium transition ${
                  explorerMode === "metadata"
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:bg-background/70"
                }`}
                aria-label="Show metadata explorer"
              >
                Metadata
              </button>
            </div>
          </div>
          {explorerMode === "schema" ? (
            <>
              <div className="border-b p-2">
                <Input
                  value={schemaSearch}
                  onChange={(event) => onSearchChange(event.target.value)}
                  placeholder="Search table or column..."
                  className="h-8 text-xs"
                  aria-label="Search schema tables and columns"
                />
              </div>
              <div className="min-h-0 flex-1 overflow-y-auto p-2">
                {schemaLoading && (
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    Loading schema...
                  </div>
                )}
                {!schemaLoading && schemaError && (
                  <div className="rounded border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
                    {schemaError}
                  </div>
                )}
                {!schemaLoading && !schemaError && filteredSchemaTables.length === 0 && (
                  <div className="rounded border border-dashed p-3 text-xs text-muted-foreground">
                    No tables matched your search.
                  </div>
                )}
                <div className="space-y-2">
                  {filteredSchemaTables.map((table) => {
                    const fullName = `${table.schema_name}.${table.table_name}`;
                    const isSelected = selectedSchemaTable === fullName;
                    return (
                      <details key={fullName} className="rounded border border-border bg-background">
                        <summary
                          className={`cursor-pointer list-none px-2 py-2 text-xs ${
                            isSelected ? "bg-primary/5" : ""
                          }`}
                          aria-label={`Toggle schema table ${fullName}`}
                          onClick={() => onSelectTable(fullName)}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <div className="min-w-0">
                              <div className="truncate font-medium">{fullName}</div>
                              <div className="text-[11px] text-muted-foreground">
                                {table.table_type}
                                {typeof table.row_count === "number"
                                  ? ` · ~${table.row_count} rows`
                                  : ""}
                              </div>
                            </div>
                            <div className="flex items-center gap-1 text-muted-foreground">
                              <ChevronDown size={12} />
                              <ChevronRight size={12} />
                            </div>
                          </div>
                        </summary>
                        <div className="border-t px-2 py-2">
                          <button
                            type="button"
                            className="mb-2 inline-flex items-center gap-1 rounded border border-border px-2 py-1 text-[11px] hover:bg-muted"
                            aria-label={`Use table ${fullName} in query`}
                            onClick={() => onUseTable(fullName)}
                          >
                            <Table2 size={12} />
                            Use In Query
                          </button>
                          <ul className="space-y-1 text-[11px]">
                            {table.columns.map((column) => (
                              <li key={`${fullName}.${column.name}`} className="flex flex-wrap gap-1">
                                <span className="font-medium">{column.name}</span>
                                <span className="text-muted-foreground">({column.data_type})</span>
                                {column.is_primary_key && (
                                  <span className="rounded bg-blue-100 px-1 text-[10px] text-blue-800">
                                    PK
                                  </span>
                                )}
                                {column.is_foreign_key && (
                                  <span className="rounded bg-amber-100 px-1 text-[10px] text-amber-900">
                                    FK
                                  </span>
                                )}
                              </li>
                            ))}
                          </ul>
                        </div>
                      </details>
                    );
                  })}
                </div>
              </div>
            </>
          ) : (
            <>
              <div className="border-b p-2">
                <Input
                  value={metadataSearch}
                  onChange={(event) => onMetadataSearchChange(event.target.value)}
                  placeholder="Search datapoint id, name, or type..."
                  className="h-8 text-xs"
                  aria-label="Search generated and managed metadata"
                />
                <label className="mt-2 flex items-center gap-2 text-[11px] text-muted-foreground">
                  <input
                    type="checkbox"
                    checked={includeExampleMetadata}
                    onChange={(event) => onIncludeExampleMetadataChange(event.target.checked)}
                  />
                  Include demo/example metadata
                </label>
              </div>
              <div className="min-h-0 flex-1 space-y-2 overflow-y-auto p-2">
                {metadataContextNote && (
                  <div className="rounded border border-amber-300 bg-amber-50 p-2 text-[11px] text-amber-900">
                    {metadataContextNote}
                  </div>
                )}
                {metadataLoading && (
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    Loading metadata...
                  </div>
                )}
                {!metadataLoading && metadataError && (
                  <div className="rounded border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
                    {metadataError}
                  </div>
                )}
                {!metadataLoading && !metadataError && (
                  <>
                    {renderMetadataSection(
                      "Generated (Pending)",
                      pendingMetadataItems,
                      "No pending generated DataPoints for this context."
                    )}
                    {renderMetadataSection(
                      "Generated (Approved)",
                      approvedMetadataItems,
                      "No approved generated DataPoints for this context."
                    )}
                    {renderMetadataSection(
                      "Managed (Active)",
                      managedMetadataItems,
                      "No active managed DataPoints matched your filters."
                    )}
                  </>
                )}
              </div>
            </>
          )}
        </div>
      ) : (
        <div className="flex flex-1 items-center justify-center text-muted-foreground">
          {explorerMode === "metadata" ? <FileText size={16} /> : <Database size={16} />}
        </div>
      )}
      {metadataDetailOpen && (
        <div
          className="fixed inset-0 z-[120] flex items-center justify-center bg-black/40 p-4"
          onClick={() => setMetadataDetailOpen(false)}
        >
          <div
            className="flex max-h-[88vh] w-full max-w-4xl flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="flex items-center justify-between border-b px-4 py-3">
              <div className="text-sm font-semibold text-foreground">Metadata Detail</div>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                onClick={() => setMetadataDetailOpen(false)}
                aria-label="Close metadata detail modal"
              >
                <X size={14} />
              </Button>
            </div>
            <div className="overflow-y-auto p-4 text-sm">
              {!selectedMetadataKey && (
                <p className="text-muted-foreground">
                  Select a metadata item and click Show details.
                </p>
              )}
              {selectedMetadataKey && metadataDetailLoading && (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading metadata detail...
                </div>
              )}
              {selectedMetadataKey && !metadataDetailLoading && metadataDetailError && (
                <div className="rounded border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                  {metadataDetailError}
                </div>
              )}
              {selectedMetadataKey &&
                !metadataDetailLoading &&
                !metadataDetailError &&
                metadataDetail && (
                  <div className="space-y-3 text-sm">
                    <div>
                      <span className="font-medium">Name:</span>{" "}
                      {typeof metadataDetail.name === "string" ? metadataDetail.name : "—"}
                    </div>
                    <div>
                      <span className="font-medium">Type:</span>{" "}
                      {typeof metadataDetail.type === "string" ? metadataDetail.type : "—"}
                    </div>
                    {typeof metadataDetail.description === "string" && (
                      <div>
                        <div className="font-medium">Description</div>
                        <div className="text-muted-foreground">{metadataDetail.description}</div>
                      </div>
                    )}
                    {typeof metadataDetail.business_purpose === "string" && (
                      <div>
                        <div className="font-medium">Business Purpose</div>
                        <div className="text-muted-foreground">
                          {metadataDetail.business_purpose}
                        </div>
                      </div>
                    )}
                    {typeof metadataDetail.sql_template === "string" && (
                      <div>
                        <div className="font-medium">SQL Template</div>
                        <pre className="mt-1 max-h-56 overflow-auto rounded border border-border/60 bg-muted/20 p-3 text-xs">
                          {metadataDetail.sql_template}
                        </pre>
                      </div>
                    )}
                    <details className="rounded border border-border/60 bg-muted/20 p-3">
                      <summary className="cursor-pointer text-sm font-medium">Raw JSON</summary>
                      <pre className="mt-2 max-h-[40vh] overflow-auto text-xs text-muted-foreground">
                        {JSON.stringify(metadataDetail, null, 2)}
                      </pre>
                    </details>
                  </div>
                )}
            </div>
          </div>
        </div>
      )}
    </aside>
  );
}
