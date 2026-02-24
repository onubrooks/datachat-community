"use client";

import { ChevronDown, ChevronRight, Database, Loader2, PanelRightClose, PanelRightOpen, Table2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { DatabaseSchemaTable } from "@/lib/api";

interface SchemaExplorerSidebarProps {
  isOpen: boolean;
  schemaSearch: string;
  schemaLoading: boolean;
  schemaError: string | null;
  filteredSchemaTables: DatabaseSchemaTable[];
  selectedSchemaTable: string | null;
  onToggle: () => void;
  onSearchChange: (value: string) => void;
  onSelectTable: (fullName: string) => void;
  onUseTable: (fullName: string) => void;
}

export function SchemaExplorerSidebar({
  isOpen,
  schemaSearch,
  schemaLoading,
  schemaError,
  filteredSchemaTables,
  selectedSchemaTable,
  onToggle,
  onSearchChange,
  onSelectTable,
  onUseTable,
}: SchemaExplorerSidebarProps) {
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
        {isOpen && <div className="text-xs font-medium">Schema Explorer</div>}
      </div>
      {isOpen ? (
        <div className="flex min-h-0 flex-1 flex-col">
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
        </div>
      ) : (
        <div className="flex flex-1 items-center justify-center text-muted-foreground">
          <Database size={16} />
        </div>
      )}
    </aside>
  );
}
