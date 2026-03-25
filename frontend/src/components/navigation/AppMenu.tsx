"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Menu, X } from "lucide-react";

import { Button } from "@/components/ui/button";

const NAV_ITEMS = [
  { href: "/", label: "Chat" },
  { href: "/databases", label: "Databases" },
  { href: "/settings", label: "Settings" },
  { href: "/runs", label: "Runs" },
  { href: "/quality", label: "Quality" },
  { href: "/monitoring", label: "Monitoring" },
];

export function AppMenu({ currentPath }: { currentPath?: string }) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const pathname = usePathname();
  const activePath = currentPath || pathname || "";

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, []);

  return (
    <div ref={containerRef} className="fixed right-4 top-4 z-[70]">
      <Button
        type="button"
        variant="outline"
        size="icon"
        aria-label={open ? "Close navigation menu" : "Open navigation menu"}
        onClick={() => setOpen((value) => !value)}
        className="h-11 w-11 rounded-full border-border/80 bg-background/95 shadow-lg backdrop-blur"
      >
        {open ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
      </Button>

      {open ? (
        <div className="absolute right-0 mt-3 w-64 overflow-hidden rounded-2xl border border-border/70 bg-background/95 p-2 shadow-2xl backdrop-blur">
          <div className="px-3 pb-2 pt-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
            Navigate
          </div>
          <div className="space-y-1">
            {NAV_ITEMS.map((item) => {
              const active = activePath === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setOpen(false)}
                  className={`flex items-center justify-between rounded-xl px-3 py-2 text-sm transition ${
                    active
                      ? "bg-primary text-primary-foreground"
                      : "text-foreground hover:bg-muted"
                  }`}
                >
                  <span>{item.label}</span>
                  {active ? (
                    <span className="text-[10px] uppercase tracking-[0.16em] opacity-80">
                      current
                    </span>
                  ) : null}
                </Link>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}
