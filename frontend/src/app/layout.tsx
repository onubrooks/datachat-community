import type { Metadata } from "next";
import "./globals.css";
import { ReactQueryProvider } from "@/components/providers/ReactQueryProvider";

export const metadata: Metadata = {
  title: "DataChat - AI Data Assistant",
  description: "Natural language interface for your data warehouse",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script
          id="datachat-theme-init"
          // Execute as early as possible to avoid first-paint theme flash.
          dangerouslySetInnerHTML={{
            __html: `
              (function() {
                try {
                  var key = "datachat.themeMode";
                  var mode = window.localStorage.getItem(key) || "system";
                  var prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
                  var useDark = mode === "dark" || (mode === "system" && prefersDark);
                  if (useDark) {
                    document.documentElement.classList.add("dark");
                  } else {
                    document.documentElement.classList.remove("dark");
                  }
                } catch (_) {}
              })();
            `,
          }}
        />
      </head>
      <body className="font-sans">
        <ReactQueryProvider>{children}</ReactQueryProvider>
      </body>
    </html>
  );
}
