import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  experimental: {
    // Work around a Next.js dev bundler issue resolving next-devtools SegmentViewNode.
    devtoolSegmentExplorer: false,
  },
};

export default nextConfig;
