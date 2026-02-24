# Multi-stage build for DataChat frontend
# Uses Node.js 20 for Next.js application

# =============================================================================
# Stage 1: Dependencies - Install node modules
# =============================================================================
FROM node:20-alpine AS deps

# Install dependencies only when needed
RUN apk add --no-cache libc6-compat

WORKDIR /app

# Copy package files
COPY package.json package-lock.json ./

# Install dependencies (include dev deps for build)
RUN npm ci

# =============================================================================
# Stage 2: Builder - Build Next.js application
# =============================================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy source code
COPY . .

# Build-time public env vars (baked into the client bundle)
ARG NEXT_PUBLIC_API_URL
ARG NEXT_PUBLIC_WS_URL

# Set environment variables for build
ENV NEXT_TELEMETRY_DISABLED=1 \
    NODE_ENV=production \
    NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL} \
    NEXT_PUBLIC_WS_URL=${NEXT_PUBLIC_WS_URL}

# Build Next.js application
RUN npm run build

# =============================================================================
# Stage 3: Runner - Run production server
# =============================================================================
FROM node:20-alpine AS runner

WORKDIR /app

# Set environment variables
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1

# Create non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy necessary files from builder
COPY --from=builder /app/public ./public
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static

# Switch to non-root user
USER nextjs

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3000', (r) => {process.exit(r.statusCode === 200 ? 0 : 1)})"

# Run application
CMD ["node", "server.js"]
