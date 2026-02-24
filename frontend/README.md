# DataChat Frontend

Next.js-based web interface for DataChat - AI-powered natural language data assistant.

## Tech Stack

- **Next.js 15** - React framework with App Router
- **TypeScript** - Type safety
- **Tailwind CSS** - Utility-first styling
- **shadcn/ui** - Component library
- **Zustand** - State management
- **React Query** - Server state (optional, for caching)
- **Lucide React** - Icons
- **WebSockets** - Real-time agent updates

## Features

- **Chat Interface** - Conversational UI for asking questions
- **Real-time Agent Status** - Live updates during query processing
- **SQL Display** - View generated SQL queries
- **Data Tables** - Display query results in tables
- **Source Citations** - Show DataPoints used for context
- **Performance Metrics** - Display latency, LLM calls, retries
- **Responsive Design** - Works on desktop and mobile
- **Dark Mode Support** - Toggle between light and dark themes

## Getting Started

### Prerequisites

- Node.js 18+ or 20+
- npm or yarn
- DataChat backend running on http://localhost:8000

### Installation

```bash
# Install dependencies
npm install

# Create environment file
cp .env.example .env.local

# Edit .env.local with your backend URL (if different from default)
# NEXT_PUBLIC_API_URL=http://localhost:8000
# NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

### Development

```bash
# Start development server
npm run dev

# Open http://localhost:3000 in your browser
```

### Production Build

```bash
# Build for production
npm run build

# Start production server
npm start
```

### Linting

```bash
# Run ESLint
npm run lint
```

## Project Structure

```
frontend/
├── src/
│   ├── app/                    # Next.js App Router
│   │   ├── globals.css        # Global styles
│   │   ├── layout.tsx         # Root layout
│   │   └── page.tsx           # Home page (chat interface)
│   ├── components/
│   │   ├── ui/                # shadcn/ui components
│   │   │   ├── button.tsx
│   │   │   ├── input.tsx
│   │   │   └── card.tsx
│   │   ├── chat/              # Chat-specific components
│   │   │   ├── ChatInterface.tsx  # Main chat UI
│   │   │   └── Message.tsx        # Individual message
│   │   └── agents/            # Agent-related components
│   │       └── AgentStatus.tsx    # Agent pipeline status
│   └── lib/
│       ├── api.ts             # API client + WebSocket
│       ├── utils.ts           # Utility functions
│       └── stores/
│           └── chat.ts        # Zustand chat store
├── public/                     # Static assets
├── package.json
├── tsconfig.json
├── tailwind.config.ts
└── next.config.ts
```

## API Integration

The frontend communicates with the backend through:

1. **REST API** (`/api/v1/chat`) - Send queries and receive responses
2. **WebSocket** (`/ws`) - Real-time agent status updates during processing

### API Client Usage

```typescript
import { apiClient } from "@/lib/api";

// Send a chat message
const response = await apiClient.chat({
  message: "What is the total revenue?",
  conversation_id: "conv_123",
});

// Check API health
const health = await apiClient.health();
```

### WebSocket Usage

```typescript
import { wsClient } from "@/lib/api";

// Connect and listen for agent updates
wsClient.connect(
  (update) => {
    console.log("Agent update:", update);
    // { current_agent: "SQLAgent", status: "running", message: "..." }
  },
  (error) => console.error("WebSocket error:", error),
  () => console.log("WebSocket closed")
);

// Disconnect when done
wsClient.disconnect();
```

## State Management

The app uses Zustand for state management with the following stores:

### Chat Store

Located in `src/lib/stores/chat.ts`, manages:

- Messages (user and assistant)
- Conversation ID
- Agent status (current agent, status, messages, errors)
- Loading states
- WebSocket connection status

Example usage:

```typescript
import { useChatStore } from "@/lib/stores/chat";

function MyComponent() {
  const { messages, addMessage, setLoading } = useChatStore();

  // Use store state and actions
}
```

## Styling

The app uses Tailwind CSS with a custom theme defined in `tailwind.config.ts`.

- **Light/Dark Mode** - Supports both themes via CSS variables
- **shadcn/ui** - Pre-built accessible components
- **Custom Colors** - Defined in `globals.css` using HSL

## Environment Variables

Create a `.env.local` file:

```env
# Backend API URL
NEXT_PUBLIC_API_URL=http://localhost:8000

# WebSocket URL
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

## Deployment

### Vercel (Recommended)

1. Push code to GitHub
2. Import project to Vercel
3. Set environment variables
4. Deploy

### Docker

```bash
# Build image
docker build -t datachat-frontend .

# Run container
docker run -p 3000:3000 \
  -e NEXT_PUBLIC_API_URL=http://backend:8000 \
  datachat-frontend
```

### Self-Hosted

```bash
# Build
npm run build

# Start with PM2
pm2 start npm --name "datachat-frontend" -- start
```

## Development Tips

1. **Hot Reload** - Changes to components are instantly reflected
2. **Type Safety** - Use TypeScript for all new code
3. **Component Library** - Use shadcn/ui components when possible
4. **State Management** - Keep state in Zustand stores, not component state
5. **API Calls** - Use the `apiClient` singleton, don't create new instances

## Troubleshooting

### Build Errors

```bash
# Clear cache and rebuild
rm -rf .next node_modules
npm install
npm run build
```

### WebSocket Connection Issues

- Check backend is running on the correct port
- Verify `NEXT_PUBLIC_WS_URL` matches backend WebSocket endpoint
- Check browser console for WebSocket errors

### Styling Issues

```bash
# Rebuild Tailwind
npm run dev
```

## Contributing

1. Create a feature branch
2. Make changes
3. Run linter: `npm run lint`
4. Test build: `npm run build`
5. Submit pull request

## License

Apache 2.0
