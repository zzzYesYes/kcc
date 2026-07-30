# ---- Stage 1: Build ----
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies first (cache layer)
COPY package.json package-lock.json ./
RUN npm ci

# Copy source and build
COPY index.html tsconfig.json tsconfig.node.json vite.config.ts \
     tailwind.config.js postcss.config.js ./
COPY src/ src/

# Build args for runtime API endpoints (baked into static bundle)
ARG VITE_API_BASE_URL=""
ARG VITE_IAM_BASE_URL=""

RUN npm run build

# ---- Stage 2: Serve ----
FROM nginx:1.27-alpine

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy built static files
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
