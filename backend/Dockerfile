# syntax=docker/dockerfile:1.7
# One image, three roles. FLOWGRID_ROLE=api|scheduler|worker selects what the process runs.
# Node 22 runs the TypeScript sources directly (type stripping), so there is no build step and no source maps to ship.
FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force
COPY src ./src
COPY migrations ./migrations
USER node
EXPOSE 8080 9100
CMD ["node", "src/main.ts"]
