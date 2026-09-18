# Multi-stage build: compile the Vite/React/TypeScript dashboard UI, then
# serve the static build with nginx (which also reverse-proxies /api and /ws
# to the existing FastAPI dashboard backend -- see nginx.conf).

FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine AS serve
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
