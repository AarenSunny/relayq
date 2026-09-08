FROM node:24-alpine

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts \
    && mkdir -p /data \
    && chown node:node /data
COPY --chown=node:node src ./src

ENV NODE_ENV=production \
    PORT=8080 \
    RELAYQ_DB=/data/relayq.db
VOLUME ["/data"]
EXPOSE 8080
USER node

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["node", "-e", "fetch('http://127.0.0.1:8080/health').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"]

CMD ["node", "src/server.ts"]
