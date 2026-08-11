# SPDX-License-Identifier: Apache-2.0
ARG BUILD_IMAGE=node:20-bullseye
ARG RUN_IMAGE=gcr.io/distroless/nodejs20-debian11:nonroot

FROM ${BUILD_IMAGE} AS builder
LABEL stage=build

WORKDIR /app
COPY ./package*.json ./
COPY ./tsconfig.json ./
COPY ./src ./src
COPY .npmrc ./

RUN --mount=type=secret,id=GH_TOKEN,env=GH_TOKEN npm ci

RUN npm run build

FROM ${RUN_IMAGE} AS run-env

WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./

# Execute watchdog command
CMD ["dist/job.js"]