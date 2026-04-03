# SPDX-License-Identifier: Apache-2.0
ARG BUILD_IMAGE=node:22-alpine
ARG RUN_IMAGE=node:22-alpine

FROM ${BUILD_IMAGE} AS builder
LABEL stage=build
# TS -> JS stage

WORKDIR /app
COPY ./package*.json ./
COPY ./tsconfig.json ./
COPY ./src ./src
COPY .npmrc ./
ARG GH_TOKEN

RUN npm i
RUN npm run build

FROM ${BUILD_IMAGE} AS dep-resolver
LABEL stage=pre-prod
# To filter out dev dependencies from final build

WORKDIR /app
COPY package*.json ./
COPY .npmrc ./
ARG GH_TOKEN
RUN npm i

FROM ${RUN_IMAGE} AS run-env

WORKDIR /app
COPY --from=dep-resolver /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY package.json ./

# Execute watchdog command
CMD ["node", "dist/job.js"]