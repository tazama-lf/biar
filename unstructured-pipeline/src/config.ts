import type {
  AdditionalConfig,
  ProcessorConfig,
} from '@tazama-lf/frms-coe-lib/lib/config/processor.config';
import * as dotenv from 'dotenv';
import path from 'node:path';

dotenv.config({
  path: path.resolve(__dirname, '../.env'),
});

export interface ExtendedConfig {
  COUCHDB_URL: string;
  TIKA_URL: string;
  SOLR_URL: string;
  NIFI_URL: string;
  CRON_ENABLED: boolean;
  CRON_SCHEDULE: string;
  MAX_FILE_SIZE_MB: number;
  MAX_SOLR_CONTENT: number;
  TIKA_TIMEOUT: number;
  NIFI_TIMEOUT: number;
}

export const additionalEnvironmentVariables: AdditionalConfig[] = [
  { name: 'COUCHDB_URL', type: 'string' },
  { name: 'TIKA_URL', type: 'string' },
  { name: 'SOLR_URL', type: 'string' },
  { name: 'NIFI_URL', type: 'string' },
  { name: 'CRON_ENABLED', type: 'boolean' },
  { name: 'CRON_SCHEDULE', type: 'string' },
  { name: 'MAX_FILE_SIZE_MB', type: 'number' },
  { name: 'MAX_SOLR_CONTENT', type: 'number' },
  { name: 'TIKA_TIMEOUT', type: 'number' },
  { name: 'NIFI_TIMEOUT', type: 'number' },
];

export type Configuration = ProcessorConfig & ExtendedConfig;
