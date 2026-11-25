import { LoggerService } from '@tazama-lf/frms-coe-lib';
import { validateProcessorConfig } from '@tazama-lf/frms-coe-lib/lib/config';
import axios from 'axios';
import cron from 'node-cron';
import { additionalEnvironmentVariables, type Configuration } from './config';
import { CouchDBService } from './services/CouchDBService';
import { TikaService } from './services/TikaService';
import { SolrService } from './services/SolrService';
import { NiFiService } from './services/NiFiService';
import { DecryptionService } from './services/DecryptionService';
import type { IEvidenceDocument } from './interfaces/iEvidenceDocument';

const BYTES_PER_KB = 1024;
const BYTES_PER_MB = BYTES_PER_KB * BYTES_PER_KB;

// A document that fails this many times for a reason of its own is marked
// FAILED and is no longer selected for processing.
const MAX_DOCUMENT_ATTEMPTS = 3;

// While a dependency is unavailable, runs are paused for 30 s, doubling per
// consecutive outage up to 10 min.
const OUTAGE_BACKOFF_BASE_MS = 30_000;
const OUTAGE_BACKOFF_MAX_MS = 600_000;
const OUTAGE_BACKOFF_FACTOR = 2;
const MS_PER_SECOND = 1000;

const HTTP_UNAUTHORIZED = 401;
const HTTP_FORBIDDEN = 403;
const HTTP_REQUEST_TIMEOUT = 408;
const HTTP_TOO_MANY_REQUESTS = 429;
const HTTP_BAD_GATEWAY = 502;
const HTTP_SERVICE_UNAVAILABLE = 503;
const HTTP_GATEWAY_TIMEOUT = 504;

// Responses that mean the service, not the document, is the problem.
// 500 is deliberately absent: Tika and Solr return it for individual
// documents they cannot process.
const SERVICE_UNAVAILABLE_STATUSES = new Set([
  HTTP_UNAUTHORIZED,
  HTTP_FORBIDDEN,
  HTTP_REQUEST_TIMEOUT,
  HTTP_TOO_MANY_REQUESTS,
  HTTP_BAD_GATEWAY,
  HTTP_SERVICE_UNAVAILABLE,
  HTTP_GATEWAY_TIMEOUT,
]);

// Network errors that mean the service could not be reached at all.
// Response timeouts (ECONNABORTED) and resets (ECONNRESET) are deliberately
// absent: a single oversized or malformed document can cause them.
const SERVICE_UNREACHABLE_CODES = new Set([
  'ECONNREFUSED',
  'ENOTFOUND',
  'EAI_AGAIN',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENETUNREACH',
]);

function isServiceUnavailable(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  if (error.response !== undefined) {
    return SERVICE_UNAVAILABLE_STATUSES.has(error.response.status);
  }
  return error.code !== undefined && SERVICE_UNREACHABLE_CODES.has(error.code);
}

async function tryCatch<T>(
  fn: () => Promise<T>,
  onError: (error: Error) => Promise<void> | void,
  rethrow = false
): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    await onError(err);
    if (rethrow) throw error;
    return undefined;
  }
}

class DocumentProcessor {
  private readonly logger: LoggerService;
  private readonly config: Configuration;
  private readonly couchdb: CouchDBService;
  private readonly tika: TikaService;
  private readonly solr: SolrService;
  private readonly nifi: NiFiService;
  private readonly decryption: DecryptionService;
  private consecutiveOutages = 0;
  private pausedUntil = 0;

  constructor() {
    this.config = validateProcessorConfig(
      additionalEnvironmentVariables
    ) as Configuration;
    this.logger = new LoggerService(this.config);
    this.couchdb = CouchDBService.getInstance(this.config);
    this.tika = TikaService.getInstance(this.config);
    this.solr = SolrService.getInstance(this.config);
    this.nifi = NiFiService.getInstance(this.config);
    this.decryption = DecryptionService.getInstance();
  }

  async run(): Promise<void> {
    if (Date.now() < this.pausedUntil) return;

    await tryCatch(
      async () => {
        const documents =
          await this.couchdb.findUnprocessedDocs<IEvidenceDocument>();
        for (const doc of documents) {
          // Throws only when a dependency is unavailable, which ends the run:
          // every remaining document would fail the same way.
          await this.processDocument(doc);
        }
        this.consecutiveOutages = 0;
      },
      (error) => {
        if (isServiceUnavailable(error)) {
          this.pauseAfterOutage(error);
          return;
        }
        this.logger.error('Job failed: ' + error.message, error, 'run');
      }
    );
  }

  private pauseAfterOutage(error: Error): void {
    this.consecutiveOutages += 1;
    const delayMs = Math.min(
      OUTAGE_BACKOFF_BASE_MS *
        OUTAGE_BACKOFF_FACTOR ** (this.consecutiveOutages - 1),
      OUTAGE_BACKOFF_MAX_MS
    );
    this.pausedUntil = Date.now() + delayMs;
    this.logger.warn(
      `Service unavailable (${error.message}); pausing processing for ` +
        `${delayMs / MS_PER_SECOND} s`,
      'run'
    );
  }

  private async processDocument(doc: IEvidenceDocument): Promise<void> {
    const docId = doc._id;
    const { evidenceId } = doc;
    const { taskId } = doc;

    await tryCatch(
      async () => {
        await this.couchdb.updateStatus(docId, 'PROCESSING');

        const attachmentNames = doc._attachments
          ? Object.keys(doc._attachments)
          : [];
        if (attachmentNames.length === 0) {
          throw new Error('No attachments found');
        }
        const [attachmentName] = attachmentNames;

        if (!doc.metadata || doc.metadata.length === 0) {
          throw new Error('Document metadata missing');
        }
        const [fileMeta] = doc.metadata;

        if (fileMeta.fileSize > this.config.MAX_FILE_SIZE_MB * BYTES_PER_MB) {
          throw new Error(
            'File too large: ' +
              Math.round(fileMeta.fileSize / BYTES_PER_MB) +
              'MB'
          );
        }

        let fileBuffer = await this.couchdb.getAttachment(
          docId,
          attachmentName
        );
        if (fileMeta.encryption) {
          fileBuffer = this.decryption.decrypt(fileBuffer, fileMeta.encryption);
        }

        const extraction = await this.tika.extract(fileBuffer);
        this.logger.log(`Content length: ${extraction.text.length}`);

        const contentForSolr =
          extraction.text.length > this.config.MAX_SOLR_CONTENT
            ? extraction.text.substring(0, this.config.MAX_SOLR_CONTENT)
            : extraction.text;

        await tryCatch(
          async () => {
            await this.solr.indexDocument({
              id: docId,
              evidenceId,
              taskId,
              evidenceType: doc.evidenceType,
              fileName: fileMeta.fileName,
              content: contentForSolr,
              contentType: fileMeta.mimeType,
              uploadedAt: doc.uploadedAt,
              extractedAt: new Date().toISOString(),
              textLength: extraction.text.length,
              processingStatus: 'INDEXED',
            });
          },
          (solrErr) => {
            this.logger.error(
              'Solr indexing failed for ' + docId + ': ' + solrErr.message,
              solrErr,
              'process'
            );
          },
          true
        );
        this.logger.log('Sent to solr');

        await tryCatch(
          async () => {
            this.logger.log('sending to nifi');
            await this.nifi.sendDocument({
              documentId: docId,
              evidenceId,
              taskId,
              filename: fileMeta.fileName,
              content: extraction.text,
              metadata: extraction.metadata,
              extractedAt: new Date().toISOString(),
            });
          },
          (error) => {
            this.logger.log(
              'NIFI send failed for ' + docId + JSON.stringify(error.message)
            );
            this.couchdb.updateStatus(docId, 'ERROR', error.message);
          }
        );

        await tryCatch(
          async () => {
            await this.couchdb.updateStatus(docId, 'COMPLETED', undefined, 0);
          },
          (finalizeErr) => {
            this.logger.error(
              'Failed to mark COMPLETED for ' + docId,
              finalizeErr,
              'process'
            );
          },
          true
        );
      },
      async (error) => {
        this.logger.error(
          'Failed ' + docId + ': ' + error.message,
          error,
          'process'
        );

        // A dependency outage is not the document's fault: keep it eligible
        // without counting the attempt, then end the run.
        const serviceUnavailable = isServiceUnavailable(error);
        const attempts = serviceUnavailable
          ? undefined
          : (doc.retryCount ?? 0) + 1;
        const status =
          attempts !== undefined && attempts >= MAX_DOCUMENT_ATTEMPTS
            ? 'FAILED'
            : 'ERROR';

        await tryCatch(
          async () => {
            await this.couchdb.updateStatus(
              docId,
              status,
              error.message,
              attempts
            );
          },
          (updateErr) => {
            this.logger.error(
              'Failed to mark ' + status + ' status for ' + docId,
              updateErr,
              'process'
            );
            // CouchDB went down while saving the failure: end the run so it
            // backs off, even if the original error was the document's own.
            if (isServiceUnavailable(updateErr)) throw updateErr;
          }
        );

        if (serviceUnavailable) throw error;
      }
    );
  }
}

const startProcessor = (): void => {
  const processor = new DocumentProcessor();
  const cfg = (processor as unknown as { config: Configuration }).config;
  const cronEnabled = cfg.CRON_ENABLED;
  const cronSchedule = cfg.CRON_SCHEDULE;

  let running = false;

  const safeRun = async (): Promise<void> => {
    if (running) return;
    running = true;

    try {
      await processor.run();
    } finally {
      running = false;
    }
  };

  if (cronEnabled) {
    // eslint-disable-next-line no-console -- Startup log before logger is available externally
    console.log(`Starting cron with schedule: ${cronSchedule}`);
    cron.schedule(cronSchedule, () => {
      void safeRun();
    });
  } else {
    void safeRun();
  }
};

if (process.env.NODE_ENV !== 'test') {
  startProcessor();
}

export { DocumentProcessor, startProcessor };
