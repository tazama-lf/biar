import { LoggerService } from '@tazama-lf/frms-coe-lib';
import { validateProcessorConfig } from '@tazama-lf/frms-coe-lib/lib/config';
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

class DocumentProcessor {
  private readonly logger: LoggerService;
  private readonly config: Configuration;
  private readonly couchdb: CouchDBService;
  private readonly tika: TikaService;
  private readonly solr: SolrService;
  private readonly nifi: NiFiService;
  private readonly decryption: DecryptionService;

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
    try {
      const documents =
        await this.couchdb.findUnprocessedDocs<IEvidenceDocument>();
      for (const doc of documents) {
        await this.processDocument(doc);
      }
    } catch (error) {
      this.logger.error(
        'Job failed: ' + (error as Error).message,
        error,
        'run'
      );
    }
  }

  private async processDocument(doc: IEvidenceDocument): Promise<void> {
    const docId = doc._id;
    const { evidenceId } = doc;
    const { taskId } = doc;

    try {
      await this.couchdb.updateStatus(docId, 'PROCESSING');

      const attachmentNames = Object.keys(doc._attachments!);
      if (attachmentNames.length === 0) {
        throw new Error('No attachments found');
      }

      const [attachmentName] = attachmentNames;
      const [fileMeta] = doc.metadata!;

      if (fileMeta.fileSize > this.config.MAX_FILE_SIZE_MB * BYTES_PER_MB) {
        throw new Error(
          'File too large: ' +
            Math.round(fileMeta.fileSize / BYTES_PER_MB) +
            'MB'
        );
      }

      let fileBuffer = await this.couchdb.getAttachment(docId, attachmentName);

      if (fileMeta.encryption) {
        fileBuffer = this.decryption.decrypt(fileBuffer, fileMeta.encryption);
      }

      const extraction = await this.tika.extract(fileBuffer);
      this.logger.log(`Content length: ${extraction.text.length}`);

      const contentForSolr =
        extraction.text.length > this.config.MAX_SOLR_CONTENT
          ? extraction.text.substring(0, this.config.MAX_SOLR_CONTENT)
          : extraction.text;

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

      this.logger.log('Sent to solr');

      await this.nifi.sendDocument({
        documentId: docId,
        evidenceId,
        taskId,
        filename: fileMeta.fileName,
        content: extraction.text,
        metadata: extraction.metadata,
        extractedAt: new Date().toISOString(),
      });

      await this.couchdb.updateStatus(docId, 'COMPLETED');
    } catch (error) {
      const errorMsg = (error as Error).message;
      this.logger.error('Failed ' + docId + ': ' + errorMsg, error, 'process');
      await this.couchdb.updateStatus(docId, 'ERROR', errorMsg);
    }
  }
}

const startProcessor = (): void => {
  const processor = new DocumentProcessor();
  const cronEnabled = process.env.CRON_ENABLED === 'true';
  const cronSchedule = process.env.CRON_SCHEDULE ?? '*/5 * * * * *';

  if (cronEnabled) {
    // eslint-disable-next-line no-console -- Startup log before logger is available externally
    console.log(`Starting cron with schedule: ${cronSchedule}`);
    cron.schedule(cronSchedule, () => {
      processor.run();
    });
  } else {
    processor.run();
  }
};

if (process.env.NODE_ENV !== 'test') {
  startProcessor();
}

export { DocumentProcessor, startProcessor };
