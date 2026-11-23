import { createCipheriv, randomBytes } from 'crypto';
import { AxiosError, type AxiosResponse } from 'axios';

const mockCouchDB = {
  getAllDocs: jest.fn(),
  findUnprocessedDocs: jest.fn(),
  getDocument: jest.fn(),
  getAttachment: jest.fn(),
  updateStatus: jest.fn(),
};

const mockTika = {
  extract: jest.fn(),
};

const mockSolr = {
  indexDocument: jest.fn(),
};

const mockNifi = {
  sendDocument: jest.fn(),
};

const mockDecryption = {
  decrypt: jest.fn(),
};

jest.mock('@tazama-lf/frms-coe-lib', () => {
  class MockLoggerService {
    log = jest.fn();
    warn = jest.fn();
    error = jest.fn();
  }
  return { LoggerService: MockLoggerService };
});

jest.mock('@tazama-lf/frms-coe-lib/lib/config', () => ({
  validateProcessorConfig: jest.fn((_additionalEnvVars) => ({
    COUCHDB_URL: 'http://localhost:5984/cms-evidence',
    TIKA_URL: 'http://localhost:9998',
    SOLR_URL: 'http://localhost:8983/solr/biar_docs',
    NIFI_URL: 'http://localhost:8081',
    CRON_ENABLED: process.env.CRON_ENABLED === 'true',
    CRON_SCHEDULE: process.env.CRON_SCHEDULE || '*/5 * * * *',
    MAX_FILE_SIZE_MB: 50,
    MAX_SOLR_CONTENT: 30000,
    TIKA_TIMEOUT: 120000,
    NIFI_TIMEOUT: 120000,
  })),
}));

jest.mock('../src/services/CouchDBService', () => ({
  CouchDBService: {
    getInstance: jest.fn(),
  },
}));

jest.mock('../src/services/TikaService', () => ({
  TikaService: {
    getInstance: jest.fn(),
  },
}));

jest.mock('../src/services/SolrService', () => ({
  SolrService: {
    getInstance: jest.fn(),
  },
}));

jest.mock('../src/services/NiFiService', () => ({
  NiFiService: {
    getInstance: jest.fn(),
  },
}));

jest.mock('../src/services/DecryptionService', () => ({
  DecryptionService: {
    getInstance: jest.fn(),
  },
}));

jest.mock('node-cron', () => ({ schedule: jest.fn() }));

jest.mock('../src/config', () => ({
  additionalEnvironmentVariables: [],
}));

import { CouchDBService } from '../src/services/CouchDBService';
import { TikaService } from '../src/services/TikaService';
import { SolrService } from '../src/services/SolrService';
import { NiFiService } from '../src/services/NiFiService';
import { DecryptionService } from '../src/services/DecryptionService';
import { DocumentProcessor } from '../src/job';
import { validateProcessorConfig } from '@tazama-lf/frms-coe-lib/lib/config';

const mockConfig = {
  COUCHDB_URL: 'http://localhost:5984/cms-evidence',
  TIKA_URL: 'http://localhost:9998',
  SOLR_URL: 'http://localhost:8983/solr/biar_docs',
  NIFI_URL: 'http://localhost:8081',
  CRON_ENABLED: false,
  CRON_SCHEDULE: '*/5 * * * *',
  MAX_FILE_SIZE_MB: 50,
  MAX_SOLR_CONTENT: 30000,
  TIKA_TIMEOUT: 120000,
  NIFI_TIMEOUT: 120000,
};

describe('DocumentProcessor', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    (validateProcessorConfig as jest.Mock).mockImplementation(
      (_additionalEnvVars) => ({
        COUCHDB_URL: 'http://localhost:5984/cms-evidence',
        TIKA_URL: 'http://localhost:9998',
        SOLR_URL: 'http://localhost:8983/solr/biar_docs',
        NIFI_URL: 'http://localhost:8081',
        CRON_ENABLED: process.env.CRON_ENABLED === 'true',
        CRON_SCHEDULE: process.env.CRON_SCHEDULE || mockConfig.CRON_SCHEDULE,
        MAX_FILE_SIZE_MB: 50,
        MAX_SOLR_CONTENT: 30000,
        TIKA_TIMEOUT: 120000,
        NIFI_TIMEOUT: 120000,
      })
    );
    (CouchDBService.getInstance as jest.Mock).mockReturnValue(mockCouchDB);
    (TikaService.getInstance as jest.Mock).mockReturnValue(mockTika);
    (SolrService.getInstance as jest.Mock).mockReturnValue(mockSolr);
    (NiFiService.getInstance as jest.Mock).mockReturnValue(mockNifi);
    (DecryptionService.getInstance as jest.Mock).mockReturnValue(
      mockDecryption
    );
  });

  const getProcessor = () => new DocumentProcessor();

  const createValidDoc = (overrides = {}) => ({
    _id: 'doc1',
    evidenceId: 'evidence1',
    taskId: 'task1',
    evidenceType: 'document',
    uploadedAt: '2024-01-01T00:00:00Z',
    _attachments: { 'file.pdf': { content_type: 'application/pdf' } },
    metadata: [
      {
        fileName: 'file.pdf',
        mimeType: 'application/pdf',
        fileSize: 1024,
      },
    ],
    archive: false,
    processingStatus: 'PENDING',
    ...overrides,
  });

  describe('run()', () => {
    it('should process all unprocessed documents', async () => {
      const docs = [
        createValidDoc({ _id: 'doc1' }),
        createValidDoc({ _id: 'doc2' }),
      ];
      mockCouchDB.findUnprocessedDocs.mockResolvedValue(docs);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('PDF content'));
      mockTika.extract.mockResolvedValue({
        text: 'Extracted text',
        metadata: {},
      });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.findUnprocessedDocs).toHaveBeenCalled();
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'COMPLETED',
        undefined,
        0
      );
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc2',
        'COMPLETED',
        undefined,
        0
      );
    });

    it('should handle errors during job execution', async () => {
      mockCouchDB.findUnprocessedDocs.mockRejectedValue(
        new Error('Database error')
      );

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.findUnprocessedDocs).toHaveBeenCalled();
    });
  });

  describe('getUnprocessedDocuments()', () => {
    it('should return documents from database query', async () => {
      // Database query (findUnprocessedDocs) handles filtering
      // Only valid unprocessed docs are returned
      const docs = [
        createValidDoc({ _id: 'valid1' }),
        createValidDoc({ _id: 'valid2' }),
      ];
      mockCouchDB.findUnprocessedDocs.mockResolvedValue(docs);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: 'text', metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.findUnprocessedDocs).toHaveBeenCalled();
      // 2 docs × 2 calls each (PROCESSING + COMPLETED)
      expect(mockCouchDB.updateStatus).toHaveBeenCalledTimes(4);
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'valid1',
        'PROCESSING'
      );
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'valid2',
        'PROCESSING'
      );
    });
  });

  describe('processDocument()', () => {
    it('should process document through full pipeline', async () => {
      const doc = createValidDoc();
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('PDF content'));
      mockTika.extract.mockResolvedValue({
        text: 'Extracted text',
        metadata: { author: 'Test' },
      });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'PROCESSING'
      );
      expect(mockCouchDB.getAttachment).toHaveBeenCalledWith(
        'doc1',
        'file.pdf'
      );
      expect(mockTika.extract).toHaveBeenCalledWith(Buffer.from('PDF content'));
      expect(mockSolr.indexDocument).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'doc1',
          evidenceId: 'evidence1',
          taskId: 'task1',
          fileName: 'file.pdf',
          content: 'Extracted text',
          processingStatus: 'INDEXED',
        })
      );
      expect(mockNifi.sendDocument).toHaveBeenCalledWith(
        expect.objectContaining({
          documentId: 'doc1',
          content: 'Extracted text',
          metadata: { author: 'Test' },
        })
      );
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'COMPLETED',
        undefined,
        0
      );
    });

    it('should decrypt encrypted files', async () => {
      const key = randomBytes(32);
      const iv = randomBytes(12);
      const plaintext = 'Decrypted content';

      const cipher = createCipheriv('aes-256-gcm', key, iv);
      const encrypted = Buffer.concat([
        cipher.update(plaintext, 'utf8'),
        cipher.final(),
      ]);
      const authTag = cipher.getAuthTag();

      const encryptionParams = {
        key: key.toString('base64'),
        iv: iv.toString('base64'),
        authTag: authTag.toString('base64'),
      };

      const doc = createValidDoc({
        metadata: [
          {
            fileName: 'file.pdf',
            mimeType: 'application/pdf',
            fileSize: 1024,
            encryption: encryptionParams,
          },
        ],
      });

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(encrypted);
      mockDecryption.decrypt.mockReturnValue(Buffer.from(plaintext));
      mockTika.extract.mockResolvedValue({ text: 'Extracted', metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockDecryption.decrypt).toHaveBeenCalledWith(
        encrypted,
        encryptionParams
      );
      expect(mockTika.extract).toHaveBeenCalledWith(Buffer.from(plaintext));
    });

    it('should truncate content exceeding max length for Solr', async () => {
      const longText = 'x'.repeat(50000);
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: longText, metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockSolr.indexDocument).toHaveBeenCalledWith(
        expect.objectContaining({
          content: 'x'.repeat(30000),
          textLength: 50000,
        })
      );
      expect(mockNifi.sendDocument).toHaveBeenCalledWith(
        expect.objectContaining({
          content: longText,
        })
      );
    });

    it('should handle files under content limit without truncation', async () => {
      const shortText = 'Short content';
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: shortText, metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockSolr.indexDocument).toHaveBeenCalledWith(
        expect.objectContaining({
          content: shortText,
          textLength: shortText.length,
        })
      );
    });

    it('should reject files exceeding size limit', async () => {
      const doc = createValidDoc({
        metadata: [
          {
            fileName: 'large.pdf',
            mimeType: 'application/pdf',
            fileSize: 100 * 1024 * 1024,
          },
        ],
      });

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        expect.stringContaining('File too large'),
        1
      );
      expect(mockTika.extract).not.toHaveBeenCalled();
    });

    it('should handle document with no attachments', async () => {
      const doc = createValidDoc({ _attachments: {} });

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'No attachments found',
        1
      );
    });

    it('should handle processing errors and update status to ERROR', async () => {
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockRejectedValue(
        new Error('Attachment not found')
      );
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'Attachment not found',
        1
      );
    });

    it('should handle Tika extraction errors', async () => {
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockRejectedValue(new Error('Tika server unavailable'));
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'Tika server unavailable',
        1
      );
    });

    it('should handle Solr indexing errors', async () => {
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: 'text', metadata: {} });
      mockSolr.indexDocument.mockRejectedValue(new Error('Solr error'));
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'Solr error',
        1
      );
    });

    it('should handle NiFi errors', async () => {
      const doc = createValidDoc();

      mockCouchDB.findUnprocessedDocs.mockResolvedValue([doc]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: 'text', metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockRejectedValue(
        new Error('NiFi connection failed')
      );
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'NiFi connection failed'
      );
    });
  });

  describe('retry limit and service outages', () => {
    const networkError = (code: string) =>
      new AxiosError(`connect ${code}`, code);
    const httpError = (status: number) =>
      new AxiosError(
        `Request failed with status code ${status}`,
        'ERR_BAD_RESPONSE',
        undefined,
        undefined,
        {
          status,
          statusText: '',
          headers: {},
          data: {},
        } as unknown as AxiosResponse
      );
    const loggerOf = (processor: unknown) =>
      (processor as { logger: { warn: jest.Mock; error: jest.Mock } }).logger;

    const mockSuccessfulServices = () => {
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockResolvedValue({ text: 'text', metadata: {} });
      mockSolr.indexDocument.mockResolvedValue(undefined);
      mockNifi.sendDocument.mockResolvedValue(undefined);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);
    };

    it('counts each document error and keeps the document eligible', async () => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([
        createValidDoc({ _attachments: {}, retryCount: 1 }),
      ]);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      await getProcessor().run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'No attachments found',
        2
      );
    });

    it('marks a document FAILED on its third document error', async () => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([
        createValidDoc({ _attachments: {}, retryCount: 2 }),
      ]);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      await getProcessor().run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'FAILED',
        'No attachments found',
        3
      );
    });

    it('resets retryCount when a previously failing document succeeds', async () => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([
        createValidDoc({ retryCount: 2 }),
      ]);
      mockSuccessfulServices();

      await getProcessor().run();

      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'COMPLETED',
        undefined,
        0
      );
    });

    it('does not count a service outage against the document and ends the run', async () => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([
        createValidDoc({ _id: 'doc1', retryCount: 2 }),
        createValidDoc({ _id: 'doc2' }),
      ]);
      mockCouchDB.getAttachment.mockRejectedValue(networkError('ECONNREFUSED'));
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      const processor = getProcessor();
      await processor.run();

      // ERROR without an attempt count, even though the document already
      // has two document errors: an outage never makes it FAILED.
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'ERROR',
        'connect ECONNREFUSED',
        undefined
      );
      expect(mockCouchDB.updateStatus).not.toHaveBeenCalledWith(
        'doc1',
        'FAILED',
        expect.anything(),
        expect.anything()
      );
      // The run stops at the first outage; doc2 is left for a later run.
      expect(mockCouchDB.getAttachment).toHaveBeenCalledTimes(1);
      expect(mockCouchDB.updateStatus).not.toHaveBeenCalledWith(
        'doc2',
        expect.anything()
      );
      expect(loggerOf(processor).warn).toHaveBeenCalledWith(
        expect.stringContaining('pausing processing for 30 s'),
        'run'
      );
    });

    it('ends the run when CouchDB goes down while a document error is saved', async () => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([
        createValidDoc({ _id: 'doc1', _attachments: {} }),
        createValidDoc({ _id: 'doc2' }),
      ]);
      mockCouchDB.updateStatus.mockImplementation(
        async (_docId: string, status: string) =>
          status === 'ERROR'
            ? Promise.reject(httpError(503))
            : Promise.resolve(undefined)
      );

      const processor = getProcessor();
      await processor.run();

      // The outage is not swallowed: the run stops before doc2 and backs off.
      expect(mockCouchDB.updateStatus).not.toHaveBeenCalledWith(
        'doc2',
        expect.anything()
      );
      expect(loggerOf(processor).warn).toHaveBeenCalledWith(
        expect.stringContaining('pausing processing for 30 s'),
        'run'
      );
    });

    it.each([
      ['connection refused', networkError('ECONNREFUSED')],
      ['host not found', networkError('ENOTFOUND')],
      ['connect timeout', networkError('ETIMEDOUT')],
      ['HTTP 401', httpError(401)],
      ['HTTP 403', httpError(403)],
      ['HTTP 429', httpError(429)],
      ['HTTP 503', httpError(503)],
    ])('treats %s as a service outage', async (_label, error) => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([createValidDoc()]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockRejectedValue(error);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      await getProcessor().run();

      expect(mockCouchDB.updateStatus).toHaveBeenLastCalledWith(
        'doc1',
        'ERROR',
        error.message,
        undefined
      );
    });

    it.each([
      ['HTTP 400', httpError(400)],
      ['HTTP 500', httpError(500)],
      ['a response timeout', networkError('ECONNABORTED')],
      ['a connection reset', networkError('ECONNRESET')],
      [
        'a decryption failure',
        new Error('Unsupported state or unable to authenticate data'),
      ],
    ])('treats %s as a document error', async (_label, error) => {
      mockCouchDB.findUnprocessedDocs.mockResolvedValue([createValidDoc()]);
      mockCouchDB.getAttachment.mockResolvedValue(Buffer.from('content'));
      mockTika.extract.mockRejectedValue(error);
      mockCouchDB.updateStatus.mockResolvedValue(undefined);

      await getProcessor().run();

      expect(mockCouchDB.updateStatus).toHaveBeenLastCalledWith(
        'doc1',
        'ERROR',
        error.message,
        1
      );
    });

    it('pauses runs with exponential backoff while a service is down', async () => {
      const now = jest.spyOn(Date, 'now');
      mockCouchDB.findUnprocessedDocs.mockRejectedValue(httpError(401));
      const processor = getProcessor();
      const { warn, error } = loggerOf(processor);

      now.mockReturnValue(0);
      await processor.run();
      expect(warn).toHaveBeenLastCalledWith(
        expect.stringContaining('pausing processing for 30 s'),
        'run'
      );

      // Still paused: the next scheduled run does not touch CouchDB.
      now.mockReturnValue(29_999);
      await processor.run();
      expect(mockCouchDB.findUnprocessedDocs).toHaveBeenCalledTimes(1);

      // Each further outage doubles the pause, up to 10 minutes.
      let t = 30_000;
      for (const seconds of [60, 120, 240, 480, 600, 600]) {
        now.mockReturnValue(t);
        await processor.run();
        expect(warn).toHaveBeenLastCalledWith(
          expect.stringContaining(`pausing processing for ${seconds} s`),
          'run'
        );
        t += seconds * 1000;
      }
      expect(error).not.toHaveBeenCalledWith(
        expect.stringContaining('Job failed'),
        expect.anything(),
        'run'
      );
    });

    it('resumes and resets the backoff once the service is back', async () => {
      const now = jest.spyOn(Date, 'now');
      const processor = getProcessor();
      const { warn } = loggerOf(processor);

      mockCouchDB.findUnprocessedDocs.mockRejectedValueOnce(
        networkError('ECONNREFUSED')
      );
      now.mockReturnValue(0);
      await processor.run();

      mockCouchDB.findUnprocessedDocs.mockResolvedValueOnce([createValidDoc()]);
      mockSuccessfulServices();
      now.mockReturnValue(30_000);
      await processor.run();
      expect(mockCouchDB.updateStatus).toHaveBeenCalledWith(
        'doc1',
        'COMPLETED',
        undefined,
        0
      );

      // A new outage starts again from the base delay.
      mockCouchDB.findUnprocessedDocs.mockRejectedValueOnce(
        networkError('ECONNREFUSED')
      );
      now.mockReturnValue(60_000);
      await processor.run();
      expect(warn).toHaveBeenLastCalledWith(
        expect.stringContaining('pausing processing for 30 s'),
        'run'
      );
    });

    it('still logs non-outage job failures without pausing', async () => {
      mockCouchDB.findUnprocessedDocs.mockRejectedValue(httpError(400));
      const processor = getProcessor();
      const { warn, error } = loggerOf(processor);

      await processor.run();
      await processor.run();

      expect(mockCouchDB.findUnprocessedDocs).toHaveBeenCalledTimes(2);
      expect(warn).not.toHaveBeenCalled();
      expect(error).toHaveBeenCalledWith(
        'Job failed: Request failed with status code 400',
        expect.anything(),
        'run'
      );
    });
  });

  describe('startProcessor()', () => {
    const originalEnv = process.env;

    beforeEach(() => {
      process.env = { ...originalEnv };
    });

    afterEach(() => {
      process.env = originalEnv;
      jest.resetModules();
    });

    it('should schedule cron job when CRON_ENABLED is true', () => {
      process.env.CRON_ENABLED = 'true';
      process.env.CRON_SCHEDULE = '0 * * * *';

      const cron = require('node-cron');
      cron.schedule.mockClear();

      const { startProcessor } = require('../src/job');

      startProcessor();

      expect(cron.schedule).toHaveBeenCalledWith(
        '0 * * * *',
        expect.any(Function)
      );
    });

    it('should run immediately when CRON_ENABLED is false', () => {
      process.env.CRON_ENABLED = 'false';

      const cron = require('node-cron');
      cron.schedule.mockClear();

      const { startProcessor } = require('../src/job');

      startProcessor();

      expect(cron.schedule).not.toHaveBeenCalled();
    });

    it('should use custom cron schedule when provided', () => {
      process.env.CRON_ENABLED = 'true';
      process.env.CRON_SCHEDULE = '*/10 * * * *';

      const cron = require('node-cron');
      cron.schedule.mockClear();

      const { startProcessor } = require('../src/job');

      startProcessor();

      expect(cron.schedule).toHaveBeenCalledWith(
        '*/10 * * * *',
        expect.any(Function)
      );
    });

    it('should execute processor.run when cron job triggers', async () => {
      process.env.CRON_ENABLED = 'true';

      const cron = require('node-cron');
      cron.schedule.mockClear();

      const { startProcessor } = require('../src/job');

      startProcessor();

      const cronCallback = cron.schedule.mock.calls[0][1];
      cronCallback();

      await new Promise((resolve) => setImmediate(resolve));

      expect(cron.schedule).toHaveBeenCalled();
    });

    it('should not auto-start when NODE_ENV is test', () => {
      const cron = require('node-cron');
      cron.schedule.mockClear();

      jest.isolateModules(() => {
        process.env.NODE_ENV = 'test';
        require('../src/job');
      });

      expect(cron.schedule).not.toHaveBeenCalled();
    });

    it('should auto-start when NODE_ENV is not test', () => {
      const cron = require('node-cron');
      cron.schedule.mockClear();

      jest.isolateModules(() => {
        process.env.NODE_ENV = 'production';
        process.env.CRON_ENABLED = 'false';
        require('../src/job');
      });

      expect(cron.schedule).not.toHaveBeenCalled();
    });

    it('should auto-start with cron when NODE_ENV is not test and CRON_ENABLED', () => {
      const cron = require('node-cron');
      cron.schedule.mockClear();

      jest.isolateModules(() => {
        process.env.NODE_ENV = 'production';
        process.env.CRON_ENABLED = 'true';
        process.env.CRON_SCHEDULE = '*/30 * * * *';
        require('../src/job');
      });

      expect(cron.schedule).toHaveBeenCalledWith(
        '*/30 * * * *',
        expect.any(Function)
      );
    });
  });
});
