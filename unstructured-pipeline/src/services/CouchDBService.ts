import axios from 'axios';
import { setTimeout as sleep } from 'timers/promises';
import type { Configuration } from '../config';

interface CouchDBFindResponse<T> {
  docs: T[];
}

const DEFAULT_RETRIES = 3;
const RETRY_DELAY_MS = 100;
const HTTP_CONFLICT = 409;

export class CouchDBService {
  private static instance: CouchDBService | null = null;
  private readonly baseUrl: string;

  private constructor(config: Configuration) {
    this.baseUrl = config.COUCHDB_URL;
  }

  static getInstance(config: Configuration): CouchDBService {
    CouchDBService.instance ??= new CouchDBService(config);
    return CouchDBService.instance;
  }

  async findUnprocessedDocs<T>(): Promise<T[]> {
    const response = await axios.post<CouchDBFindResponse<T>>(
      this.baseUrl + '/_find',
      {
        selector: {
          $or: [
            { processingStatus: { $exists: false } },
            { processingStatus: { $ne: 'COMPLETED' } },
          ],
          archive: { $ne: true },
          _attachments: { $exists: true },
          'metadata.0': { $exists: true },
        },
        limit: 1000,
      },
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
    return response.data.docs;
  }

  async getDocument<T>(documentId: string): Promise<T> {
    const response = await axios.get<T>(this.baseUrl + '/' + documentId);
    return response.data;
  }

  async getAttachment(
    documentId: string,
    attachmentName: string
  ): Promise<Buffer> {
    const response = await axios.get(
      this.baseUrl +
        '/' +
        documentId +
        '/' +
        encodeURIComponent(attachmentName),
      {
        responseType: 'arraybuffer',
      }
    );
    return Buffer.from(response.data as ArrayBuffer);
  }

  async updateStatus(
    documentId: string,
    status: string,
    errorMessage?: string,
    retries = DEFAULT_RETRIES
  ): Promise<void> {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const currentDoc =
          await this.getDocument<Record<string, unknown>>(documentId);
        const updatedDoc = {
          ...currentDoc,
          processingStatus: status,
          processedAt: new Date().toISOString(),
          ...(errorMessage && { lastError: errorMessage }),
        };
        await axios.put(this.baseUrl + '/' + documentId, updatedDoc, {
          headers: { 'Content-Type': 'application/json' },
        });
        return;
      } catch (error) {
        const isConflict =
          axios.isAxiosError(error) && error.response?.status === HTTP_CONFLICT;
        if (isConflict && attempt < retries) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        throw error;
      }
    }
  }
}
