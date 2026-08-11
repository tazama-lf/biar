import axios from 'axios';
import type { Configuration } from '../config';

export class TikaService {
  private static instance: TikaService | null = null;
  private readonly baseUrl: string;
  private readonly timeout: number;

  private constructor(config: Configuration) {
    this.baseUrl = config.TIKA_URL;
    this.timeout = config.TIKA_TIMEOUT;
  }

  static getInstance(config: Configuration): TikaService {
    TikaService.instance ??= new TikaService(config);
    return TikaService.instance;
  }

  async extract(
    fileBuffer: Buffer
  ): Promise<{ text: string; metadata: Record<string, unknown> }> {
    const [text, metadata] = await Promise.all([
      this.extractText(fileBuffer),
      this.extractMetadata(fileBuffer),
    ]);
    return { text, metadata };
  }

  private async extractText(fileBuffer: Buffer): Promise<string> {
    const response = await axios.put(this.baseUrl + '/tika', fileBuffer, {
      headers: {
        Accept: 'text/plain',
        'Content-Type': 'application/octet-stream',
      },
      timeout: this.timeout,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
    });
    return typeof response.data === 'string'
      ? response.data
      : String(response.data);
  }

  private async extractMetadata(
    fileBuffer: Buffer
  ): Promise<Record<string, unknown>> {
    const response = await axios.put<Record<string, unknown>>(
      this.baseUrl + '/meta',
      fileBuffer,
      {
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/octet-stream',
        },
        timeout: this.timeout,
      }
    );
    return response.data;
  }
}
