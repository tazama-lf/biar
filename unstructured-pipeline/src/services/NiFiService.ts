import axios from 'axios';
import type { Configuration } from '../config';

export class NiFiService {
  private static instance: NiFiService | null = null;
  private readonly baseUrl: string;
  private readonly timeout: number;

  private constructor(config: Configuration) {
    this.baseUrl = config.NIFI_URL;
    this.timeout = config.NIFI_TIMEOUT;
  }

  static getInstance(config: Configuration): NiFiService {
    NiFiService.instance ??= new NiFiService(config);
    return NiFiService.instance;
  }

  async sendDocument(payload: Record<string, unknown>): Promise<void> {
    await axios.post(this.baseUrl + '/contentListener', payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: this.timeout,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
    });
  }
}
