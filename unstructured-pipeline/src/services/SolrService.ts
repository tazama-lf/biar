import axios from 'axios';
import type { Configuration } from '../config';

export class SolrService {
  private static instance: SolrService | null = null;
  private readonly baseUrl: string;

  private constructor(config: Configuration) {
    this.baseUrl = config.SOLR_URL;
  }

  static getInstance(config: Configuration): SolrService {
    SolrService.instance ??= new SolrService(config);
    return SolrService.instance;
  }

  async indexDocument(document: Record<string, unknown>): Promise<void> {
    await axios.post(
      this.baseUrl + '/update/json/docs?commit=true',
      [document],
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }
}
