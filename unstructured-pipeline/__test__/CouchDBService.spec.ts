import axios from 'axios';
import type { Configuration } from '../src/config';
import { CouchDBService } from '../src/services/CouchDBService';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

const BASE_URL = 'http://localhost:5984/cms-evidence';

describe('CouchDBService', () => {
  const service = CouchDBService.getInstance({
    COUCHDB_URL: BASE_URL,
  } as Configuration);

  describe('findUnprocessedDocs()', () => {
    it('selects new and ERROR documents but not COMPLETED or FAILED ones', async () => {
      mockedAxios.post.mockResolvedValue({ data: { docs: [] } });

      await service.findUnprocessedDocs();

      const [url, body] = mockedAxios.post.mock.calls[0];
      expect(url).toBe(BASE_URL + '/_find');
      expect(body).toMatchObject({
        selector: {
          $or: [
            { processingStatus: { $exists: false } },
            { processingStatus: { $nin: ['COMPLETED', 'FAILED'] } },
          ],
        },
      });
    });
  });

  describe('updateStatus()', () => {
    beforeEach(() => {
      mockedAxios.get.mockResolvedValue({
        data: { _id: 'doc1', _rev: '1-a', retryCount: 2 },
      });
      mockedAxios.put.mockResolvedValue({});
    });

    it('stores the status, error and retry count', async () => {
      await service.updateStatus('doc1', 'FAILED', 'Decryption failed', 3);

      expect(mockedAxios.put).toHaveBeenCalledWith(
        BASE_URL + '/doc1',
        expect.objectContaining({
          processingStatus: 'FAILED',
          lastError: 'Decryption failed',
          retryCount: 3,
        }),
        expect.anything()
      );
    });

    it('resets the retry count to 0', async () => {
      await service.updateStatus('doc1', 'COMPLETED', undefined, 0);

      expect(mockedAxios.put).toHaveBeenCalledWith(
        BASE_URL + '/doc1',
        expect.objectContaining({
          processingStatus: 'COMPLETED',
          retryCount: 0,
        }),
        expect.anything()
      );
    });

    it('leaves an existing retry count unchanged when none is given', async () => {
      await service.updateStatus('doc1', 'ERROR', 'connect ECONNREFUSED');

      expect(mockedAxios.put).toHaveBeenCalledWith(
        BASE_URL + '/doc1',
        expect.objectContaining({ processingStatus: 'ERROR', retryCount: 2 }),
        expect.anything()
      );
    });
  });
});
