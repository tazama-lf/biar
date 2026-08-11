import { createDecipheriv } from 'crypto';

export interface EncryptionParams {
  key: string;
  iv: string;
  authTag: string;
}

export class DecryptionService {
  private static instance: DecryptionService | null = null;
  private readonly algorithm = 'aes-256-gcm';

  static getInstance(): DecryptionService {
    DecryptionService.instance ??= new DecryptionService();
    return DecryptionService.instance;
  }

  decrypt(encryptedBuffer: Buffer, params: EncryptionParams): Buffer {
    const key = Buffer.from(params.key, 'base64');
    const iv = Buffer.from(params.iv, 'base64');
    const authTag = Buffer.from(params.authTag, 'base64');

    const decipher = createDecipheriv(this.algorithm, key, iv);
    decipher.setAuthTag(authTag);

    const decrypted = Buffer.concat([
      decipher.update(encryptedBuffer),
      decipher.final(),
    ]);

    return decrypted;
  }
}
