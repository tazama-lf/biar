export interface IFileMetadata {
  fileName: string;
  fileSize: number;
  filePath: string;
  mimeType: string;
  hash: string;
  encryption?: {
    key: string;
    iv: string;
    authTag: string;
  };
}

export interface IAttachment {
  content_type: string;
  revpos: number;
  digest: string;
  length: number;
  stub: boolean;
}

export interface IEvidenceDocument {
  _id: string;
  _rev: string;
  evidenceId: string;
  tenantId: string;
  taskId: string;
  uploadedBy: string;
  uploadedAt: string;
  evidenceType: string;
  description: string;
  archive: boolean;
  metadata?: IFileMetadata[];
  _attachments?: Record<string, IAttachment>;
  processingStatus?: string;
  processedAt?: string;
  lastError?: string;
}
