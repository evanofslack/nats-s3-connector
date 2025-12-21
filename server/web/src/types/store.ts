import type { Batch, Encoding } from "./common";

export type StoreJobStatus =
  | "Created"
  | "Running"
  | "Paused"
  | "Success"
  | "Failure";

export interface StoreJob {
  id: string;
  name: string;
  status: StoreJobStatus;
  stream: string;
  consumer?: string;
  subject: string;
  bucket: string;
  prefix?: string;
  batch: Batch;
  encoding: Encoding;
}

export interface CreateStoreJob {
  name: string;
  stream: string;
  consumer?: string;
  subject: string;
  bucket: string;
  prefix?: string;
  batch?: Batch;
  encoding?: Encoding;
}
