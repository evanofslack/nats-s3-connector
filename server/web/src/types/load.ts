export type LoadJobStatus =
  | "Created"
  | "Running"
  | "Paused"
  | "Success"
  | "Failure";

export interface LoadJob {
  id: string;
  name: string;
  status: LoadJobStatus;
  bucket: string;
  prefix?: string;
  read_stream: string;
  read_consumer?: string;
  read_subject: string;
  write_subject: string;
  poll_interval?: { secs: number; nanos: number };
  delete_chunks: boolean;
  from_time?: string;
  to_time?: string;
  created: string;
  updated: string;
}

export interface CreateLoadJob {
  name: string;
  bucket: string;
  prefix?: string;
  read_stream: string;
  read_consumer?: string;
  read_subject: string;
  write_subject: string;
  poll_interval?: { secs: number; nanos: number };
  delete_chunks: boolean;
  from_time?: string;
  to_time?: string;
}
