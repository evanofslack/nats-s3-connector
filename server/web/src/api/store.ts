import type { StoreJob, CreateStoreJob } from "../types/store";
import { get, post, del } from "./http";

export async function getStoreJobs(): Promise<StoreJob[]> {
  return get<StoreJob[]>("/store/jobs");
}

export async function getStoreJob(jobId: string): Promise<StoreJob> {
  return get<StoreJob>("/store/job", { job_id: jobId });
}

export async function createStoreJob(job: CreateStoreJob): Promise<StoreJob> {
  return post<StoreJob, CreateStoreJob>("/store/job", job);
}

export async function deleteStoreJob(jobId: string): Promise<void> {
  return del("/store/job", { job_id: jobId });
}
