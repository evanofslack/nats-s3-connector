import type { StoreJob, CreateStoreJob } from "../types/store";
import { get, post, del } from "./http";

const API_PREFIX = "/api/v1";

export async function getStoreJobs(): Promise<StoreJob[]> {
  return get<StoreJob[]>(`${API_PREFIX}/store/jobs`);
}

export async function getStoreJob(jobId: string): Promise<StoreJob> {
  return get<StoreJob>(`${API_PREFIX}/store/job`, { job_id: jobId });
}

export async function createStoreJob(job: CreateStoreJob): Promise<StoreJob> {
  return post<StoreJob, CreateStoreJob>(`${API_PREFIX}/store/job`, job);
}

export async function pauseStoreJob(jobId: string): Promise<StoreJob> {
  return post<StoreJob>(`${API_PREFIX}/store/job/pause`, null, {
    job_id: jobId,
  });
}

export async function resumeStoreJob(jobId: string): Promise<StoreJob> {
  return post<StoreJob>(`${API_PREFIX}/store/job/resume`, null, {
    job_id: jobId,
  });
}

export async function deleteStoreJob(jobId: string): Promise<void> {
  return del(`${API_PREFIX}/store/job`, { job_id: jobId });
}
