import type { LoadJob, CreateLoadJob } from "../types/load";
import { get, post, del } from "./http";

const API_PREFIX = "/api/v1";

export async function getLoadJobs(): Promise<LoadJob[]> {
  return get<LoadJob[]>(`${API_PREFIX}/load/jobs`);
}

export async function getLoadJob(jobId: string): Promise<LoadJob> {
  return get<LoadJob>(`${API_PREFIX}/load/job`, { job_id: jobId });
}

export async function createLoadJob(job: CreateLoadJob): Promise<LoadJob> {
  return post<LoadJob, CreateLoadJob>(`${API_PREFIX}/load/job`, job);
}

export async function pauseLoadJob(jobId: string): Promise<LoadJob> {
  return post<LoadJob>(`${API_PREFIX}/load/job/pause`, null, { job_id: jobId });
}

export async function resumeLoadJob(jobId: string): Promise<LoadJob> {
  return post<LoadJob>(`${API_PREFIX}/load/job/resume`, null, {
    job_id: jobId,
  });
}

export async function deleteLoadJob(jobId: string): Promise<void> {
  return del(`${API_PREFIX}/load/job`, { job_id: jobId });
}
