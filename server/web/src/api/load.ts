import type { LoadJob, CreateLoadJob } from "../types/load";
import { get, post, del } from "./http";

export async function getLoadJobs(): Promise<LoadJob[]> {
  return get<LoadJob[]>("/load/jobs");
}

export async function getLoadJob(jobId: string): Promise<LoadJob> {
  return get<LoadJob>("/load/job", { job_id: jobId });
}

export async function createLoadJob(job: CreateLoadJob): Promise<LoadJob> {
  return post<LoadJob, CreateLoadJob>("/load/job", job);
}

export async function pauseLoadJob(jobId: string): Promise<LoadJob> {
  return post<LoadJob>("/load/job/pause", null, { job_id: jobId });
}

export async function resumeLoadJob(jobId: string): Promise<LoadJob> {
  return post<LoadJob>("/load/job/resume", null, { job_id: jobId });
}

export async function deleteLoadJob(jobId: string): Promise<void> {
  return del("/load/job", { job_id: jobId });
}
