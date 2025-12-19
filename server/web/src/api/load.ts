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

export async function deleteLoadJob(jobId: string): Promise<void> {
  return del("/load/job", { job_id: jobId });
}
