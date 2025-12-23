import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { getStoreJob } from "../api";
import { useDeleteStoreJob } from "../hooks/useDeleteStoreJob";
import { usePauseStoreJob } from "../hooks/usePauseStoreJob";
import { useResumeStoreJob } from "../hooks/useResumeStoreJob";
import { getStatusColor } from "../utils/status";
import { ArrowLeft, Trash2, Pause, Play } from "lucide-react";
import { Button } from "../components/Button";
import { Spinner } from "../components/Spinner";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { formatDateTime } from "../utils/time";

export function StoreJobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const deleteJob = useDeleteStoreJob();
  const pauseJob = usePauseStoreJob();
  const resumeJob = useResumeStoreJob();

  const {
    data: job,
    isLoading,
    error,
  } = useQuery({
    queryKey: ["storeJob", jobId],
    queryFn: () => getStoreJob(jobId!),
    enabled: !!jobId,
  });

  const handleDelete = async () => {
    if (!jobId) return;
    await deleteJob.mutateAsync(jobId);
    navigate("/");
  };

  const handlePause = async () => {
    if (!jobId) return;
    await pauseJob.mutateAsync(jobId);
  };

  const handleResume = async () => {
    if (!jobId) return;
    await resumeJob.mutateAsync(jobId);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen p-8 flex items-center justify-center">
        <Spinner size={48} />
      </div>
    );
  }

  if (error || !job) {
    return (
      <div className="min-h-screen p-8">
        <div className="max-w-4xl mx-auto">
          <Button onClick={() => navigate("/")} variant="secondary">
            <ArrowLeft size={16} className="inline mr-2" />
            Back to Dashboard
          </Button>
          <div className="mt-8 text-error">
            Error loading job: {(error as Error)?.message || "Job not found"}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen py-24 px-6">
      <div className="max-w-4xl mx-auto space-y-6">
        <div className="flex items-center justify-between">
          <Button onClick={() => navigate("/")} variant="secondary">
            <ArrowLeft size={16} className="inline mr-2" />
            Back to Dashboard
          </Button>
          <div className="flex items-center gap-2">
            {job.status === "Running" && (
              <Button variant="secondary" onClick={handlePause}>
                <Pause size={16} className="inline mr-2" />
                Pause Job
              </Button>
            )}
            {job.status === "Paused" && (
              <Button variant="secondary" onClick={handleResume}>
                <Play size={16} className="inline mr-2" />
                Resume Job
              </Button>
            )}
            <Button variant="danger" onClick={() => setShowDeleteConfirm(true)}>
              <Trash2 size={16} className="inline mr-2" />
              Delete Job
            </Button>
          </div>
        </div>

        <div className="bg-bg-panel border border-border-subtle rounded-lg p-6">
          <div className="flex items-center justify-between mb-6 pr-12">
            <div>
              <h1 className="text-2xl font-bold">{job.name}</h1>
              <p className="text-text-muted text-sm mt-1">ID: {job.id}</p>
              <div className="flex gap-4 mt-2 text-sm text-text-muted">
                <span>Created: {formatDateTime(job.created)}</span>
              </div>
              <div className="flex gap-4 mt-2 text-sm text-text-muted">
                <span>Updated: {formatDateTime(job.updated)}</span>
              </div>
            </div>
            <div
              className={`text-2xl font-medium ${getStatusColor(job.status)}`}
            >
              {job.status}
            </div>
          </div>

          <div className="space-y-6 ">
            <div>
              <h2 className="text-lg font-medium mb-4">Configuration</h2>
              <dl className="grid grid-cols-2 gap-4">
                <div>
                  <dt className="text-sm text-text-muted">Stream</dt>
                  <dd className="mt-1 font-mono">{job.stream}</dd>
                </div>
                <div>
                  <dt className="text-sm text-text-muted">Consumer</dt>
                  <dd className="mt-1 font-mono">{job.consumer || "-"}</dd>
                </div>
                <div>
                  <dt className="text-sm text-text-muted">Subject</dt>
                  <dd className="mt-1 font-mono">{job.subject}</dd>
                </div>
                <div>
                  <dt className="text-sm text-text-muted">Bucket</dt>
                  <dd className="mt-1 font-mono">{job.bucket}</dd>
                </div>
                <div>
                  <dt className="text-sm text-text-muted">Prefix</dt>
                  <dd className="mt-1 font-mono">{job.prefix || "-"}</dd>
                </div>
              </dl>
            </div>

            <div>
              <h2 className="text-lg font-medium mb-4">Batch Settings</h2>
              <dl className="grid grid-cols-2 gap-4">
                <div>
                  <dt className="text-sm text-text-muted">Max Bytes</dt>
                  <dd className="mt-1">
                    {job.batch.max_bytes.toLocaleString()}
                  </dd>
                </div>
                <div>
                  <dt className="text-sm text-text-muted">Max Message Count</dt>
                  <dd className="mt-1">
                    {job.batch.max_count.toLocaleString()}
                  </dd>
                </div>
              </dl>
            </div>

            <div>
              <h2 className="text-lg font-medium mb-4">Encoding</h2>
              <dl className="grid grid-cols-2 gap-4">
                <div>
                  <dt className="text-sm text-text-muted">Codec</dt>
                  <dd className="mt-1">{job.encoding.codec}</dd>
                </div>
              </dl>
            </div>
          </div>
        </div>
      </div>

      <ConfirmDialog
        isOpen={showDeleteConfirm}
        onClose={() => setShowDeleteConfirm(false)}
        onConfirm={handleDelete}
        title="Delete Store Job"
        message="Are you sure you want to delete this store job? This action cannot be undone."
        confirmText="Delete"
        isLoading={deleteJob.isPending}
      />
    </div>
  );
}
