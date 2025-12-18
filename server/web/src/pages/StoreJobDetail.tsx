import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { ArrowLeft, Trash2 } from "lucide-react";
import { useState } from "react";
import { getStoreJob } from "../api";
import { useDeleteStoreJob } from "../hooks/useDeleteStoreJob";
import { Button } from "../components/Button";
import { Spinner } from "../components/Spinner";
import { ConfirmDialog } from "../components/ConfirmDialog";

export function StoreJobDetail() {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const deleteJob = useDeleteStoreJob();

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

  const getStatusColor = (status: string) => {
    switch (status) {
      case "Created":
        return "text-text-muted";
      case "Running":
        return "text-warning";
      case "Success":
        return "text-success";
      case "Failure":
        return "text-error";
      default:
        return "text-text-primary";
    }
  };

  return (
    <div className="min-h-screen p-8">
      <div className="max-w-4xl mx-auto space-y-6">
        <div className="flex items-center justify-between">
          <Button onClick={() => navigate("/")} variant="secondary">
            <ArrowLeft size={16} className="inline mr-2" />
            Back to Dashboard
          </Button>
          <Button variant="danger" onClick={() => setShowDeleteConfirm(true)}>
            <Trash2 size={16} className="inline mr-2" />
            Delete Job
          </Button>
        </div>

        <div className="bg-bg-panel border border-border-subtle rounded-lg p-6">
          <div className="flex items-center justify-between mb-6">
            <div>
              <h1 className="text-2xl font-bold">{job.name}</h1>
              <p className="text-text-muted text-sm mt-1">ID: {job.id}</p>
            </div>
            <div
              className={`text-lg font-medium ${getStatusColor(job.status)}`}
            >
              {job.status}
            </div>
          </div>

          <div className="space-y-6">
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
                  <dt className="text-sm text-text-muted">Max Count</dt>
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
