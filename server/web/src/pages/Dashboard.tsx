import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Plus, Trash2 } from "lucide-react";
import { useLoadJobs } from "../hooks/useLoadJobs";
import { useStoreJobs } from "../hooks/useStoreJobs";
import { useDeleteLoadJob } from "../hooks/useDeleteLoadJob";
import { useDeleteStoreJob } from "../hooks/useDeleteStoreJob";
import { Table } from "../components/Table";
import { Button } from "../components/Button";
import { Spinner } from "../components/Spinner";
import { EmptyState } from "../components/EmptyState";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { CreateLoadJobModal } from "../components/CreateLoadJobModal";
import { CreateStoreJobModal } from "../components/CreateStoreJobModal";
import type { LoadJob } from "../types/load";
import type { StoreJob } from "../types/store";

export function Dashboard() {
  const navigate = useNavigate();
  const {
    data: loadJobs,
    isLoading: loadJobsLoading,
    error: loadJobsError,
  } = useLoadJobs();
  const {
    data: storeJobs,
    isLoading: storeJobsLoading,
    error: storeJobsError,
  } = useStoreJobs();

  const deleteLoadJob = useDeleteLoadJob();
  const deleteStoreJob = useDeleteStoreJob();

  const [showCreateLoadModal, setShowCreateLoadModal] = useState(false);
  const [showCreateStoreModal, setShowCreateStoreModal] = useState(false);
  const [deleteLoadConfirm, setDeleteLoadConfirm] = useState<string | null>(
    null,
  );
  const [deleteStoreConfirm, setDeleteStoreConfirm] = useState<string | null>(
    null,
  );

  const handleDeleteLoad = async () => {
    if (!deleteLoadConfirm) return;
    try {
      await deleteLoadJob.mutateAsync(deleteLoadConfirm);
      console.log("Delete completed");
      setDeleteLoadConfirm(null);
    } catch (err) {
      console.error("Delete failed:", err);
    }
  };

  const handleDeleteStore = async () => {
    if (!deleteStoreConfirm) return;
    try {
      await deleteStoreJob.mutateAsync(deleteStoreConfirm);
      console.log("Delete completed");
      setDeleteStoreConfirm(null);
    } catch (err) {
      console.error("Delete failed:", err);
    }
  };

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

  const loadColumns = [
    { header: "ID", accessor: (job: LoadJob) => job.id.slice(0, 8) },
    {
      header: "Status",
      accessor: (job: LoadJob) => (
        <span className={getStatusColor(job.status)}>{job.status}</span>
      ),
    },
    { header: "Bucket", accessor: (job: LoadJob) => job.bucket },
    { header: "Prefix", accessor: (job: LoadJob) => job.prefix || "-" },
    { header: "Read Stream", accessor: (job: LoadJob) => job.read_stream },
    { header: "Read Subject", accessor: (job: LoadJob) => job.read_subject },
    { header: "Write Subject", accessor: (job: LoadJob) => job.write_subject },
    {
      header: "Actions",
      accessor: (job: LoadJob) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            setDeleteLoadConfirm(job.id);
          }}
          className="text-error hover:text-error/80 transition-colors"
        >
          <Trash2 size={16} />
        </button>
      ),
    },
  ];

  const storeColumns = [
    { header: "ID", accessor: (job: StoreJob) => job.id.slice(0, 8) },
    { header: "Name", accessor: (job: StoreJob) => job.name },
    {
      header: "Status",
      accessor: (job: StoreJob) => (
        <span className={getStatusColor(job.status)}>{job.status}</span>
      ),
    },
    { header: "Stream", accessor: (job: StoreJob) => job.stream },
    { header: "Subject", accessor: (job: StoreJob) => job.subject },
    { header: "Bucket", accessor: (job: StoreJob) => job.bucket },
    { header: "Prefix", accessor: (job: StoreJob) => job.prefix || "-" },
    {
      header: "Actions",
      accessor: (job: StoreJob) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            setDeleteStoreConfirm(job.id);
          }}
          className="text-error hover:text-error/80 transition-colors"
        >
          <Trash2 size={16} />
        </button>
      ),
    },
  ];

  return (
    <div className="min-h-screen p-8">
      <div className="max-w-7xl mx-auto space-y-8">
        <header>
          <h1 className="text-3xl font-bold">nats3</h1>
          <p className="text-text-muted mt-1">NATS to S3 connector</p>
        </header>

        <section className="bg-bg-panel border border-border-subtle rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-medium">Load Jobs</h2>
            <Button onClick={() => setShowCreateLoadModal(true)}>
              <Plus size={16} className="inline mr-2" />
              Create Load Job
            </Button>
          </div>

          {loadJobsLoading && (
            <div className="flex justify-center py-8">
              <Spinner size={32} />
            </div>
          )}

          {loadJobsError && (
            <div className="text-error py-4">
              Error loading jobs: {(loadJobsError as Error).message}
            </div>
          )}

          {loadJobs && loadJobs.length === 0 && (
            <EmptyState
              title="No load jobs"
              description="Create a load job to start publishing messages from S3 to NATS"
              action={
                <Button onClick={() => setShowCreateLoadModal(true)}>
                  Create Load Job
                </Button>
              }
            />
          )}

          {loadJobs && loadJobs.length > 0 && (
            <Table
              columns={loadColumns}
              data={loadJobs}
              onRowClick={(job) => navigate(`/load/${job.id}`)}
            />
          )}
        </section>

        <section className="bg-bg-panel border border-border-subtle rounded-lg p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-medium">Store Jobs</h2>
            <Button onClick={() => setShowCreateStoreModal(true)}>
              <Plus size={16} className="inline mr-2" />
              Create Store Job
            </Button>
          </div>

          {storeJobsLoading && (
            <div className="flex justify-center py-8">
              <Spinner size={32} />
            </div>
          )}

          {storeJobsError && (
            <div className="text-error py-4">
              Error loading jobs: {(storeJobsError as Error).message}
            </div>
          )}

          {storeJobs && storeJobs.length === 0 && (
            <EmptyState
              title="No store jobs"
              description="Create a store job to start consuming messages from NATS and storing to S3"
              action={
                <Button onClick={() => setShowCreateStoreModal(true)}>
                  Create Store Job
                </Button>
              }
            />
          )}

          {storeJobs && storeJobs.length > 0 && (
            <Table
              columns={storeColumns}
              data={storeJobs}
              onRowClick={(job) => navigate(`/store/${job.id}`)}
            />
          )}
        </section>
      </div>

      <CreateLoadJobModal
        isOpen={showCreateLoadModal}
        onClose={() => setShowCreateLoadModal(false)}
      />

      <CreateStoreJobModal
        isOpen={showCreateStoreModal}
        onClose={() => setShowCreateStoreModal(false)}
      />

      <ConfirmDialog
        isOpen={deleteLoadConfirm !== null}
        onClose={() => setDeleteLoadConfirm(null)}
        onConfirm={handleDeleteLoad}
        title="Delete Load Job"
        message="Are you sure you want to delete this load job? This action cannot be undone."
        confirmText="Delete"
        isLoading={deleteLoadJob.isPending}
      />

      <ConfirmDialog
        isOpen={deleteStoreConfirm !== null}
        onClose={() => setDeleteStoreConfirm(null)}
        onConfirm={handleDeleteStore}
        title="Delete Store Job"
        message="Are you sure you want to delete this store job? This action cannot be undone."
        confirmText="Delete"
        isLoading={deleteStoreJob.isPending}
      />
    </div>
  );
}
