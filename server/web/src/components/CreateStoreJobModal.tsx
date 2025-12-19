import { useState } from "react";
import { Modal } from "./Modal";
import { Button } from "./Button";
import { useCreateStoreJob } from "../hooks/useCreateStoreJob";
import type { CreateStoreJob } from "../types/store";
import type { Codec } from "../types/common";

interface CreateStoreJobModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const DEFAULT_MAX_BYTES = 1000000;
const DEFAULT_MAX_COUNT = 1000;

export function CreateStoreJobModal({
  isOpen,
  onClose,
}: CreateStoreJobModalProps) {
  const createJob = useCreateStoreJob();
  const [formData, setFormData] = useState({
    name: "",
    stream: "",
    consumer: "",
    subject: "",
    bucket: "",
    prefix: "",
    max_bytes: DEFAULT_MAX_BYTES.toString(),
    max_count: DEFAULT_MAX_COUNT.toString(),
    codec: "binary" as Codec,
  });

  const [errors, setErrors] = useState<Record<string, string>>({});

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name.trim()) newErrors.name = "Name is required";
    if (!formData.stream.trim()) newErrors.stream = "Stream is required";
    if (!formData.subject.trim()) newErrors.subject = "Subject is required";
    if (!formData.bucket.trim()) newErrors.bucket = "Bucket is required";

    const maxBytes = parseInt(formData.max_bytes);
    if (isNaN(maxBytes) || maxBytes <= 0) {
      newErrors.max_bytes = "Must be a positive number";
    }

    const maxCount = parseInt(formData.max_count);
    if (isNaN(maxCount) || maxCount <= 0) {
      newErrors.max_count = "Must be a positive number";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validate()) return;

    const job: CreateStoreJob = {
      name: formData.name,
      stream: formData.stream,
      consumer: formData.consumer || undefined,
      subject: formData.subject,
      bucket: formData.bucket,
      prefix: formData.prefix || undefined,
      batch: {
        max_bytes: parseInt(formData.max_bytes),
        max_count: parseInt(formData.max_count),
      },
      encoding: {
        codec: formData.codec,
      },
    };

    await createJob.mutateAsync(job);
    onClose();
    setFormData({
      name: "",
      stream: "",
      consumer: "",
      subject: "",
      bucket: "",
      prefix: "",
      max_bytes: DEFAULT_MAX_BYTES.toString(),
      max_count: DEFAULT_MAX_COUNT.toString(),
      codec: "binary",
    });
  };

  const handleClose = () => {
    setErrors({});
    onClose();
  };

  return (
    <Modal isOpen={isOpen} onClose={handleClose} title="Create store job">
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label className="block text-sm font-medium mb-1">
            Job Name <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.name}
            onChange={(e) => setFormData({ ...formData, name: e.target.value })}
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.name && (
            <p className="text-error text-sm mt-1">{errors.name}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Stream <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.stream}
            onChange={(e) =>
              setFormData({ ...formData, stream: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.stream && (
            <p className="text-error text-sm mt-1">{errors.stream}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">Consumer</label>
          <input
            type="text"
            value={formData.consumer}
            onChange={(e) =>
              setFormData({ ...formData, consumer: e.target.value })
            }
            placeholder="Optional"
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Subject <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.subject}
            onChange={(e) =>
              setFormData({ ...formData, subject: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.subject && (
            <p className="text-error text-sm mt-1">{errors.subject}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Bucket <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.bucket}
            onChange={(e) =>
              setFormData({ ...formData, bucket: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.bucket && (
            <p className="text-error text-sm mt-1">{errors.bucket}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">Prefix</label>
          <input
            type="text"
            value={formData.prefix}
            onChange={(e) =>
              setFormData({ ...formData, prefix: e.target.value })
            }
            placeholder="Optional"
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
        </div>

        <div className="border-t border-border-subtle pt-4">
          <h3 className="text-md font-medium mb-3">Batch Settings</h3>

          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-1">
                Max Bytes
              </label>
              <input
                type="number"
                value={formData.max_bytes}
                onChange={(e) =>
                  setFormData({ ...formData, max_bytes: e.target.value })
                }
                className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
              />
              {errors.max_bytes && (
                <p className="text-error text-sm mt-1">{errors.max_bytes}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">
                Max Message Count
              </label>
              <input
                type="number"
                value={formData.max_count}
                onChange={(e) =>
                  setFormData({ ...formData, max_count: e.target.value })
                }
                className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
              />
              {errors.max_count && (
                <p className="text-error text-sm mt-1">{errors.max_count}</p>
              )}
            </div>
          </div>
        </div>

        <div className="border-t border-border-subtle pt-4">
          <h3 className="text-md font-medium mb-3">Encoding</h3>

          <div>
            <select
              value={formData.codec}
              onChange={(e) =>
                setFormData({ ...formData, codec: e.target.value as Codec })
              }
              className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
            >
              <option value="binary">binary</option>
              <option value="json">json</option>
            </select>
          </div>
        </div>

        <div className="flex justify-end gap-2 pt-4">
          <Button type="button" variant="secondary" onClick={handleClose}>
            Cancel
          </Button>
          <Button type="submit" disabled={createJob.isPending}>
            {createJob.isPending ? "Creating..." : "Create Job"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
