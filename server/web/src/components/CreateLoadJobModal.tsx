import { useState } from "react";
import { Modal } from "./Modal";
import { Button } from "./Button";
import { useCreateLoadJob } from "../hooks/useCreateLoadJob";
import type { CreateLoadJob } from "../types/load";

interface CreateLoadJobModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export function CreateLoadJobModal({
  isOpen,
  onClose,
}: CreateLoadJobModalProps) {
  const createJob = useCreateLoadJob();
  const [formData, setFormData] = useState({
    bucket: "",
    prefix: "",
    read_stream: "",
    read_consumer: "",
    read_subject: "",
    write_subject: "",
    poll_interval: "",
    delete_chunks: false,
    from_time: "",
    to_time: "",
  });

  const [errors, setErrors] = useState<Record<string, string>>({});

  const parseDuration = (
    input: string,
  ): { secs: number; nanos: number } | null => {
    if (!input.trim()) return null;

    const match = input.match(
      /^(\d+)(s|sec|secs|m|min|mins|h|hr|hrs|d|day|days)?$/i,
    );
    if (!match) return null;

    const value = parseInt(match[1]);
    const unit = (match[2] || "s").toLowerCase();

    let seconds = 0;
    if (unit.startsWith("s")) seconds = value;
    else if (unit.startsWith("m")) seconds = value * 60;
    else if (unit.startsWith("h")) seconds = value * 3600;
    else if (unit.startsWith("d")) seconds = value * 86400;

    return { secs: seconds, nanos: 0 };
  };

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.bucket.trim()) newErrors.bucket = "Bucket is required";
    if (!formData.read_stream.trim())
      newErrors.read_stream = "Read stream is required";
    if (!formData.read_subject.trim())
      newErrors.read_subject = "Read subject is required";
    if (!formData.write_subject.trim())
      newErrors.write_subject = "Write subject is required";

    if (formData.poll_interval && !parseDuration(formData.poll_interval)) {
      newErrors.poll_interval = "Invalid duration format (e.g., 30s, 5m, 1h)";
    }

    if (formData.from_time && formData.to_time) {
      const from = new Date(formData.from_time);
      const to = new Date(formData.to_time);
      if (from >= to) {
        newErrors.to_time = "To time must be after from time";
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!validate()) return;

    const job: CreateLoadJob = {
      bucket: formData.bucket,
      prefix: formData.prefix || undefined,
      read_stream: formData.read_stream,
      read_consumer: formData.read_consumer || undefined,
      read_subject: formData.read_subject,
      write_subject: formData.write_subject,
      poll_interval: formData.poll_interval
        ? parseDuration(formData.poll_interval) || undefined
        : undefined,
      delete_chunks: formData.delete_chunks,
      from_time: formData.from_time || undefined,
      to_time: formData.to_time || undefined,
    };

    await createJob.mutateAsync(job);
    onClose();
    setFormData({
      bucket: "",
      prefix: "",
      read_stream: "",
      read_consumer: "",
      read_subject: "",
      write_subject: "",
      poll_interval: "",
      delete_chunks: false,
      from_time: "",
      to_time: "",
    });
  };

  const handleClose = () => {
    setErrors({});
    onClose();
  };

  return (
    <Modal isOpen={isOpen} onClose={handleClose} title="Create Load Job">
      <form onSubmit={handleSubmit} className="space-y-4">
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
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Read Stream <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.read_stream}
            onChange={(e) =>
              setFormData({ ...formData, read_stream: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.read_stream && (
            <p className="text-error text-sm mt-1">{errors.read_stream}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Read Consumer
          </label>
          <input
            type="text"
            value={formData.read_consumer}
            onChange={(e) =>
              setFormData({ ...formData, read_consumer: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Read Subject <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.read_subject}
            onChange={(e) =>
              setFormData({ ...formData, read_subject: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.read_subject && (
            <p className="text-error text-sm mt-1">{errors.read_subject}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Write Subject <span className="text-error">*</span>
          </label>
          <input
            type="text"
            value={formData.write_subject}
            onChange={(e) =>
              setFormData({ ...formData, write_subject: e.target.value })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.write_subject && (
            <p className="text-error text-sm mt-1">{errors.write_subject}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            Poll Interval
            <span className="text-text-muted text-xs ml-2">
              (e.g., 30s, 5m, 1h)
            </span>
          </label>
          <input
            type="text"
            value={formData.poll_interval}
            onChange={(e) =>
              setFormData({ ...formData, poll_interval: e.target.value })
            }
            placeholder="Optional"
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.poll_interval && (
            <p className="text-error text-sm mt-1">{errors.poll_interval}</p>
          )}
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            From Time (UTC)
          </label>
          <input
            type="datetime-local"
            value={formData.from_time}
            onChange={(e) =>
              setFormData({
                ...formData,
                from_time: e.target.value
                  ? new Date(e.target.value).toISOString()
                  : "",
              })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
        </div>

        <div>
          <label className="block text-sm font-medium mb-1">
            To Time (UTC)
          </label>
          <input
            type="datetime-local"
            value={formData.to_time}
            onChange={(e) =>
              setFormData({
                ...formData,
                to_time: e.target.value
                  ? new Date(e.target.value).toISOString()
                  : "",
              })
            }
            className="w-full px-3 py-2 bg-bg-main border border-border-subtle rounded focus:outline-none focus:border-accent"
          />
          {errors.to_time && (
            <p className="text-error text-sm mt-1">{errors.to_time}</p>
          )}
        </div>

        <div className="flex items-center">
          <input
            type="checkbox"
            id="delete_chunks"
            checked={formData.delete_chunks}
            onChange={(e) =>
              setFormData({ ...formData, delete_chunks: e.target.checked })
            }
            className="mr-2"
          />
          <label htmlFor="delete_chunks" className="text-sm">
            Delete chunks after loading
          </label>
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
