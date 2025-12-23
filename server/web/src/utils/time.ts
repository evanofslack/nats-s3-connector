import { format } from "date-fns";

export const formatDuration = (duration?: { secs: number; nanos: number }) => {
  if (!duration) return "-";
  const { secs } = duration;
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h`;
  return `${Math.floor(secs / 86400)}d`;
};

export const formatDateTime = (dateStr?: string) => {
  if (!dateStr) return "-";
  try {
    return format(new Date(dateStr), "yyyy-MM-dd HH:mm:ss") + " UTC";
  } catch {
    return dateStr;
  }
};
