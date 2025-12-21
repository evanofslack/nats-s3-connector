import { useMutation, useQueryClient } from "@tanstack/react-query";
import { resumeStoreJob } from "../api";

export function useResumeStoreJob(onResumeSuccess?: () => void) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: resumeStoreJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["storeJobs"] });
      queryClient.invalidateQueries({ queryKey: ["storeJob"] });
      onResumeSuccess?.();
    },
  });
}
