import { useMutation, useQueryClient } from "@tanstack/react-query";
import { resumeLoadJob } from "../api";

export function useResumeLoadJob(onResumeSuccess?: () => void) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: resumeLoadJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loadJobs"] });
      queryClient.invalidateQueries({ queryKey: ["loadJob"] });
      onResumeSuccess?.();
    },
  });
}
