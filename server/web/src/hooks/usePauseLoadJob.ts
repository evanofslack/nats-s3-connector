import { useMutation, useQueryClient } from "@tanstack/react-query";
import { pauseLoadJob } from "../api";

export function usePauseLoadJob(onPauseSuccess?: () => void) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: pauseLoadJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loadJobs"] });
      queryClient.invalidateQueries({ queryKey: ["loadJob"] });
      onPauseSuccess?.();
    },
  });
}
