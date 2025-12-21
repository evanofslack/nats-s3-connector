import { useMutation, useQueryClient } from "@tanstack/react-query";
import { pauseStoreJob } from "../api";

export function usePauseStoreJob(onPauseSuccess?: () => void) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: pauseStoreJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["storeJobs"] });
      queryClient.invalidateQueries({ queryKey: ["storeJob"] });
      onPauseSuccess?.();
    },
  });
}
