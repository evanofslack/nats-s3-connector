import { useMutation, useQueryClient } from "@tanstack/react-query";
import { createStoreJob } from "../api";

export function useCreateStoreJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: createStoreJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["storeJobs"] });
    },
  });
}
