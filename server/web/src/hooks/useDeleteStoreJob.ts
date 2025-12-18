import { useMutation, useQueryClient } from "@tanstack/react-query";
import { deleteStoreJob } from "../api";

export function useDeleteStoreJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: deleteStoreJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["storeJobs"] });
    },
  });
}
