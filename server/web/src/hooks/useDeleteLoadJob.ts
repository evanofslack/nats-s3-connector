import { useMutation, useQueryClient } from "@tanstack/react-query";
import { deleteLoadJob } from "../api";

export function useDeleteLoadJob(onDeleteSuccess?: () => void) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: deleteLoadJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loadJobs"] });
      onDeleteSuccess?.();
    },
  });
}
