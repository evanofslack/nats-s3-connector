import { useMutation, useQueryClient } from "@tanstack/react-query";
import { createLoadJob } from "../api";

export function useCreateLoadJob() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: createLoadJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["loadJobs"] });
    },
  });
}
