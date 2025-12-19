import { useQuery } from "@tanstack/react-query";
import { getLoadJobs } from "../api";

export function useLoadJobs() {
  return useQuery({
    queryKey: ["loadJobs"],
    queryFn: getLoadJobs,
    staleTime: 30000,
    retry: 2,
  });
}
