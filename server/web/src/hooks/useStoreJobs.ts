import { useQuery } from "@tanstack/react-query";
import { getStoreJobs } from "../api";

export function useStoreJobs() {
  return useQuery({
    queryKey: ["storeJobs"],
    queryFn: getStoreJobs,
    staleTime: 30000,
    retry: 2,
  });
}
