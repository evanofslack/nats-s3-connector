export const getStatusColor = (status: string) => {
  switch (status) {
    case "Created":
      return "text-status-created";
    case "Running":
      return "text-status-running";
    case "Paused":
      return "text-status-paused";
    case "Success":
      return "text-status-success";
    case "Failure":
      return "text-status-failure";
    default:
      return "text-text-primary";
  }
};
