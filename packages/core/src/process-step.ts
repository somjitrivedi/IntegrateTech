export enum ProcessStep {
  Pending = 'Pending',
  Validating = 'Validating',
  Processing = 'Processing',
  Invalid = 'Invalid',
  Retrying = 'Retrying',
  MaxRetriesExceeded = 'MaxRetriesExceeded',
  Error = 'Error',
  Complete = 'Complete',
}

export const allProcessSteps: readonly ProcessStep[] = Object.values(ProcessStep);
