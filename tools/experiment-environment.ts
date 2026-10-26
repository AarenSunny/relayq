import { arch, availableParallelism, platform, release } from "node:os";

export interface ExperimentEnvironment {
  nodeVersion: string;
  platform: string;
  operatingSystemRelease: string;
  architecture: string;
  availableParallelism: number;
}

export function captureExperimentEnvironment(): ExperimentEnvironment {
  return {
    nodeVersion: process.version,
    platform: platform(),
    operatingSystemRelease: release(),
    architecture: arch(),
    availableParallelism: availableParallelism(),
  };
}
