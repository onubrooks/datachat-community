export const WAITING_LABEL_THRESHOLD_SECONDS = 10;

export function formatWaitingChipLabel(elapsedSeconds: number): string {
  const seconds = Math.max(0, Math.floor(elapsedSeconds));
  if (seconds >= WAITING_LABEL_THRESHOLD_SECONDS) {
    return "Still working...";
  }
  return "Working...";
}
