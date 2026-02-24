import { describe, expect, it } from "vitest";

import {
  WAITING_LABEL_THRESHOLD_SECONDS,
  formatWaitingChipLabel,
} from "@/components/chat/loadingUx";

describe("formatWaitingChipLabel", () => {
  it("shows working message before threshold", () => {
    expect(formatWaitingChipLabel(0)).toBe("Working...");
    expect(formatWaitingChipLabel(3.8)).toBe("Working...");
    expect(formatWaitingChipLabel(WAITING_LABEL_THRESHOLD_SECONDS - 1)).toBe(
      "Working..."
    );
  });

  it("shows still working message at threshold and after", () => {
    expect(formatWaitingChipLabel(WAITING_LABEL_THRESHOLD_SECONDS)).toBe(
      "Still working..."
    );
    expect(formatWaitingChipLabel(17)).toBe("Still working...");
  });
});
