const DAY_MS = 24 * 60 * 60 * 1000;

export interface LongDrive {
  startedAt: number;
  durationMinutes: number;
}

export interface GapSummary {
  /** The longest stretch without a long drive, in days. */
  longestGapDays: number;
  /** Where that stretch runs, for the shaded band on the timeline. */
  longestGapFrom: number;
  longestGapTo: number;
  /** Average days between long drives across the range. */
  averageGapDays: number;
}

/**
 * The longest stretch without a long drive. The stretch before the first one
 * and the stretch after the last one count too - a two-month silence at the
 * end of the range is exactly what the question is about.
 */
export function summarizeGaps(drives: LongDrive[], rangeFrom: number, rangeTo: number): GapSummary {
  const marks = [rangeFrom, ...drives.map((drive) => drive.startedAt), rangeTo];
  let longest = 0;
  let longestFrom = rangeFrom;
  let longestTo = rangeFrom;

  for (let i = 1; i < marks.length; i += 1) {
    const gap = marks[i] - marks[i - 1];
    if (gap > longest) {
      longest = gap;
      longestFrom = marks[i - 1];
      longestTo = marks[i];
    }
  }

  const span = Math.max(rangeTo - rangeFrom, 1);
  return {
    longestGapDays: longest / DAY_MS,
    longestGapFrom: longestFrom,
    longestGapTo: longestTo,
    averageGapDays: drives.length > 0 ? span / DAY_MS / drives.length : span / DAY_MS,
  };
}
