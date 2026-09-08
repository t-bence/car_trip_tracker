import { type GapSummary, type LongDrive } from '../../lib/longDrives';

function positionPercent(value: number, from: number, to: number) {
  return ((value - from) / Math.max(to - from, 1)) * 100;
}

function formatDay(epochMs: number) {
  return new Date(epochMs).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/**
 * Every long drive as a tick on a real time axis, so the gaps between them are
 * visible as gaps. The longest gap is shaded.
 */
export function LongDriveTimeline({
  drives,
  rangeFrom,
  rangeTo,
  gaps,
  height = 72,
}: {
  drives: LongDrive[];
  rangeFrom: number;
  rangeTo: number;
  gaps: GapSummary;
  height?: number;
}) {
  const longestDrive = Math.max(...drives.map((drive) => drive.durationMinutes), 1);

  return (
    <div className="space-y-1">
      <div className="relative w-full rounded-md bg-muted/50" style={{ height }}>
        <div
          className="absolute inset-y-0 bg-destructive/10"
          style={{
            left: `${positionPercent(gaps.longestGapFrom, rangeFrom, rangeTo)}%`,
            width: `${positionPercent(gaps.longestGapTo, rangeFrom, rangeTo) - positionPercent(gaps.longestGapFrom, rangeFrom, rangeTo)}%`,
          }}
          title={`Longest gap: ${gaps.longestGapDays.toFixed(1)} days`}
        />
        {drives.map((drive) => (
          <div
            key={drive.startedAt}
            className="absolute bottom-0 w-1 -translate-x-1/2 rounded-t-sm"
            style={{
              left: `${positionPercent(drive.startedAt, rangeFrom, rangeTo)}%`,
              height: `${20 + (drive.durationMinutes / longestDrive) * 80}%`,
              backgroundColor: 'var(--trip-blue)',
            }}
            title={`${new Date(drive.startedAt).toLocaleString()} · ${drive.durationMinutes.toFixed(0)} min`}
          />
        ))}
      </div>
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>{formatDay(rangeFrom)}</span>
        <span>{formatDay(rangeTo)}</span>
      </div>
    </div>
  );
}
