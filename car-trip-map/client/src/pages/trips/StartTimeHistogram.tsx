import { useMemo } from 'react';

const BIN_COUNT = 48;

function formatBinLabel(from: number, to: number) {
  const options: Intl.DateTimeFormatOptions = {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  };
  return `${new Date(from).toLocaleString(undefined, options)} - ${new Date(to).toLocaleString(undefined, options)}`;
}

/**
 * How many trips started when, over the same span as the start-time slider.
 * Sits directly above the slider so the bars line up with its track: the bars
 * inside the selected window are highlighted, the ones outside are dimmed.
 */
export function StartTimeHistogram({
  startTimes,
  from,
  to,
  windowFrom,
  windowTo,
  height = 56,
}: {
  startTimes: number[];
  from: number;
  to: number;
  windowFrom: number;
  windowTo: number;
  height?: number;
}) {
  const bins = useMemo(() => {
    const span = Math.max(to - from, 1);
    const width = span / BIN_COUNT;
    const counts = new Array<number>(BIN_COUNT).fill(0);

    for (const startedAt of startTimes) {
      const index = Math.min(Math.floor((startedAt - from) / width), BIN_COUNT - 1);
      if (index >= 0) counts[index] += 1;
    }

    return counts.map((count, index) => ({
      count,
      from: from + index * width,
      to: from + (index + 1) * width,
    }));
  }, [startTimes, from, to]);

  const busiestBin = Math.max(...bins.map((bin) => bin.count), 1);

  return (
    <div
      className="flex items-end gap-px"
      style={{ height }}
      role="img"
      aria-label={`Trip starts over time, ${startTimes.length} trips in ${BIN_COUNT} intervals`}
    >
      {bins.map((bin) => {
        const selected = bin.to > windowFrom && bin.from < windowTo;
        return (
          <div
            key={bin.from}
            className="flex h-full flex-1 flex-col justify-end"
            title={`${bin.count} trip${bin.count === 1 ? '' : 's'} · ${formatBinLabel(bin.from, bin.to)}`}
          >
            <div
              className="rounded-t-sm"
              style={{
                height: `${(bin.count / busiestBin) * 100}%`,
                // Same blue as the trip lines on the map; dimmed outside the window.
                backgroundColor: selected ? 'var(--trip-blue)' : 'var(--muted-foreground)',
                opacity: selected ? 1 : 0.3,
              }}
            />
          </div>
        );
      })}
    </div>
  );
}
