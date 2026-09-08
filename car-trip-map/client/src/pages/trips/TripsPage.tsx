import { useState } from 'react';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  BarChart,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Input,
  Label,
  Skeleton,
  Slider,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import { useTripBlue } from '../../lib/useTripBlue';
import { summarizeGaps, type LongDrive } from '../../lib/longDrives';
import { LongDriveTimeline } from './LongDriveTimeline';
import { StartTimeHistogram } from './StartTimeHistogram';
import { TripMap, type Trip } from './TripMap';

const SOURCE_NOTE = 'car_usage.dev.silver_trips';

/** A diesel particulate filter needs a sustained hot run to burn its soot off.
 *  Twenty minutes of continuous driving is the usual rule of thumb. */
const LONG_DRIVE_MINUTES = 20;

const DURATION_BUCKETS = [
  { label: '<5', max: 5 },
  { label: '5-10', max: 10 },
  { label: '10-20', max: 20 },
  { label: '20-30', max: 30 },
  { label: '30-45', max: 45 },
  { label: '45-60', max: 60 },
  { label: '60+', max: Infinity },
];

function bucketDurations(trips: { duration_minutes: number }[]) {
  return DURATION_BUCKETS.map((bucket, index) => {
    const from = index === 0 ? 0 : DURATION_BUCKETS[index - 1].max;
    return {
      bucket: bucket.label,
      trip_count: trips.filter((trip) => trip.duration_minutes >= from && trip.duration_minutes < bucket.max).length,
    };
  });
}

/** The analytics API serializes every numeric column as a string. */
function toNumber(value: unknown): number {
  return Number(value);
}

function formatDateTime(epochMs: number) {
  return new Date(epochMs).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatNumber(value: unknown, digits = 1) {
  const parsed = toNumber(value);
  return Number.isFinite(parsed) ? parsed.toFixed(digits) : '-';
}

function QueryError({ message }: { message: string }) {
  return (
    <Alert variant="destructive">
      <AlertTitle>Could not load the data</AlertTitle>
      <AlertDescription>{message}</AlertDescription>
    </Alert>
  );
}

function Kpi({ label, value, unit, loading }: { label: string; value: string; unit: string; loading: boolean }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{label}</CardDescription>
      </CardHeader>
      <CardContent>
        {loading ? (
          <Skeleton className="h-8 w-24" />
        ) : (
          <div className="flex items-baseline gap-1">
            <span className="text-3xl font-semibold text-foreground">{value}</span>
            <span className="text-sm text-muted-foreground">{unit}</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function TripsPage() {
  const tripBlue = useTripBlue();
  const bounds = useAnalyticsQuery('trip_bounds');
  const firstTripDate = bounds.data?.[0]?.first_trip_date;
  const lastTripDate = bounds.data?.[0]?.last_trip_date;
  const lastEventTime = bounds.data?.[0]?.last_event_time;

  // Until the user picks a range, the filter shows the full range of the data.
  const [range, setRange] = useState<{ start: string; end: string } | null>(null);
  const startDate = range?.start ?? firstTripDate ?? '';
  const endDate = range?.end ?? lastTripDate ?? '';

  const ready = startDate !== '' && endDate !== '';
  const parameters = ready ? { start_date: sql.date(startDate), end_date: sql.date(endDate) } : null;

  // The three queries below need the date range, which the bounds query
  // provides. Until it arrives they stay unstarted rather than running with
  // unbound parameters, which would fail and flash an error on first load.
  const totals = useAnalyticsQuery('trip_totals', parameters, { autoStart: ready });
  const trips = useAnalyticsQuery('trips', parameters, { autoStart: ready });
  const daily = useAnalyticsQuery('daily_summary', parameters, { autoStart: ready });

  // A query that has not started yet is still "busy" from the page's side.
  const tripsBusy = !ready || trips.loading;
  const dailyBusy = !ready || daily.loading;
  const warehouseStarting = [bounds, totals, trips, daily].some((query) => query.warehouseStatus?.state === 'STARTING');

  const totalsRow = totals.data?.[0];

  const tripRows: Trip[] = (trips.data ?? []).map((row) => ({
    start_event_id: toNumber(row.start_event_id),
    start_time: row.start_time,
    end_time: row.end_time,
    duration_minutes: toNumber(row.duration_minutes),
    distance_km: toNumber(row.distance_km),
    start_latitude: toNumber(row.start_latitude),
    start_longitude: toNumber(row.start_longitude),
    end_latitude: toNumber(row.end_latitude),
    end_longitude: toNumber(row.end_longitude),
  }));

  const dailyRows = (daily.data ?? []).map((row) => ({
    trip_date: row.trip_date,
    trip_count: toNumber(row.trip_count),
    avg_duration_minutes: toNumber(row.avg_duration_minutes),
    avg_distance_km: toNumber(row.avg_distance_km),
    total_distance_km: toNumber(row.total_distance_km),
  }));

  const loading = !ready || totals.loading;

  // Everything the diesel question needs, derived from the trips in range.
  const durationBuckets = bucketDurations(tripRows);
  const shortTrips = tripRows.filter((trip) => trip.duration_minutes < LONG_DRIVE_MINUTES).length;
  const shortTripShare = tripRows.length > 0 ? (shortTrips / tripRows.length) * 100 : 0;

  const longDrives: LongDrive[] = tripRows
    .filter((trip) => trip.duration_minutes >= LONG_DRIVE_MINUTES)
    .map((trip) => ({ startedAt: new Date(trip.start_time).getTime(), durationMinutes: trip.duration_minutes }))
    .sort((a, b) => a.startedAt - b.startedAt);

  const rangeFrom = startDate !== '' ? new Date(`${startDate}T00:00:00`).getTime() : 0;
  const rangeTo = endDate !== '' ? new Date(`${endDate}T23:59:59`).getTime() : 0;
  const gaps = summarizeGaps(longDrives, rangeFrom, rangeTo);

  // The slider under the map narrows the map to the trips that started inside
  // the window. Its own bounds are the earliest and latest start time of the
  // trips the date filter returned, so a new date range resets the window.
  const startTimes = tripRows.map((trip) => new Date(trip.start_time).getTime());
  const earliestStart = startTimes.length > 0 ? Math.min(...startTimes) : 0;
  const latestStart = startTimes.length > 0 ? Math.max(...startTimes) : 0;
  const slidable = latestStart > earliestStart;

  const [startWindow, setStartWindow] = useState<{ bounds: [number, number]; value: [number, number] } | null>(null);
  const windowMatchesData =
    startWindow !== null && startWindow.bounds[0] === earliestStart && startWindow.bounds[1] === latestStart;
  const [startWindowFrom, startWindowTo] = windowMatchesData ? startWindow.value : [earliestStart, latestStart];

  const mappedTrips = tripRows.filter((trip) => {
    const startedAt = new Date(trip.start_time).getTime();
    return startedAt >= startWindowFrom && startedAt <= startWindowTo;
  });

  const resetRange = () => {
    setRange(null);
    setStartWindow(null);
  };

  return (
    <div className="space-y-6 w-full max-w-7xl mx-auto">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h2 className="text-2xl font-semibold text-foreground">Car trips</h2>
          <p className="text-sm text-muted-foreground">
            {SOURCE_NOTE}
            {lastEventTime ? ` · last trip ended ${new Date(lastEventTime).toLocaleString()}` : ''}
          </p>
        </div>

        <div className="flex flex-wrap items-end gap-3">
          <div className="space-y-1">
            <Label htmlFor="start-date">From</Label>
            <Input
              id="start-date"
              type="date"
              value={startDate}
              min={firstTripDate}
              max={endDate || lastTripDate}
              onChange={(event) => setRange({ start: event.target.value, end: endDate })}
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="end-date">To</Label>
            <Input
              id="end-date"
              type="date"
              value={endDate}
              min={startDate || firstTripDate}
              max={lastTripDate}
              onChange={(event) => setRange({ start: startDate, end: event.target.value })}
            />
          </div>
          <Button variant="outline" onClick={resetRange} disabled={range === null}>
            All trips
          </Button>
        </div>
      </div>

      {!bounds.loading && bounds.error && <QueryError message={bounds.error} />}
      {!loading && totals.error && <QueryError message={totals.error} />}

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <Kpi label="Trips" value={formatNumber(totalsRow?.trip_count, 0)} unit="in range" loading={loading} />
        <Kpi
          label="Average trip length"
          value={formatNumber(totalsRow?.avg_duration_minutes)}
          unit="minutes"
          loading={loading}
        />
        <Kpi
          label="Average trip distance"
          value={formatNumber(totalsRow?.avg_distance_km, 2)}
          unit="km"
          loading={loading}
        />
        <Kpi label="Total distance" value={formatNumber(totalsRow?.total_distance_km, 1)} unit="km" loading={loading} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Where the trips went</CardTitle>
          <CardDescription>
            Each line runs from where a trip started (green play marker) to where it ended (red stop marker). Straight
            lines, not the route driven - only the two endpoints are logged.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {!tripsBusy && trips.error && <QueryError message={trips.error} />}
          {tripsBusy && (
            <div className="space-y-2">
              <Skeleton className="h-[480px] w-full" />
              {warehouseStarting && <p className="text-sm text-muted-foreground">Starting the SQL warehouse...</p>}
            </div>
          )}
          {!tripsBusy && !trips.error && tripRows.length === 0 && (
            <Empty>
              <EmptyHeader>
                <EmptyTitle>No trips in this range</EmptyTitle>
                <EmptyDescription>Widen the date range to see trips.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
          {!tripsBusy && !trips.error && tripRows.length > 0 && (
            <div className="space-y-4">
              <TripMap trips={mappedTrips} fitTo={tripRows} />

              <div className="space-y-2">
                <div className="flex flex-wrap items-baseline justify-between gap-2">
                  <Label htmlFor="start-time-window">Started between</Label>
                  <span className="text-sm text-muted-foreground">
                    {formatDateTime(startWindowFrom)} - {formatDateTime(startWindowTo)} · {mappedTrips.length} of{' '}
                    {tripRows.length} trips
                  </span>
                </div>
                <StartTimeHistogram
                  startTimes={startTimes}
                  from={earliestStart}
                  to={latestStart}
                  windowFrom={startWindowFrom}
                  windowTo={startWindowTo}
                />
                <Slider
                  id="start-time-window"
                  min={earliestStart}
                  max={slidable ? latestStart : earliestStart + 1}
                  step={60_000}
                  value={[startWindowFrom, startWindowTo]}
                  disabled={!slidable}
                  onValueChange={([from, to]) =>
                    setStartWindow({ bounds: [earliestStart, latestStart], value: [from, to] })
                  }
                />
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>Trips per day</CardTitle>
          </CardHeader>
          <CardContent>
            {!dailyBusy && daily.error && <QueryError message={daily.error} />}
            {dailyBusy && <Skeleton className="h-[260px] w-full" />}
            {!dailyBusy && !daily.error && (
              <BarChart data={dailyRows} xKey="trip_date" yKey="trip_count" colors={[tripBlue]} height={260} />
            )}
          </CardContent>
        </Card>
        <Card className="min-w-0">
          <CardHeader>
            <CardTitle>How long the trips are</CardTitle>
            <CardDescription>
              {tripsBusy
                ? 'Minutes per trip'
                : `${shortTrips} of ${tripRows.length} trips (${shortTripShare.toFixed(0)}%) are under ${LONG_DRIVE_MINUTES} minutes`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {tripsBusy ? (
              <Skeleton className="h-[260px] w-full" />
            ) : (
              <BarChart data={durationBuckets} xKey="bucket" yKey="trip_count" colors={[tripBlue]} height={260} />
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Long drives, and the gaps between them</CardTitle>
          <CardDescription>
            A diesel particulate filter burns its soot off during a sustained hot run. Each tick is a drive of{' '}
            {LONG_DRIVE_MINUTES} minutes or more, placed on a real time axis; the shaded band is the longest stretch
            without one. Tick height is the drive&apos;s length.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {tripsBusy && <Skeleton className="h-24 w-full" />}
          {!tripsBusy && tripRows.length === 0 && (
            <Empty>
              <EmptyHeader>
                <EmptyTitle>No trips in this range</EmptyTitle>
                <EmptyDescription>Widen the date range to see the long drives.</EmptyDescription>
              </EmptyHeader>
            </Empty>
          )}
          {!tripsBusy && tripRows.length > 0 && (
            <div className="space-y-4">
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                <div>
                  <div className="text-sm text-muted-foreground">Drives of {LONG_DRIVE_MINUTES}+ minutes</div>
                  <div className="text-2xl font-semibold text-foreground">{longDrives.length}</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground">Longest gap without one</div>
                  <div className="text-2xl font-semibold text-foreground">
                    {formatNumber(gaps.longestGapDays)} <span className="text-sm font-normal">days</span>
                  </div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground">On average, one every</div>
                  <div className="text-2xl font-semibold text-foreground">
                    {formatNumber(gaps.averageGapDays)} <span className="text-sm font-normal">days</span>
                  </div>
                </div>
              </div>
              <LongDriveTimeline drives={longDrives} rangeFrom={rangeFrom} rangeTo={rangeTo} gaps={gaps} />
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
