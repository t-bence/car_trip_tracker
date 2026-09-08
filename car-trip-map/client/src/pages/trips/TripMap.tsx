import { Fragment, useEffect, useMemo } from 'react';
import { MapContainer, TileLayer, CircleMarker, Polyline, Tooltip, useMap } from 'react-leaflet';
import type { LatLngBoundsExpression, LatLngTuple } from 'leaflet';
import 'leaflet/dist/leaflet.css';

/** A trip with its numbers already parsed - the analytics API sends every
 *  numeric column as a string. */
export interface Trip {
  start_event_id: number;
  start_time: string;
  end_time: string;
  duration_minutes: number;
  distance_km: number;
  start_latitude: number;
  start_longitude: number;
  end_latitude: number;
  end_longitude: number;
}

/** Keeps the viewport on the trips currently selected. */
function FitToTrips({ bounds }: { bounds: LatLngBoundsExpression | null }) {
  const map = useMap();

  useEffect(() => {
    if (bounds) map.fitBounds(bounds, { padding: [32, 32], maxZoom: 15 });
  }, [bounds, map]);

  return null;
}

function formatTime(value: string) {
  return new Date(value).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function TripMap({ trips, height = 480 }: { trips: Trip[]; height?: number }) {
  const points: LatLngTuple[] = useMemo(
    () =>
      trips.flatMap((trip) => [
        [trip.start_latitude, trip.start_longitude] as LatLngTuple,
        [trip.end_latitude, trip.end_longitude] as LatLngTuple,
      ]),
    [trips]
  );

  const bounds = points.length > 0 ? (points as LatLngBoundsExpression) : null;
  const center: LatLngTuple = points[0] ?? [47.4979, 19.0402];

  return (
    <MapContainer
      center={center}
      zoom={12}
      scrollWheelZoom
      style={{ height, width: '100%' }}
      className="rounded-md z-0"
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FitToTrips bounds={bounds} />

      {trips.map((trip) => {
        const start: LatLngTuple = [trip.start_latitude, trip.start_longitude];
        const end: LatLngTuple = [trip.end_latitude, trip.end_longitude];
        const label = `${formatTime(trip.start_time)} → ${formatTime(trip.end_time)} · ${trip.duration_minutes.toFixed(0)} min · ${trip.distance_km.toFixed(1)} km`;

        return (
          <Fragment key={trip.start_event_id}>
            <Polyline positions={[start, end]} className="trip-line" weight={3} opacity={0.8}>
              <Tooltip sticky>{label}</Tooltip>
            </Polyline>
            <CircleMarker center={start} radius={6} className="trip-start-marker">
              <Tooltip>Start · {formatTime(trip.start_time)}</Tooltip>
            </CircleMarker>
            <CircleMarker center={end} radius={6} className="trip-end-marker">
              <Tooltip>End · {formatTime(trip.end_time)}</Tooltip>
            </CircleMarker>
          </Fragment>
        );
      })}
    </MapContainer>
  );
}
