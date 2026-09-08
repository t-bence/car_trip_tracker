import { Fragment, useEffect, useMemo } from 'react';
import { MapContainer, TileLayer, Marker, Polyline, Tooltip, useMap } from 'react-leaflet';
import { divIcon, type LatLngBoundsExpression, type LatLngTuple } from 'leaflet';
import 'leaflet/dist/leaflet.css';

/** A round badge holding one glyph. The badge takes its color from the CSS
 *  class, so both markers follow the theme. */
function pinIcon(className: string, glyph: string) {
  return divIcon({
    className: `trip-pin ${className}`,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    html: `<svg viewBox="0 0 24 24" width="24" height="24" aria-hidden="true">
             <circle cx="12" cy="12" r="10" fill="currentColor" stroke="white" stroke-width="2" />
             ${glyph}
           </svg>`,
  });
}

/** A play triangle for the start of a trip, a stop square for its end. */
const START_ICON = pinIcon('trip-pin-start', '<polygon points="10,8 16,12 10,16" fill="white" />');
const END_ICON = pinIcon('trip-pin-end', '<rect x="9" y="9" width="6" height="6" rx="1" fill="white" />');

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

export function TripMap({
  trips,
  fitTo = trips,
  height = 480,
}: {
  /** The trips drawn on the map. */
  trips: Trip[];
  /** The trips the viewport is fitted to. Defaults to the drawn ones; pass the
   *  unfiltered set to keep the view still while a filter changes. */
  fitTo?: Trip[];
  height?: number;
}) {
  const points: LatLngTuple[] = useMemo(
    () =>
      fitTo.flatMap((trip) => [
        [trip.start_latitude, trip.start_longitude] as LatLngTuple,
        [trip.end_latitude, trip.end_longitude] as LatLngTuple,
      ]),
    [fitTo]
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
            <Marker position={start} icon={START_ICON}>
              <Tooltip>Start · {formatTime(trip.start_time)}</Tooltip>
            </Marker>
            <Marker position={end} icon={END_ICON}>
              <Tooltip>End · {formatTime(trip.end_time)}</Tooltip>
            </Marker>
          </Fragment>
        );
      })}
    </MapContainer>
  );
}
