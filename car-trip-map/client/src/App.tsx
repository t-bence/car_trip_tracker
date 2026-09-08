import { TripsPage } from './pages/trips/TripsPage';

export default function App() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="border-b px-4 md:px-6 py-3">
        <h1 className="text-lg font-semibold text-foreground">Car trip tracker</h1>
      </header>
      <main className="flex-1 p-4 md:p-6">
        <TripsPage />
      </main>
    </div>
  );
}
