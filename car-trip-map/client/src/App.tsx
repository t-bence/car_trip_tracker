import { TripsPage } from './pages/trips/TripsPage';

export default function App() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="app-header px-4 md:px-6 py-3 shadow-sm">
        <h1 className="text-lg font-semibold tracking-tight">Car trip tracker</h1>
      </header>
      <main className="flex-1 p-4 md:p-6">
        <TripsPage />
      </main>
    </div>
  );
}
