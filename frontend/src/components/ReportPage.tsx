import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

const API_URL = process.env.REACT_APP_API_URL || '';

const toISODate = (d: Date): string => d.toISOString().slice(0, 10);

const defaultFrom = (): string => {
  const d = new Date();
  d.setDate(d.getDate() - 29);
  return toISODate(d);
};

const defaultTo = (): string => toISODate(new Date());

interface Availability {
  data_available_from: string;
  data_available_until: string;
}

const ReportPage: React.FC = () => {
  const { keycloak } = useKeycloak();
  const [periodFrom, setPeriodFrom] = useState<string>(defaultFrom());
  const [periodTo, setPeriodTo] = useState<string>(defaultTo());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);

  const handleLogin = async () => {
    if (!keycloak) return;
    try {
      await keycloak.login();
    } catch {
      setError('Login failed');
    }
  };

  const handleLogout = () => {
    keycloak?.logout();
  };

  const apiGet = async (path: string) => {
    await keycloak!.updateToken(30);
    return fetch(`${API_URL}${path}`, {
      headers: { Authorization: `Bearer ${keycloak!.token}` },
    });
  };

  const downloadReport = async () => {
    if (!keycloak?.token) return;
    setBusy(true);
    setError(null);
    setInfo(null);

    try {
      const availabilityResponse = await apiGet('/api/v1/reports/availability');
      if (availabilityResponse.status === 401) {
        setError('Session expired. Please log in again.');
        return;
      }
      if (!availabilityResponse.ok) {
        const body = await availabilityResponse.json().catch(() => null);
        setError(body?.detail || `Availability check failed: ${availabilityResponse.status}`);
        return;
      }
      const availability: Availability = await availabilityResponse.json();

      if (periodFrom > availability.data_available_until) {
        setError(
          `Data for the requested period has not been processed yet. ` +
          `Available: ${availability.data_available_from} .. ${availability.data_available_until}. ` +
          `ETL runs daily (T+1).`
        );
        return;
      }

      let effectiveTo = periodTo;
      if (periodTo > availability.data_available_until) {
        effectiveTo = availability.data_available_until;
        setInfo(`Data is available up to ${effectiveTo} — generating report for the processed period only.`);
      }

      const response = await apiGet(
        `/api/v1/reports/usage?from=${periodFrom}&to=${effectiveTo}&format=csv`
      );

      if (response.status === 401) {
        setError('Session expired. Please log in again.');
        return;
      }
      if (response.status === 403) {
        const body = await response.json().catch(() => null);
        setError(body?.detail || 'You do not have access to reports.');
        return;
      }
      if (response.status === 404) {
        const body = await response.json().catch(() => null);
        setError(body?.detail || 'No report data for the selected period.');
        return;
      }
      if (response.status === 422) {
        const body = await response.json().catch(() => null);
        setError(body?.detail || 'Invalid request parameters.');
        return;
      }
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `usage-report_${periodFrom}_${effectiveTo}.csv`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
      setInfo('Report downloaded.');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to download the report.');
    } finally {
      setBusy(false);
    }
  };

  if (!keycloak.authenticated) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-gray-50">
        <h1 className="text-2xl font-bold text-gray-800 mb-4">BionicPRO Reports</h1>
        <p className="text-gray-600 mb-6">Sign in to view and download your usage report</p>
        <button
          onClick={handleLogin}
          className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition-colors"
        >
          Log in
        </button>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm">
        <div className="max-w-4xl mx-auto px-4 py-4 flex items-center justify-between">
          <h1 className="text-xl font-bold text-gray-800">BionicPRO Reports</h1>
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-600">{keycloak.tokenParsed?.preferred_username}</span>
            <button
              onClick={handleLogout}
              className="text-sm px-3 py-1.5 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors"
            >
              Log out
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 py-8">
        <div className="bg-white rounded-xl shadow p-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-4">Usage Report</h2>
          <p className="text-sm text-gray-600 mb-6">
            Report is generated on demand from the pre-computed OLAP mart by your request.
            Only data already processed by the daily ETL pipeline is available (T+1).
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1" htmlFor="period-from">
                Period from
              </label>
              <input
                id="period-from"
                type="date"
                value={periodFrom}
                max={periodTo}
                onChange={(e) => setPeriodFrom(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1" htmlFor="period-to">
                Period to
              </label>
              <input
                id="period-to"
                type="date"
                value={periodTo}
                min={periodFrom}
                onChange={(e) => setPeriodTo(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          <button
            onClick={downloadReport}
            disabled={busy}
            className="px-6 py-3 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {busy ? 'Generating report…' : 'Download Report'}
          </button>

          {info && (
            <div className="mt-4 px-4 py-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-800">
              {info}
            </div>
          )}
          {error && (
            <div className="mt-4 px-4 py-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-800">
              {error}
            </div>
          )}
        </div>
      </main>
    </div>
  );
};

export default ReportPage;
