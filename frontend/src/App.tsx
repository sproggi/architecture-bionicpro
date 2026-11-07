import React from 'react';
import Keycloak from 'keycloak-js';
import { ReactKeycloakProvider } from '@react-keycloak/web';
import ReportPage from './components/ReportPage';

const keycloak = new Keycloak({
  url: process.env.REACT_APP_KEYCLOAK_URL || '',
  realm: process.env.REACT_APP_KEYCLOAK_REALM || '',
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || '',
});

const App: React.FC = () => {
  return (
    <ReactKeycloakProvider
      authClient={keycloak}
      initOptions={{ onReady: () => keycloak.updateToken(30).catch(() => {}) }}
    >
      <ReportPage />
    </ReactKeycloakProvider>
  );
};

export default App;
