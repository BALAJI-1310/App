import React from 'react';
import ReactDOM from 'react-dom/client';
import { PublicClientApplication, EventType, AuthenticationResult } from '@azure/msal-browser';
import { MsalProvider } from '@azure/msal-react';
import axios, { AxiosError, InternalAxiosRequestConfig } from 'axios';
import { msalConfig, apiTokenRequest } from '@/config/authConfig';
import { AuthProvider } from '@/contexts/AuthContext';
import { ChatProvider } from '@/contexts/ChatContext';
import { ThemeProvider } from '@/contexts/ThemeContext';
import App from '@/App';
import '@/styles/global.css';

// Initialize MSAL instance
const msalInstance = new PublicClientApplication(msalConfig);

// Handle redirect promise and initialize MSAL
async function initializeMsal() {  
  await msalInstance.initialize();

  // Handle the redirect response - wait for it to complete before rendering
  try {
    const response = await msalInstance.handleRedirectPromise();
    console.log('Redirect response:', response);
    if (response) {
      console.log('Login successful:', response.account?.username);
      msalInstance.setActiveAccount(response.account);
    } else {
      // No redirect response - check for existing accounts
      console.log('Failed to login');
      const accounts = msalInstance.getAllAccounts();
      if (accounts.length > 0 && !msalInstance.getActiveAccount()) {
        msalInstance.setActiveAccount(accounts[0]);
      }
    }
  } catch (error:any) {
    console.log('Error handling redirect:', error);
    console.error('error',error.errorcode);
  }

  // Add event callback to track login success
  msalInstance.addEventCallback((event) => {
    if (event.eventType === EventType.LOGIN_SUCCESS && event.payload) {
      const payload = event.payload as AuthenticationResult;
      msalInstance.setActiveAccount(payload.account);
    }
  });
}

// Setup Axios interceptor to add auth token to all requests
axios.interceptors.request.use(
  async (config: InternalAxiosRequestConfig) => {
    const account = msalInstance.getActiveAccount();
    if (account) {
      try {
        // Use apiTokenRequest to get token for the backend API
        const response = await msalInstance.acquireTokenSilent({
          ...apiTokenRequest,
          account: account,
        });
        config.headers.Authorization = `Bearer ${response.accessToken}`;
      } catch (error) {
        console.error('Failed to acquire token silently:', error);
        // Don't redirect here - let the AuthContext handle it
      }
    }
    return config;
  },
  (error: AxiosError) => {
    return Promise.reject(error);
  }
);

// Set default axios base URL
axios.defaults.baseURL = import.meta.env.VITE_API_BASE_URL || '';

// Initialize and render
initializeMsal().then(() => {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <MsalProvider instance={msalInstance}>
        <ThemeProvider>
          <AuthProvider>
            <ChatProvider>
              <App />
            </ChatProvider>
          </AuthProvider>
        </ThemeProvider>
      </MsalProvider>
    </React.StrictMode>
  );
});
----------------------------------------

import React from 'react';
import ReactDOM from 'react-dom/client';
import {
  PublicClientApplication,
  EventType,
  AuthenticationResult,
} from '@azure/msal-browser';
import { MsalProvider } from '@azure/msal-react';
import axios, {
  AxiosError,
  InternalAxiosRequestConfig,
} from 'axios';

import { msalConfig, apiTokenRequest } from '@/config/authConfig';
import { AuthProvider } from '@/contexts/AuthContext';
import { ChatProvider } from '@/contexts/ChatContext';
import { ThemeProvider } from '@/contexts/ThemeContext';
import { logException } from '@/services/TelemetryService';
import App from '@/App';
import '@/styles/global.css';

// --------------------------------------------------
// MSAL INSTANCE
// --------------------------------------------------
const msalInstance = new PublicClientApplication(msalConfig);

// --------------------------------------------------
// MSAL INITIALIZATION & REDIRECT HANDLING
// --------------------------------------------------
async function initializeMsal() {
  await msalInstance.initialize();

  try {
    const response = await msalInstance.handleRedirectPromise();

    if (response) {
      msalInstance.setActiveAccount(response.account);
    } else {
      const accounts = msalInstance.getAllAccounts();
      if (accounts.length > 0 && !msalInstance.getActiveAccount()) {
        msalInstance.setActiveAccount(accounts[0]);
      }
    }
  } catch (error: any) {
    // ❌ Redirect / login failure
    logException(error, {
      source: 'MSAL.handleRedirectPromise',
      errorCode: error?.errorCode,
      correlationId: error?.correlationId,
    });
  }

  // --------------------------------------------------
  // MSAL EVENT CALLBACK (login success only sets account)
  // --------------------------------------------------
  msalInstance.addEventCallback((event) => {
    if (
      event.eventType === EventType.LOGIN_SUCCESS &&
      event.payload
    ) {
      const payload = event.payload as AuthenticationResult;
      msalInstance.setActiveAccount(payload.account);
    }
  });
}

// --------------------------------------------------
// AXIOS TOKEN INTERCEPTOR
// --------------------------------------------------
axios.interceptors.request.use(
  async (config: InternalAxiosRequestConfig) => {
    const account = msalInstance.getActiveAccount();

    if (account) {
      try {
        const response = await msalInstance.acquireTokenSilent({
          ...apiTokenRequest,
          account,
        });

        config.headers.Authorization = `Bearer ${response.accessToken}`;
      } catch (error: any) {
        // ❌ Silent token acquisition failure
        logException(error, {
          source: 'MSAL.acquireTokenSilent',
          username: account.username,
        });
      }
    }

    return config;
  },
  (error: AxiosError) => {
    // ❌ Axios request error
    logException(error, {
      source: 'Axios.Request',
      url: error.config?.url,
      status: error.response?.status,
    });
    return Promise.reject(error);
  }
);

// --------------------------------------------------
// AXIOS BASE URL
// --------------------------------------------------
axios.defaults.baseURL =
  import.meta.env.VITE_API_BASE_URL || '';

// --------------------------------------------------
// APP BOOTSTRAP
// --------------------------------------------------
initializeMsal().then(() => {
  ReactDOM.createRoot(
    document.getElementById('root')!
  ).render(
    <React.StrictMode>
      <MsalProvider instance={msalInstance}>
        <ThemeProvider>
          <AuthProvider>
            <ChatProvider>
              <App />
            </ChatProvider>
          </AuthProvider>
        </ThemeProvider>
      </MsalProvider>
    </React.StrictMode>
  );
});
