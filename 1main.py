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

-------------------------------------------

import { ApplicationInsights } from '@microsoft/applicationinsights-web';

const instrumentationKey = import.meta.env.VITE_INSTRUMENTATION_KEY;

if (!instrumentationKey) {
    throw new Error('AppInsights key not provided');
}

const appInsights = new ApplicationInsights({
    config: {
        instrumentationKey
    }
});

appInsights.loadAppInsights();

export function logException(exception: Error, properties?: Record<string, any>) {
    appInsights.trackException({
        exception,
        properties
    });
}


----------------------------------------------------


async initThread(
  tabId: string,
  agentId?: string
): Promise<{ threadId?: string; welcomeMessage?: string }> {
  try {
    const response = await axios.post(`${API_BASE}/chat/init-thread`, {
      tabId,
      agentId,
    });

    return response.data;
  } catch (error: any) {
    logException(error, {
      apiName: '/chat/init-thread',
      tabId,
      agentId,
      statusCode: error.response?.status,
      statusText: error.response?.statusText,
      errorMessage: error.message,
      backendMessage: error.response?.data,
      isAxiosError: axios.isAxiosError(error),
    });

    return {};
  }
}

-----------------------------------------------------------

async initThread(
  tabId: string,
  agentId?: string
): Promise<{ threadId?: string; welcomeMessage?: string }> {
  try {
    const response = await axios.post(`${API_BASE}/chat/init-thread`, {
      tabId,
      agentId,
    });

    return response.data;
  } catch (error: any) {
    const statusCode = error.response?.status;
    const statusText = error.response?.statusText ?? 'Unknown Error';

    logException(
      new Error('Error'),
      {
        problemId: `${statusCode} ${statusText}`,
        type: 'Error',
        apiName: '/chat/init-thread',
        statusCode,
        backendMessage: error.response?.data,
        tabId,
        agentId,
      }
    );

    return {};
  }
}
-----------------------------------
import axios from 'axios';
import { ChatRequest, ChatResponse, ChatMessage } from '@/types';
import { logException } from '@/services/telemetry';

const API_BASE = '/api';

export const chatService = {
  /**
   * Send a question to the agent
   */
  async ask(request: ChatRequest): Promise<ChatResponse> {
    try {
      const formData = new FormData();
      formData.append('question', request.question);
      formData.append('tabId', request.tabId);
      if (request.agentId) {
        formData.append('agentId', request.agentId);
      }

      const response = await axios.post<ChatResponse>(
        `${API_BASE}/chat/ask`,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      );

      return response.data;
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/chat/ask',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );

      return {} as ChatResponse;
    }
  },

  /**
   * Initialize a chat thread
   */
  async initThread(
    tabId: string,
    agentId?: string
  ): Promise<{ threadId?: string; welcomeMessage?: string }> {
    try {
      const response = await axios.post(`${API_BASE}/chat/init-thread`, {
        tabId,
        agentId,
      });

      return response.data;
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/chat/init-thread',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );

      return {};
    }
  },

  /**
   * Register a browser tab
   */
  async registerTab(tabId: string): Promise<void> {
    try {
      await axios.post(`${API_BASE}/chat/register-tab`, { tabId });
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/chat/register-tab',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );
    }
  },

  /**
   * Get chat history for a tab
   */
  async getHistory(tabId: string): Promise<ChatMessage[]> {
    try {
      const response = await axios.get<ChatMessage[]>(
        `${API_BASE}/chat/history/${tabId}`
      );

      return response.data.map((msg) => ({
        ...msg,
        timestamp: new Date(msg.timestamp),
      }));
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/chat/history',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );

      return [];
    }
  },

  /**
   * Clear chat history
   */
  async clear(tabId: string): Promise<void> {
    try {
      await axios.post(`${API_BASE}/chat/clear`, { tabId });
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/chat/clear',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );
    }
  },

  /**
   * Download a file from blob storage
   */
  async downloadBlobFile(containerName: string, blobName: string): Promise<void> {
    try {
      const response = await axios.get(`${API_BASE}/blob/download`, {
        params: {
          fileName: blobName,
          fileId: `${containerName}/${blobName}`,
        },
        responseType: 'blob',
      });

      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', blobName);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/blob/download',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );
    }
  },

  /**
   * Download a file from sandbox
   */
  async downloadSandboxFile(
    containerName: string,
    blobName: string
  ): Promise<void> {
    try {
      const response = await axios.get(`${API_BASE}/blob/sandbox-download`, {
        params: {
          fileName: blobName,
          fileId: `${containerName}/${blobName}`,
        },
        responseType: 'blob',
      });

      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', blobName);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error: any) {
      const statusCode = error.response?.status;
      const statusText = error.response?.statusText ?? 'Unknown Error';

      logException(
        new Error(`HTTPS Error ${statusCode} - ${statusText}`),
        {
          apiName: '/blob/sandbox-download',
          Type: 'Error',
          statusCode,
          backendMessage: JSON.stringify(error.response?.data),
        }
      );
    }
  },
};


