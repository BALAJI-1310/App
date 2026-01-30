// ...existing code...
import React, { createContext, useContext, useMemo, ReactNode, useEffect } from 'react';
import { useMsal, useIsAuthenticated } from '@azure/msal-react';
import { AccountInfo, InteractionStatus, EventType, EventMessage } from '@azure/msal-browser';
import { loginRequest } from '@/config/authConfig';
import { UserProfile } from '@/types';
import TelemetryService from '@/services/TelemetryService';
// ...existing code...

interface AuthContextType {
  isAuthenticated: boolean;
  isLoading: boolean;
  user: UserProfile | null;
  account: AccountInfo | null;
  login: () => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

/**
 * Get user initials from name or email
 */
const getInitials = (name: string, email: string): string => {
  if (name) {
    const parts = name.split(' ').filter(Boolean);
    if (parts.length >= 2) {
      return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    }
    return name.substring(0, 2).toUpperCase();
  }
  return email.substring(0, 2).toUpperCase();
};

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const { instance, accounts, inProgress } = useMsal();
  const isAuthenticated = useIsAuthenticated();
  const account = accounts[0] || null;

  const isLoading = inProgress !== InteractionStatus.None;

  const user: UserProfile | null = useMemo(() => {
    if (!account) return null;
    const name = account.name || '';
    const email = account.username || '';
    return {
      name,
      email,
      initials: getInitials(name, email),
    };
  }, [account]);

  // Track redirect round-trip and MSAL events for telemetry/exceptions
  useEffect(() => {
    try {
      const calledFlag = sessionStorage.getItem('authLoginCalled');
      if (calledFlag) {
        // If we returned from auth and are authenticated, track success
        if (isAuthenticated) {
          TelemetryService.trackEvent?.('Auth.Login.Succeeded', { account: account?.username || '' });
        } else {
          // We returned but not authenticated — track as failure/aborted
          TelemetryService.trackEvent?.('Auth.Login.ReturnedNotAuthenticated', { account: account?.username || '' });
        }
        sessionStorage.removeItem('authLoginCalled');
      }
    } catch (e) {
      TelemetryService.trackException?.(e as Error, { source: 'AuthContext.init' });
    }

    // subscribe to MSAL events to capture login failures/successes
    let callbackId: number | null = null;
    try {
      if (instance && instance.addEventCallback) {
        callbackId = instance.addEventCallback((message: EventMessage) => {
          try {
            if (message.eventType === EventType.LOGIN_FAILURE) {
              const err = (message.error as Error) || new Error('MSAL login failure');
              TelemetryService.trackException?.(err, { source: 'MSAL.LOGIN_FAILURE', details: JSON.stringify(message) });
            } else if (message.eventType === EventType.LOGIN_SUCCESS) {
              TelemetryService.trackEvent?.('Auth.Login.MSALSuccess', { account: accounts[0]?.username || '' });
            }
          } catch (e) {
            TelemetryService.trackException?.(e as Error, { source: 'AuthContext.msalCallback' });
          }
        });
      }
    } catch (e) {
      TelemetryService.trackException?.(e as Error, { source: 'AuthContext.addEventCallback' });
    }

    return () => {
      try {
        if (callbackId !== null && instance && instance.removeEventCallback) {
          instance.removeEventCallback(callbackId);
        }
      } catch {
        /* ignore cleanup errors */
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instance, isAuthenticated, account, accounts]);

  const login = async () => {
    try {
      // telemetry: user started login
      TelemetryService.trackEvent?.('Auth.Login.Initiated', { method: 'redirect' });

      // set flag so we can detect redirect-return
      try {
        sessionStorage.setItem('authLoginCalled', '1');
        sessionStorage.setItem('authLoginCalledAt', new Date().toISOString());
      } catch (e) {
        TelemetryService.trackException?.(e as Error, { source: 'AuthContext.sessionStorage' });
      }

      // visible debug log
      // eslint-disable-next-line no-console
      console.log(1);

      // Use redirect for more reliable login flow
      await instance.loginRedirect(loginRequest);
    } catch (error) {
      TelemetryService.trackException?.(error as Error, { source: 'Auth.login' });
      // eslint-disable-next-line no-console
      console.error('Login failed:', error);
      throw error;
    }
  };

  const logout = () => {
    instance.logoutRedirect({
      postLogoutRedirectUri: window.location.origin,
    });
  };

  const value: AuthContextType = {
    isAuthenticated,
    isLoading,
    user,
    account,
    login,
    logout,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};
// ...existing code...
export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
