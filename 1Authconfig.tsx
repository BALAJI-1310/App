
import React, {
  createContext,
  useContext,
  useMemo,
  ReactNode,
} from 'react';
import { useMsal, useIsAuthenticated } from '@azure/msal-react';
import { AccountInfo, InteractionStatus } from '@azure/msal-browser';
import { loginRequest } from '@/config/authConfig';
import { UserProfile } from '@/types';
import { logException } from '@/services/TelemetryService';

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

  /**
   * MANUAL LOGIN (button click, forced re-login)
   */
  const login = async () => {
    try {
      await instance.loginRedirect(loginRequest);
    } catch (error: any) {
      logException(error, {
        source: 'AuthContext.login',
        username: account?.username ?? 'unknown',
        errorCode: error?.errorCode,
      });

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

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
