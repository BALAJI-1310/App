import React, { createContext, useContext, useMemo, ReactNode } from 'react';
import { useMsal, useIsAuthenticated } from '@azure/msal-react';
import { AccountInfo, InteractionStatus } from '@azure/msal-browser';
import { loginRequest } from '@/config/authConfig';
import { UserProfile } from '@/types';

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

  const login = async () => {
    try {
      // Use redirect for more reliable login flow (works in all browsers including embedded)
      await instance.loginRedirect(loginRequest);
    } catch (error) {
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

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};







const httpStatus =
        (error as any)?.status ||
        (error as any)?.statusCode ||
        (error as any)?.httpStatus ||
        (error as any)?.response?.status;
      logException(error as Error, { source: 'Auth.login', account: account?.username || '', httpStatus });

