import React, {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  ReactNode,
} from 'react';
import { useIsAuthenticated } from '@azure/msal-react';
import { ChatMessage } from '@/types';
import { chatService, ApiError } from '@/services/chatService';

interface ChatContextType {
  // State
  messages: ChatMessage[];
  isLoading: boolean;
  isInitializing: boolean;
  error: string | null;
  currentTabId: string;
  threadId: string | null;
  welcomeMessage: string | null;
  isBackendUp: boolean;

  // Actions
  sendMessage: (content: string) => Promise<void>;
  clearChat: () => Promise<void>;
  dismissError: () => void;
  initializeChat: () => Promise<void>;
}

const ChatContext = createContext<ChatContextType | undefined>(undefined);

interface ChatProviderProps {
  children: ReactNode;
}

const DEFAULT_WELCOME_MESSAGE = `**Welcome to the UCMP Data Agent!**

I'm your self-service interface for UCMP data discovery and analysis. With me, you can:

* **Explore and Preview** UCMP datasets, including Accounts, Contacts and Lead Activities.
* **Download query results** in CSV format for analysis and activation workflows.
* **Execute advanced queries** to perform in-depth exploration in the lakehouse.
`;

/**
 * Generate a unique tab ID
 */
const generateTabId = (): string => {
  let tabId = sessionStorage.getItem('chatTabId');
  if (!tabId) {
    tabId = 'tab-' + Date.now() + '-' + Math.random().toString(36).substring(2, 9);
    sessionStorage.setItem('chatTabId', tabId);
  }
  return tabId;
};

export const ChatProvider: React.FC<ChatProviderProps> = ({ children }) => {
  const isAuthenticated = useIsAuthenticated();
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [threadId, setThreadId] = useState<string | null>(null);
  const [welcomeMessage, setWelcomeMessage] = useState<string | null>(null);
  
  const [isBackendUp, setIsBackendUp] = useState<boolean>(true);
  const [currentTabId] = useState<string>(generateTabId);
  const [initialized, setInitialized] = useState(false);
  const [isInitializing, setIsInitializing] = useState(false);

  /**
   * Initialize chat session
   */
  const initializeChat = useCallback(async () => {
    if (!isAuthenticated) {
      return;
    }

    setWelcomeMessage(null);
    setIsInitializing(true);
    try {
      // Register the tab with the backend - this sets session to Active
      await chatService.registerTab(currentTabId);
      
      setIsBackendUp(true);

      // Initialize thread if needed
      const response = await chatService.initThread(currentTabId);
      if (response.threadId) {
        setThreadId(response.threadId);
      }

      setWelcomeMessage(DEFAULT_WELCOME_MESSAGE);

      // Load existing history
      const history = await chatService.getHistory(currentTabId);
      if (history && history.length > 0) {
        setMessages(history);
      }

      setInitialized(true);
      setError(null);
    } catch (err: unknown) {
      console.error('Failed to initialize chat:', err);
      
      if (err instanceof ApiError) {
        setError(err.message);
        setIsBackendUp(!err.isNetworkError && err.statusCode !== 401 && err.statusCode !== 403);
      } else {
        setError('Failed to initialize chat session. Please try again.');
        
        setIsBackendUp(false);
      }
      
      // Always show welcome message so user sees the UI with error banner
      setWelcomeMessage(DEFAULT_WELCOME_MESSAGE);
    } finally {
      setIsInitializing(false);
    }
  }, [currentTabId, isAuthenticated, initialized]);

  /**
   * Send a message to the agent
   */
  const sendMessage = useCallback(
    async (content: string) => {
      if (!content.trim() || isLoading) return;

      const userMessage: ChatMessage = {
        id: 'msg-' + Date.now(),
        role: 'user',
        content: content.trim(),
        timestamp: new Date(),
      };

      // Add user message
      setMessages((prev) => [...prev, userMessage]);
      setIsLoading(true);
      setError(null);

      // Add loading placeholder for assistant
      const loadingId = 'loading-' + Date.now();
      setMessages((prev) => [
        ...prev,
        {
          id: loadingId,
          role: 'assistant',
          content: '',
          timestamp: new Date(),
          isLoading: true,
        },
      ]);

      try {
        const response = await chatService.ask({
          question: content,
          tabId: currentTabId,
        });

        // Replace loading message with actual response
        setMessages((prev) =>
          prev.map((msg) =>
            msg.id === loadingId
              ? {
                  id: 'msg-' + Date.now(),
                  role: 'assistant',
                  content: response.answer[0],
                  timestamp: new Date(),
                  isLoading: false,
                }
              : msg
          )
        );

        if (response.threadId) {
          setThreadId(response.threadId);
        }
      } catch (err) {
        // Remove loading message on error
        setMessages((prev) => prev.filter((msg) => msg.id !== loadingId));
        
        // Use sanitized error message from ApiError
        if (err instanceof ApiError) {
          setError(err.message);
          // Set offline if network error
          if (err.isNetworkError) {
            
            setIsBackendUp(false);
          }
        } else {
          setError('An unexpected error occurred. Please try again.');
        }
      } finally {
        setIsLoading(false);
      }
    },
    [currentTabId, isLoading]
  );

  /**
   * Clear the chat history
   */
  const clearChat = useCallback(async () => {
    try {
      await chatService.clear(currentTabId);
      setMessages([]);
      setThreadId(null);
    } catch (err) {
      console.error('Failed to clear chat:', err);
    }
  }, [currentTabId]);

  /**
   * Dismiss error message
   */
  const dismissError = useCallback(() => {
    setError(null);
  }, []);

  // Initialize when authenticated
  useEffect(() => {
    if (isAuthenticated && !initialized) {
      initializeChat();
    }
  }, [isAuthenticated, initialized, initializeChat]);

  const value: ChatContextType = {
    messages,
    isLoading,
    isInitializing,
    error,
    currentTabId,
    threadId,
    welcomeMessage,
    sendMessage,
    clearChat,
    dismissError,
    initializeChat,
    isBackendUp
  };

  return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>;
};

export const useChat = (): ChatContextType => {
  const context = useContext(ChatContext);
  if (context === undefined) {
    throw new Error('useChat must be used within a ChatProvider');
  }
  return context;
};
