import { useEffect, useState } from 'react';

interface ToastProps {
  message: string;
  duration?: number;
}

export function Toast({ message, duration = 2000 }: ToastProps) {
  const [visible, setVisible] = useState(true);

  useEffect(() => {
    const timer = setTimeout(() => setVisible(false), duration);
    return () => clearTimeout(timer);
  }, [duration]);

  if (!visible) return null;

  return (
    <div className="toast">
      <p>{message}</p>
    </div>
  );
}

export function useToast() {
  const [toast, setToast] = useState<ToastProps | null>(null);

  const showToast = (message: string, duration = 2000) => {
    setToast({ message, duration });
    setTimeout(() => setToast(null), duration);
  };

  return { toast, showToast };
}
