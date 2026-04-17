import { useEffect, useState } from 'react';

export function Toast({ message, duration = 2000 }) {
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
  const [toast, setToast] = useState(null);

  const showToast = (message, duration = 2000) => {
    setToast({ message, duration });
    setTimeout(() => setToast(null), duration);
  };

  return { toast, showToast };
}
