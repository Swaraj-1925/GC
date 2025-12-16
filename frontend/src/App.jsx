import { useEffect } from 'react';
import './App.css';
import { Header, Sidebar } from './components/Header';
import PriceChart from './components/PriceChart';
import RightPanel from './components/RightPanel';
import AlertModal from './components/AlertModal';
import wsManager from './services/websocket';
import useAppStore from './stores/appStore';

function App() {
  // Connect to alerts WebSocket on mount
  useEffect(() => {
    const disconnect = wsManager.connectAlerts(
      (alert) => useAppStore.getState().addAlert(alert),
      () => { }
    );

    return () => {
      disconnect();
      wsManager.disconnectAll();
    };
  }, []);

  return (
    <div className="app">
      <Header />
      <div className="main-container">
        <Sidebar />
        <PriceChart />
        <RightPanel />
      </div>
      <AlertModal />
    </div>
  );
}

export default App;
