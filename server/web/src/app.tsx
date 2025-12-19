import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Dashboard } from './pages/Dashboard';
import { LoadJobDetail } from './pages/LoadJobDetail';
import { StoreJobDetail } from './pages/StoreJobDetail';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/load/:jobId" element={<LoadJobDetail />} />
        <Route path="/store/:jobId" element={<StoreJobDetail />} />
      </Routes>
    </BrowserRouter>
  );
}
