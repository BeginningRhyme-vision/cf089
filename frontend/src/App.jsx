import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Login from './pages/Login';
import AuthCallback from './pages/AuthCallback';
import Dashboard from './pages/Dashboard';
import MetadataList from './pages/MetadataList';
import JobList from './pages/JobList';
import YoutubeJobList from './pages/YoutubeJobList';
import YoutubeJobDetail from './pages/YoutubeJobDetail';
import { useAuthStore } from './store';

const PrivateRoute = ({ children }) => {
  const token = useAuthStore((state) => state.token);
  return token ? children : <Navigate to="/login" />;
};

function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/auth/callback" element={<AuthCallback />} />
      
      <Route path="/" element={<PrivateRoute><Dashboard /></PrivateRoute>}>
        <Route index element={<Navigate to="/jobs" replace />} />
        <Route path="metadata" element={<MetadataList />} />
        <Route path="jobs" element={<JobList />} />
        <Route path="youtube-jobs" element={<YoutubeJobList />} />
        <Route path="youtube-jobs/:jobId" element={<YoutubeJobDetail />} />
      </Route>
    </Routes>
  );
}

export default App;
