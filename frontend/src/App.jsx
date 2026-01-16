import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import Login from './pages/Login';
import AuthCallback from './pages/AuthCallback';
import Dashboard from './pages/Dashboard';
import MetadataList from './pages/MetadataList';
import JobList from './pages/JobList';
import YoutubeJobList from './pages/YoutubeJobList';
import YoutubeJobDetail from './pages/YoutubeJobDetail';
import FfmpegJobList from './pages/FfmpegJobList';
import PipelineList from './pages/PipelineList';
import { useAuthStore } from './store';

const PrivateRoute = ({ children }) => {
  const token = useAuthStore((state) => state.token);
  return token ? children : <Navigate to="/login" />;
};

// 检查是否为开发环境，如果是则自动登录模拟用户
if (process.env.NODE_ENV === 'development') {
  const token = useAuthStore.getState().token;
  const user = useAuthStore.getState().user;
  
  // 如果没有token且在开发环境中，自动设置模拟用户
  if (!token && typeof window !== 'undefined') {
    // 模拟用户数据
    const mockUser = {
      id: 1,
      name: 'Development User',
      email: 'dev@example.com',
      avatar_url: 'https://via.placeholder.com/150'
    };
    
    // 设置模拟token和用户信息
    useAuthStore.getState().setAuth('mock-token-dev', mockUser);
  }
}

function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/auth/callback" element={<AuthCallback />} />
      <Route path="/auth/finish" element={<AuthCallback />} />
      
      <Route path="/" element={<PrivateRoute><Dashboard /></PrivateRoute>}>
        <Route index element={<Navigate to="/jobs" replace />} />
        <Route path="metadata" element={<MetadataList />} />
        <Route path="pipelines" element={<PipelineList />} />
        <Route path="jobs" element={<JobList />} />
        <Route path="ffmpeg-jobs" element={<FfmpegJobList />} />
        <Route path="youtube-jobs" element={<YoutubeJobList />} />
        <Route path="youtube-jobs/:jobId" element={<YoutubeJobDetail />} />
      </Route>
    </Routes>
  );
}

export default App;