import React, { useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { Spin, message } from 'antd';
import api from '../api';
import { useAuthStore } from '../store';

const AuthCallback = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);

  useEffect(() => {
    const code = searchParams.get('code');
    if (code) {
      api.post(`/auth/feishu/callback?code=${code}`)
        .then((response) => {
          const { access_token, user } = response.data;
          setAuth(access_token, user);
          message.success('Login successful');
          navigate('/');
        })
        .catch((error) => {
          console.error(error);
          message.error('Login failed');
          navigate('/login');
        });
    } else {
      navigate('/login');
    }
  }, [searchParams, navigate, setAuth]);

  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
      <Spin size="large" tip="Authenticating..." />
    </div>
  );
};

export default AuthCallback;
