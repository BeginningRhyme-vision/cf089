import React, { useEffect } from 'react';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { Spin, message } from 'antd';
import api from '../api';
import { useAuthStore } from '../store';

const AuthCallback = () => {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);
  const token = useAuthStore((state) => state.token);
  const calledRef = React.useRef(false);

  useEffect(() => {
    if (token) {
        navigate('/');
        return;
    }
    
    // Check for direct token from backend redirect (via /auth/finish)
    const accessToken = searchParams.get('access_token');
    const userStr = searchParams.get('user');

    if (accessToken && userStr) {
        try {
            // Decode Base64 URL safe string
            // Replace - with + and _ with /
            let base64 = userStr.replace(/-/g, '+').replace(/_/g, '/');
            // Add padding if needed
            const pad = base64.length % 4;
            if (pad) {
                if (pad === 1) throw new Error("Invalid base64 length");
                base64 += new Array(5 - pad).join('=');
            }
            const userJson = atob(base64);
            const user = JSON.parse(userJson);
            
            setAuth(accessToken, user);
            message.success('Login successful');
            navigate('/');
            return;
        } catch (e) {
            console.error("Failed to parse user info", e);
            message.error('Login failed: Invalid user data');
            navigate('/login');
            return;
        }
    }

    const code = searchParams.get('code');
    if (code) {
      if (calledRef.current) return;
      calledRef.current = true;

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
  }, [searchParams, navigate, setAuth, token]);

  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
      <Spin size="large" tip="Authenticating..." />
    </div>
  );
};

export default AuthCallback;
