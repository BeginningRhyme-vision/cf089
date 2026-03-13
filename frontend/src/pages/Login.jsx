import React, { useState } from 'react';
import { Button, Card, Typography, Layout, message, Input, Space } from 'antd';
import { LoginOutlined, UserOutlined, LockOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';
import { useAuthStore } from '../store';

const { Title } = Typography;
const { Content } = Layout;

const Login = () => {
  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);

  const handleLogin = async () => {
    if (!password) {
      message.warning('Please enter password');
      return;
    }

    setLoading(true);
    try {
      // 发送密码到后端验证
      const response = await api.post('/auth/debug/login', { password });
      const { access_token, user } = response.data;
      setAuth(access_token, user);
      message.success('Login successful');
      navigate('/');
    } catch (error) {
      console.error('Failed to login', error);
      if (error.response && error.response.status === 401) {
        message.error("Invalid password");
      } else {
        message.error("Login failed. Please check your network or contact admin.");
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <Layout style={{ minHeight: '100vh', justifyContent: 'center', alignItems: 'center' }}>
      <Content>
        <Card style={{ width: 400, textAlign: 'center' }}>
          <Title level={2}>Unbound Future Admin</Title>
          <p style={{ marginBottom: 30 }}>Enterprise Business Management</p>
          
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Input.Password 
              size="large"
              placeholder="Enter Access Password" 
              prefix={<LockOutlined />} 
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              onPressEnter={handleLogin}
            />
            
            <Button 
              type="primary" 
              icon={<LoginOutlined />} 
              size="large" 
              onClick={handleLogin}
              loading={loading}
              block
            >
              Login
            </Button>
          </Space>
        </Card>
      </Content>
    </Layout>
  );
};

export default Login;
