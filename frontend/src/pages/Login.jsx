import React, { useEffect } from 'react';
import { Button, Card, Typography, Layout, message } from 'antd';
import { LoginOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';
import { useAuthStore } from '../store';

const { Title } = Typography;
const { Content } = Layout;

const Login = () => {
  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);

  const handleLogin = async () => {
    try {
      const response = await api.post('/auth/debug/login');
      const { access_token, user } = response.data;
      setAuth(access_token, user);
      message.success('Login successful');
      navigate('/');
    } catch (error) {
      console.error('Failed to login', error);
      message.error("Login failed. Please check your network or contact admin.");
    }
  };

  return (
    <Layout style={{ minHeight: '100vh', justifyContent: 'center', alignItems: 'center' }}>
      <Content>
        <Card style={{ width: 400, textAlign: 'center' }}>
          <Title level={2}>Unbound Future Admin</Title>
          <p>Enterprise Business Management</p>
          <Button 
            type="primary" 
            icon={<LoginOutlined />} 
            size="large" 
            onClick={handleLogin}
            block
          >
            Login with jaime123
          </Button>
        </Card>
      </Content>
    </Layout>
  );
};

export default Login;
