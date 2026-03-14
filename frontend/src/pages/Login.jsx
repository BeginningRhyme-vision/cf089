import React, { useEffect } from 'react';
import { Button, Card, Typography, Layout, message } from 'antd';
import { LoginOutlined } from '@ant-design/icons';
import api from '../api';

const { Title } = Typography;
const { Content } = Layout;

const Login = () => {
  const handleLogin = async () => {
    try {
      const response = await api.get('/auth/feishu/login_url');
      window.location.href = response.data.url;
    } catch (error) {
      console.error('Failed to get login url', error);
      message.error("Failed to connect to login service. Please check your network or contact admin.");
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
            Login with Feishu
          </Button>
        </Card>
      </Content>
    </Layout>
  );
};

export default Login;
