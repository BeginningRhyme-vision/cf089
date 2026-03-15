import React, { useState } from 'react';
import { Button, Card, Typography, Layout, message, Form, Input } from 'antd';
import { LockOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';
import { useAuthStore } from '../store';

const { Title } = Typography;
const { Content } = Layout;

const Login = () => {
  const navigate = useNavigate();
  const { setAuth } = useAuthStore();
  const [loading, setLoading] = useState(false);

  const handleLogin = async (values) => {
    try {
      setLoading(true);
      const response = await api.post('/auth/passcode/login', {
        passcode: values.passcode,
      });
      setAuth(response.data.access_token, response.data.user);
      message.success('Login successful');
      navigate('/');
    } catch (error) {
      message.error(error.response?.data?.error || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Layout style={{ minHeight: '100vh', justifyContent: 'center', alignItems: 'center' }}>
      <Content>
        <Card style={{ width: 400, textAlign: 'center' }}>
          <Title level={2}>Unbound Future Admin</Title>
          <p>Enterprise Business Management</p>
          <Form layout="vertical" onFinish={handleLogin}>
            <Form.Item
              name="passcode"
              rules={[{ required: true, message: '请输入通行密码' }]}
            >
              <Input.Password
                prefix={<LockOutlined />}
                placeholder="请输入通行密码"
                size="large"
              />
            </Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              size="large"
              loading={loading}
              block
            >
              登录
            </Button>
          </Form>
        </Card>
      </Content>
    </Layout>
  );
};

export default Login;
