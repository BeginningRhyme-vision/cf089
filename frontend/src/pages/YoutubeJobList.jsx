import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Tag, message, Space, Card, Row, Col, Statistic } from 'antd';
import { ReloadOutlined, PlusOutlined, EyeOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';

const { TextArea } = Input;

const YoutubeJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [form] = Form.useForm();
  const navigate = useNavigate();

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const res = await api.get('/youtube-jobs/');
      setJobs(res.data);
    } catch (error) {
      message.error('Failed to load jobs');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(() => {
        fetchJobs(); // Polling for updates
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      // Split URLs by newline
      const urls = values.urls.split('\n').filter(line => line.trim() !== '');
      
      const payload = {
        r2_prefix: values.r2_prefix,
        urls: urls
      };

      await api.post('/youtube-jobs/', payload);
      message.success('Job created');
      setIsModalOpen(false);
      form.resetFields();
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to create job');
    }
  };

  const statusColors = {
    PENDING: 'default',
    RUNNING: 'processing',
    PAUSED: 'warning',
    STOPPED: 'error',
    COMPLETED: 'success',
    FAILED: 'error'
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
    { title: 'R2 Prefix', dataIndex: 'r2_prefix', key: 'r2_prefix' },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    {
      title: 'Progress',
      key: 'progress',
      render: (_, record) => (
        <Space size="middle">
            <span style={{color: 'green'}}>Success: {record.success_count}</span>
            <span style={{color: 'red'}}>Failed: {record.failed_count}</span>
            <span style={{color: 'blue'}}>Pending: {record.pending_count}</span>
            <span>Total: {record.total_count}</span>
        </Space>
      )
    },
    { title: 'Created At', dataIndex: 'created_at', key: 'created_at' },
    {
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space>
          <Button
            icon={<EyeOutlined />}
            size="small"
            onClick={() => navigate(`/youtube-jobs/${record.id}`)}
          >
            Details
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setIsModalOpen(true)}>
          New Youtube Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={fetchJobs}>Refresh</Button>
      </div>
      
      <Table columns={columns} dataSource={jobs} rowKey="id" loading={loading} />

      <Modal 
        title="Create Youtube Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
        width={800}
      >
        <Form form={form} layout="vertical">
          <Form.Item 
            name="r2_prefix" 
            label="R2 Prefix" 
            rules={[{ required: true, message: 'Please enter R2 Prefix' }]}
            help="e.g. 'my-channel-uploads/'"
          >
            <Input placeholder="my-folder/" />
          </Form.Item>
          <Form.Item 
            name="urls" 
            label="Youtube URLs" 
            rules={[{ required: true, message: 'Please enter at least one URL'}]}
            help="One URL per line"
          >
            <TextArea rows={10} placeholder="https://www.youtube.com/watch?v=...\nhttps://www.youtube.com/watch?v=..." />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default YoutubeJobList;
