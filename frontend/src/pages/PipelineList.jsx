import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Tag, message, Space, Select } from 'antd';
import { ReloadOutlined, PlusOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';

const { TextArea } = Input;
const { Option } = Select;

const PipelineList = () => {
  const [jobs, setJobs] = useState([]);
  const [metadataList, setMetadataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [form] = Form.useForm();
  const navigate = useNavigate();

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const res = await api.get('/pipelines/');
      setJobs(res.data);
    } catch (error) {
      message.error('Failed to load pipelines');
    } finally {
      setLoading(false);
    }
  };
  
  const fetchMetadata = async () => {
      try {
          const res = await api.get('/metadata/');
          setMetadataList(res.data);
      } catch (error) {
          message.error('Failed to load metadata');
      }
  }

  useEffect(() => {
    fetchJobs();
    fetchMetadata();
  }, []);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      
      const youtubeUrls = values.youtube_urls.split('\n').map(u => u.trim()).filter(u => u.length > 0);
      if (youtubeUrls.length === 0) {
          message.error("Please provide Youtube URLs");
          return;
      }

      const payload = {
          name: values.name,
          youtube_urls: youtubeUrls,
          metadata_id: values.metadata_id
      };

      await api.post('/pipelines/', payload);

      message.success('Pipeline created');
      setIsModalOpen(false);
      form.resetFields();
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to create pipeline: ' + (error.response?.data?.error || error.message));
    }
  };

  const statusColors = {
    PENDING: 'default',
    RUNNING: 'processing',
    COMPLETED: 'success',
    FAILED: 'error'
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
    { title: 'Name', dataIndex: 'name', key: 'name' },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    {
        title: 'Linked Jobs',
        key: 'linked_jobs',
        render: (_, record) => (
            <Space direction="vertical" size="small">
                <a onClick={() => navigate(`/youtube-jobs/${record.youtube_job_id}`)}>Youtube Job #{record.youtube_job_id}</a>
                <span>Transfer Job #{record.transfer_job_id}</span>
                <span>FFmpeg Job #{record.ffmpeg_job_id}</span>
            </Space>
        )
    },
    { title: 'Created At', dataIndex: 'created_at', key: 'created_at' },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setIsModalOpen(true)}>
          New Pipeline
        </Button>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={fetchJobs}>Refresh</Button>
        </Space>
      </div>
      
      <Table columns={columns} dataSource={jobs} rowKey="id" loading={loading} />

      <Modal 
        title="Create Pipeline Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
        width={800}
      >
        <Form form={form} layout="vertical">
          <Form.Item 
            name="name" 
            label="Pipeline Name" 
            rules={[{ required: true, message: 'Please enter a name' }]}
          >
            <Input placeholder="My Download Pipeline" />
          </Form.Item>
          
           <Form.Item 
            name="metadata_id" 
            label="Target Storage (Transfer/FFmpeg)" 
            rules={[{ required: true, message: 'Please select target storage' }]} 
          >
            <Select placeholder="Select Metadata">
                {metadataList.map(m => (
                    <Option key={m.id} value={m.id}>{m.client_name} ({m.endpoint})</Option>
                ))}
            </Select>
          </Form.Item>

          <Form.Item 
            name="youtube_urls" 
            label="Youtube URLs" 
            help="One URL per line"
            rules={[{ required: true, message: 'Please enter URLs' }]} 
          >
            <TextArea rows={10} placeholder="https://www.youtube.com/watch?v=..." />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default PipelineList;
