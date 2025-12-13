import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Checkbox, Tag, message, Space } from 'antd';
import { PlayCircleOutlined, PauseCircleOutlined, StopOutlined, ReloadOutlined, PlusOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

const JobList = () => {
  const [jobs, setJobs] = useState([]);
  const [metadataList, setMetadataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [form] = Form.useForm();

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const res = await api.get('/jobs/');
      setJobs(res.data);
    } catch (error) {
      message.error('Failed to load jobs');
    } finally {
      setLoading(false);
    }
  };

  const fetchMetadata = async () => {
    try {
      const res = await api.get('/metadata/');
      setMetadataList(res.data);
    } catch (error) {
      console.error(error);
    }
  };

  useEffect(() => {
    fetchJobs();
    fetchMetadata();
  }, []);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      await api.post('/jobs/', values);
      message.success('Job created');
      setIsModalOpen(false);
      fetchJobs();
    } catch (error) {
      console.error(error);
    }
  };

  const handleAction = async (jobId, action) => {
    try {
      await api.post(`/jobs/${jobId}/${action}`);
      message.success(`Job ${action}ed`);
      fetchJobs();
    } catch (error) {
      message.error(`Failed to ${action} job`);
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
    { title: 'ID', dataIndex: 'job_id', key: 'job_id', width: 60 },
    { title: 'Metadata ID', dataIndex: 'metadata_id', key: 'metadata_id', width: 100 },
    { title: 'Source', dataIndex: 'src_dir', key: 'src_dir' },
    { title: 'Dest', dataIndex: 'dst_dir', key: 'dst_dir' },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    { 
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space>
          {(record.status === 'PENDING' || record.status === 'PAUSED' || record.status === 'STOPPED' || record.status === 'FAILED') && (
            <Button icon={<PlayCircleOutlined />} size="small" onClick={() => handleAction(record.job_id, 'start')}>Start</Button>
          )}
          {record.status === 'RUNNING' && (
            <Button icon={<StopOutlined />} size="small" danger onClick={() => handleAction(record.job_id, 'stop')}>Stop</Button>
          )}
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setIsModalOpen(true); }}>
          New Transfer Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={fetchJobs}>Refresh</Button>
      </div>
      
      <Table columns={columns} dataSource={jobs} rowKey="job_id" loading={loading} />

      <Modal 
        title="Create Transfer Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="metadata_id" label="Client/Metadata" rules={[{ required: true }]}>
            <Select>
              {metadataList.map(m => (
                <Option key={m.id} value={m.id}>{m.client_name} ({m.endpoint})</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="src_dir" label="Source Directory" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="dst_dir" label="Destination Directory" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="delete_source" valuePropName="checked">
            <Checkbox>Delete source files after transfer</Checkbox>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default JobList;
