import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Tag, message, Space, Upload, Popconfirm } from 'antd';
import { ReloadOutlined, PlusOutlined, EyeOutlined, UploadOutlined, DeleteOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';

const { TextArea } = Input;

const YoutubeJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [fileList, setFileList] = useState([]);
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
  }, []);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      
      // If file is present, use Multipart/Form-Data
      if (fileList.length > 0) {
        const formData = new FormData();
        formData.append('r2_prefix', values.r2_prefix);
        formData.append('download_mode', values.download_mode || 'both');
        formData.append('file', fileList[0]);
        if (values.file_url) {
            formData.append('file_url', values.file_url);
        }
        if (values.urls) {
            formData.append('tasks', values.urls);
        }

        await api.post('/youtube-jobs/', formData);
      } else {
        // Use JSON
        let tasks = [];
        
        // Parse URLs from text area
        if (values.urls) {
            const textUrls = values.urls.split('\n').map(u => u.trim()).filter(u => u.length > 0);
            tasks = [...tasks, ...textUrls];
        }

        if (tasks.length === 0 && !values.file_url) {
            message.error('Please enter URLs, provide a File URL, or upload a file');
            return;
        }

        const payload = {
            r2_prefix: values.r2_prefix,
            download_mode: values.download_mode || 'both',
            tasks: tasks,
            file_url: values.file_url
        };

        await api.post('/youtube-jobs/', payload);
      }

      message.success('Job(s) created');
      setIsModalOpen(false);
      form.resetFields();
      setFileList([]);
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to create job: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeletePending = async () => {
    try {
      const res = await api.delete('/youtube-jobs/pending');
      message.success(res.data.message || 'Pending jobs deleted');
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to delete pending jobs');
    }
  };

  const handleDeleteJob = async (jobId) => {
    try {
      await api.delete(`/youtube-jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to delete job');
    }
  };

  const uploadProps = {
    onRemove: (file) => {
      const index = fileList.indexOf(file);
      const newFileList = fileList.slice();
      newFileList.splice(index, 1);
      setFileList(newFileList);
    },
    beforeUpload: (file) => {
      setFileList([...fileList, file]);
      return false;
    },
    fileList,
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
          <Popconfirm
            title="Are you sure delete this job?"
            onConfirm={() => handleDeleteJob(record.id)}
            okText="Yes"
            cancelText="No"
          >
             <Button icon={<DeleteOutlined />} size="small" danger>Delete</Button>
          </Popconfirm>
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
        <Space>
          <Popconfirm
            title="Delete all pending jobs?"
            description="This action cannot be undone."
            onConfirm={handleDeletePending}
            okText="Yes"
            cancelText="No"
          >
            <Button danger icon={<DeleteOutlined />}>Delete Pending</Button>
          </Popconfirm>
          <Button icon={<ReloadOutlined />} onClick={fetchJobs}>Refresh</Button>
        </Space>
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
            name="download_mode"
            label="Download Mode"
            initialValue="both"
          >
            <Select>
                <Select.Option value="both">Video + Audio</Select.Option>
                <Select.Option value="audio">Audio Only</Select.Option>
                <Select.Option value="video">Video Only</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item 
            name="file_url" 
            label="File URL (Optional)" 
            help="URL to a text file containing Youtube URLs (one per line)"
          >
            <Input placeholder="https://example.com/my-urls.txt" />
          </Form.Item>
          <Form.Item 
            name="urls" 
            label="Youtube URLs" 
            help="One URL per line"
          >
            <TextArea rows={10} placeholder="https://www.youtube.com/watch?v=...\nhttps://www.youtube.com/watch?v=..." />
          </Form.Item>
          <Form.Item label="Upload File (Optional)">
            <Upload {...uploadProps} maxCount={1} accept=".txt,.csv">
                <Button icon={<UploadOutlined />}>Select File (txt/csv)</Button>
            </Upload>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default YoutubeJobList;
