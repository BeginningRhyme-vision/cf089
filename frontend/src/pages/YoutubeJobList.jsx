import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Tag, message, Space, Upload, Popconfirm } from 'antd';
import { ReloadOutlined, PlusOutlined, EyeOutlined, UploadOutlined, DeleteOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import api from '../api';

const { TextArea } = Input;

const cellStyle = {
  maxWidth: 180,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  display: 'inline-block' 
};

const YoutubeJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [fileList, setFileList] = useState([]);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
    total: 0
  });
  const [form] = Form.useForm();
  const navigate = useNavigate();

  const fetchJobs = async (page = 1, pageSize = 10) => {
    setLoading(true);
    try {
      const res = await api.get(`/youtube-jobs/?page=${page}&limit=${pageSize}`);
      setJobs(res.data);
      
      // 检查是否有 X-Total-Count 响应头
      if (res.headers && res.headers['x-total-count']) {
        setPagination(prev => ({
          ...prev,
          current: page,
          pageSize: pageSize,
          total: parseInt(res.headers['x-total-count'])
        }));
      } else {
        // 如果没有获取到总数，则基于当前数据长度进行更新
        setPagination(prev => ({
          ...prev,
          current: page,
          pageSize: pageSize
        }));
      }
    } catch (error) {
      message.error('Failed to load jobs');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs(pagination.current, pagination.pageSize);
  }, [pagination.current, pagination.pageSize]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      
      // If file is present, use Multipart/Form-Data
      if (fileList.length > 0) {
        const formData = new FormData();
        formData.append('r2_prefix', values.r2_prefix);
        formData.append('download_mode', values.download_mode || 'both');
        formData.append('video_selection_strategy', values.video_selection_strategy || 'highest_quality');
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
            video_selection_strategy: values.video_selection_strategy || 'highest_quality',
            tasks: tasks,
            file_url: values.file_url
        };

        await api.post('/youtube-jobs/', payload);
      }

      message.success('Job(s) created');
      setIsModalOpen(false);
      form.resetFields();
      setFileList([]);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      console.error(error);
      message.error('Failed to create job: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeletePending = async () => {
    try {
      const res = await api.delete('/youtube-jobs/pending');
      message.success(res.data.message || 'Pending jobs deleted');
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      console.error(error);
      message.error('Failed to delete pending jobs');
    }
  };

  const handleDeleteJob = async (jobId) => {
    try {
      await api.delete(`/youtube-jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to delete job');
    }
  };

  const uploadProps = {
    beforeUpload: (file) => {
      setFileList([file]);
      return false;
    },
    onRemove: (file) => {
      setFileList([]);
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
    { 
      title: 'R2 Prefix', 
      dataIndex: 'r2_prefix', 
      key: 'r2_prefix',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
    { 
      title: 'Download Mode', 
      dataIndex: 'download_mode', 
      key: 'download_mode',
      width: 120,
      render: (mode) => <Tag color="blue">{mode.toUpperCase()}</Tag>,
      responsive: ['md']
    },
    { 
      title: 'Strategy', 
      dataIndex: 'video_selection_strategy', 
      key: 'video_selection_strategy',
      width: 120,
      render: (mode) => <Tag color="purple">{mode ? mode.toUpperCase() : 'HIGHEST'}</Tag>,
      responsive: ['md']
    },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      width: 100,
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    { 
      title: 'Total', 
      dataIndex: 'total_count', 
      key: 'total_count',
      width: 70,
      responsive: ['sm']
    },
    { 
      title: 'Pending', 
      dataIndex: 'pending_count', 
      key: 'pending_count',
      width: 80,
      responsive: ['sm']
    },
    { 
      title: 'Running', 
      dataIndex: 'running_count', 
      key: 'running_count',
      width: 80,
      responsive: ['md']
    },
    { 
      title: 'Success', 
      dataIndex: 'success_count', 
      key: 'success_count',
      width: 80,
      responsive: ['md']
    },
    { 
      title: 'Failed', 
      dataIndex: 'failed_count', 
      key: 'failed_count',
      width: 80,
      responsive: ['md']
    },
    { 
      title: 'Action',
      key: 'action',
      width: 200,
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
        <Space>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setIsModalOpen(true); setFileList([]); }}>
            New YouTube Job
          </Button>
          <Popconfirm 
            title="Are you sure delete ALL pending jobs?"  
            onConfirm={handleDeletePending} 
            okText="Yes" 
            cancelText="No"
          >
            <Button danger>Delete Pending</Button>
          </Popconfirm>
        </Space>
        <Button icon={<ReloadOutlined />} onClick={() => fetchJobs(pagination.current, pagination.pageSize)}>Refresh</Button>
      </div>
      
      <Table 
        columns={columns} 
        dataSource={jobs} 
        rowKey="id" 
        loading={loading}
        scroll={{ x: 'max-content' }}
        pagination={{
          current: pagination.current,
          pageSize: pagination.pageSize,
          total: pagination.total,
          onChange: (page, pageSize) => handleTableChange(page, pageSize),
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} jobs`
        }}
      />

      <Modal 
        title="Create YouTube Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => { setIsModalOpen(false); form.resetFields(); setFileList([]); }}
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
            name="video_selection_strategy"
            label="Video Selection Strategy"
            initialValue="highest_quality"
          >
            <Select>
                <Select.Option value="highest_quality">Highest Quality (Default)</Select.Option>
                <Select.Option value="hd_priority">HD Priority (Prefer 1080p/720p)</Select.Option>
                <Select.Option value="best_1080p_plus">Best 1080P+ (Fail if &lt; 1080P)</Select.Option>
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
