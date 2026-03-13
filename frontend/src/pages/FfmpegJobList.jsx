import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Tag, message, Space, Popconfirm, Descriptions, Checkbox } from 'antd';
import { ReloadOutlined, PlusOutlined, DeleteOutlined, EyeOutlined, SyncOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

const FfmpegJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [metadataList, setMetadataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [detailVisible, setDetailVisible] = useState(false);
  const [selectedJob, setSelectedJob] = useState(null);
  const [form] = Form.useForm();

  const fetchJobs = async (background = false) => {
    if (!background) setLoading(true);
    try {
      const res = await api.get('/ffmpeg-jobs/');
      setJobs(res.data);
    } catch (error) {
      if (!background) message.error('Failed to load jobs');
    } finally {
      if (!background) setLoading(false);
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
    
    const interval = setInterval(() => {
      fetchJobs(true);
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      await api.post('/ffmpeg-jobs/', values);
      message.success('Job created');
      setIsModalOpen(false);
      fetchJobs();
    } catch (error) {
      console.error(error);
      message.error('Failed to create job');
    }
  };

  const handleDelete = async (jobId) => {
    try {
      await api.delete(`/ffmpeg-jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs();
    } catch (error) {
      message.error('Failed to delete job');
    }
  };

  const handleReconcile = async (jobId) => {
    try {
      await api.post('/tasks/reconcile', {
        job_id: jobId,
        type: 'ffmpeg'
      });
      message.success('Reconciliation started');
    } catch (error) {
      message.error('Failed to start reconciliation');
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
    { title: 'Metadata ID', dataIndex: 'metadata_id', key: 'metadata_id', width: 100 },
    { title: 'S3 Prefix', dataIndex: 's3_prefix', key: 's3_prefix' },
    { 
      title: 'Inc', 
      dataIndex: 'is_incremental', 
      key: 'is_incremental',
      width: 60,
      render: (val) => val ? <Tag color="blue">Yes</Tag> : <Tag>No</Tag>
    },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    { 
      title: 'Success', 
      dataIndex: 'success_count', 
      key: 'success_count' 
    },
    { 
      title: 'Failed', 
      dataIndex: 'failed_count', 
      key: 'failed_count' 
    },
    { 
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space>
          <Button 
            icon={<EyeOutlined />} 
            size="small" 
            onClick={() => { setSelectedJob(record); setDetailVisible(true); }}
          >
            Details
          </Button>
          <Popconfirm 
            title="Are you sure delete this job?" 
            onConfirm={() => handleDelete(record.id)} 
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
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setIsModalOpen(true); }}>
          New FFmpeg Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={() => fetchJobs(false)}>Refresh</Button>
      </div>
      
      <Table columns={columns} dataSource={jobs} rowKey="id" loading={loading} />

      <Modal 
        title="Create FFmpeg Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="metadata_id" label="S3 Config (Metadata)" rules={[{ required: true }]}>
            <Select>
              {metadataList.map(m => (
                <Option key={m.id} value={m.id}>{m.client_name} ({m.endpoint})</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="s3_prefix" label="S3 Prefix (Folder)" rules={[{ required: true }]} tooltip="Folder containing {id}_video.ext and {id}_audio.ext">
            <Input placeholder="raw_uploads/" />
          </Form.Item>
          <Form.Item name="s3_upload_prefix" label="S3 Upload Prefix (Optional)" tooltip="Folder to upload output files. Defaults to source folder if empty.">
            <Input placeholder="processed/" />
          </Form.Item>
          <Form.Item name="is_incremental" valuePropName="checked">
            <Checkbox>Incremental Mode (Continuously scan for new files)</Checkbox>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Job Details"
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailVisible(false)}>Close</Button>,
          selectedJob && (
            <Button key="reconcile" icon={<SyncOutlined />} onClick={() => handleReconcile(selectedJob.id)}>Reconcile Stats</Button>
          )
        ]}
        width={700}
      >
        {selectedJob && (
          <Descriptions column={1} bordered>
            <Descriptions.Item label="Job ID">{selectedJob.id}</Descriptions.Item>
            <Descriptions.Item label="Metadata ID">{selectedJob.metadata_id}</Descriptions.Item>
            <Descriptions.Item label="S3 Prefix">{selectedJob.s3_prefix}</Descriptions.Item>
            <Descriptions.Item label="S3 Upload Prefix">{selectedJob.s3_upload_prefix}</Descriptions.Item>
            <Descriptions.Item label="Incremental">{selectedJob.is_incremental ? 'Yes' : 'No'}</Descriptions.Item>
            <Descriptions.Item label="Status">
              <Tag color={statusColors[selectedJob.status]}>{selectedJob.status}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Pending Count">{selectedJob.pending_count}</Descriptions.Item>
            <Descriptions.Item label="Running Count">{selectedJob.running_count}</Descriptions.Item>
            <Descriptions.Item label="Success Count">{selectedJob.success_count}</Descriptions.Item>
            <Descriptions.Item label="Failed Count">{selectedJob.failed_count}</Descriptions.Item>
            <Descriptions.Item label="Created At">{selectedJob.created_at}</Descriptions.Item>
            <Descriptions.Item label="Updated At">{selectedJob.updated_at}</Descriptions.Item>
          </Descriptions>
        )}
      </Modal>
    </div>
  );
};

export default FfmpegJobList;
