import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Tag, message, Space, Popconfirm, Descriptions, Checkbox, Alert } from 'antd';
import { ReloadOutlined, PlusOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

const cellStyle = {
  maxWidth: 250,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  display: 'inline-block' 
};

const formatBytes = (bytes) => {
  if (!bytes || bytes <= 0) return '-';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
};

const FfmpegJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [metadataList, setMetadataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [detailVisible, setDetailVisible] = useState(false);
  const [selectedJob, setSelectedJob] = useState(null);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
    total: 0
  });
  const [form] = Form.useForm();

  const normalizeEndpointHost = (endpoint = '') => {
    const text = String(endpoint || '').trim().toLowerCase();
    if (!text) return '';
    const noScheme = text.includes('://') ? text.split('://')[1] : text;
    return noScheme.split('/')[0];
  };

  const isInternalEndpoint = (endpoint = '') => {
    const endpointText = String(endpoint || '').trim().toLowerCase();
    const host = normalizeEndpointHost(endpointText);
    const isAliInternal = endpointText.includes('aliyuncs.com') && host.includes('internal');
    const isVolcInternal = (endpointText.includes('ivolces.com') || endpointText.includes('volces.com')) && host.includes('tos-s3-');
    return isAliInternal || isVolcInternal;
  };

  const getMetadataById = (metadataId) => metadataList.find(m => m.id === metadataId);

  const confirmIfPublicEndpoint = async (metadataId) => {
    const metadata = getMetadataById(metadataId);
    if (!metadata) return true;
    if (isInternalEndpoint(metadata.endpoint)) return true;
    return new Promise((resolve) => {
      Modal.confirm({
        title: 'Public Endpoint Warning',
        content: 'Selected metadata endpoint is not internal. This may cause high public network traffic costs. Continue?',
        okText: 'Continue',
        cancelText: 'Cancel',
        okType: 'danger',
        onOk: () => resolve(true),
        onCancel: () => resolve(false)
      });
    });
  };

  const warnIfPublicEndpointSelected = (metadataId) => {
    const metadata = getMetadataById(metadataId);
    if (!metadata) return;
    if (isInternalEndpoint(metadata.endpoint)) return;
    Modal.warning({
      title: 'Public Endpoint Warning',
      content: `Selected metadata endpoint is not internal and may cause high public network traffic costs.\n${metadata.endpoint}`,
      okText: 'Got it'
    });
  };

  const getMetadataName = (record) => {
    if (record?.metadata?.client_name) {
      return record.metadata.client_name;
    }
    const matched = metadataList.find(m => m.id === record?.metadata_id);
    return matched?.client_name || '-';
  };

  const fetchJobs = async (page = 1, pageSize = 10) => {
    setLoading(true);
    try {
      const res = await api.get(`/ffmpeg-jobs/?page=${page}&limit=${pageSize}`);
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

  const fetchMetadata = async () => {
    try {
      const res = await api.get('/metadata/');
      setMetadataList(res.data);
    } catch (error) {
      console.error(error);
    }
  };

  useEffect(() => {
    fetchJobs(pagination.current, pagination.pageSize);
    fetchMetadata();
  }, [pagination.current, pagination.pageSize]);

  const handleTableChange = (page, pageSize) => {
    setPagination({
      ...pagination,
      current: page,
      pageSize: pageSize
    });
  };

  const createJob = async (payload) => {
    await api.post('/ffmpeg-jobs/', payload);
    message.success('Job created');
    setIsModalOpen(false);
    fetchJobs(pagination.current, pagination.pageSize);
  };

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const shouldContinue = await confirmIfPublicEndpoint(values.metadata_id);
      if (!shouldContinue) return;
      await createJob(values);
    } catch (error) {
      console.error(error);
      message.error('Failed to create job');
    }
  };

  const handleDuplicateJob = async () => {
    if (!selectedJob) return;
    try {
      const payload = {
        metadata_id: selectedJob.metadata_id,
        s3_prefix: selectedJob.s3_prefix,
        s3_upload_prefix: selectedJob.s3_upload_prefix,
        is_incremental: !!selectedJob.is_incremental
      };
      if (selectedJob.is_incremental) {
        payload.periodic_interval = selectedJob.periodic_interval > 0 ? selectedJob.periodic_interval : 600;
      }
      const shouldContinue = await confirmIfPublicEndpoint(payload.metadata_id);
      if (!shouldContinue) return;
      const res = await api.post('/ffmpeg-jobs/', payload);
      message.success(`New Copy Job created (ID: ${res.data?.id || '-'})`);
      setDetailVisible(false);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to create copy job');
    }
  };

  const handleDelete = async (jobId) => {
    try {
      await api.delete(`/ffmpeg-jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to delete job');
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
    { title: 'Metadata ID', dataIndex: 'metadata_id', key: 'metadata_id', width: 100, responsive: ['md'] },
    {
      title: 'Metadata Name',
      key: 'metadata_name',
      width: 180,
      render: (_, record) => (
        <div style={cellStyle} title={getMetadataName(record)}>
          {getMetadataName(record)}
        </div>
      ),
      responsive: ['md']
    },
    { 
      title: 'S3 Prefix', 
      dataIndex: 's3_prefix', 
      key: 's3_prefix',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      ) 
    },
    { 
      title: 'Inc', 
      dataIndex: 'is_incremental', 
      key: 'is_incremental',
      width: 60,
      render: (val) => val ? <Tag color="blue">Yes</Tag> : <Tag>No</Tag>,
      responsive: ['md']
    },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    {
      title: 'Success Size',
      dataIndex: 'success_size_bytes',
      key: 'success_size_bytes',
      width: 120,
      render: (value) => formatBytes(value),
      responsive: ['md']
    },
    { 
      title: 'Success', 
      dataIndex: 'success_count', 
      key: 'success_count',
      responsive: ['md']
    },
    { 
      title: 'Failed', 
      dataIndex: 'failed_count', 
      key: 'failed_count',
      responsive: ['md']
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
        title="Create FFmpeg Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Alert
            type="error"
            showIcon
            message="高风险提醒：请务必使用内网 Endpoint，否则会产生巨额公网流量费用。"
            description="阿里云内网判断：Endpoint 需包含 aliyuncs.com 且 host 包含 internal。火山云内网判断：Endpoint 需包含 ivolces.com 且 host 包含 tos-s3-。"
            style={{ marginBottom: 12 }}
          />
          <Form.Item name="metadata_id" label="Client/Metadata" rules={[{ required: true }]}>
            <Select onChange={warnIfPublicEndpointSelected}>
              {metadataList.map(m => (
                <Option key={m.id} value={m.id}>{m.client_name} ({m.endpoint})</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="s3_prefix" label="S3 Prefix (Source)" rules={[{ required: true }]}>
            <Input placeholder="e.g., bucket/prefix/" />
          </Form.Item>
          <Form.Item name="s3_upload_prefix" label="S3 Upload Prefix (Destination)" rules={[{ required: true }]}>
            <Input placeholder="e.g., bucket/output_path/" />
          </Form.Item>
          <Form.Item name="is_incremental" valuePropName="checked" label="Incremental Mode">
            <Checkbox>Enable incremental processing (continuous scan)</Checkbox>
          </Form.Item>
          <Form.Item
            noStyle
            shouldUpdate={(prev, current) => prev.is_incremental !== current.is_incremental}
          >
            {({ getFieldValue }) =>
              getFieldValue('is_incremental') ? (
                <Form.Item
                  name="periodic_interval"
                  label="Periodic Interval (Seconds)"
                  rules={[{ required: true, message: 'Please set interval' }]}
                  initialValue={600}
                >
                  <Input type="number" placeholder="600" />
                </Form.Item>
              ) : null
            }
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {selectedJob && (
              <Button type="primary" size="small" onClick={handleDuplicateJob}>New Copy Job</Button>
            )}
            <span>Job Details</span>
          </div>
        }
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailVisible(false)}>Close</Button>
        ]}
        width={700}
      >
        {selectedJob && (
          <div style={{ border: '1px solid #f0f0f0' }}>
            {[
              { label: 'Job ID', value: selectedJob.id },
              { label: 'Metadata ID', value: selectedJob.metadata_id },
              { label: 'Metadata Name', value: getMetadataName(selectedJob) },
              { label: 'S3 Prefix', value: selectedJob.s3_prefix, fullWidth: true },
              { label: 'S3 Upload Prefix', value: selectedJob.s3_upload_prefix, fullWidth: true },
              { label: 'Is Incremental', value: selectedJob.is_incremental ? 'Yes' : 'No' },
              { label: 'Periodic Interval', value: selectedJob.periodic_interval > 0 ? `${selectedJob.periodic_interval}s` : 'N/A' },
              { label: 'Status', value: <Tag color={statusColors[selectedJob.status]}>{selectedJob.status}</Tag> },
              { label: 'Success Size', value: formatBytes(selectedJob.success_size_bytes) },
              { label: 'Total Count', value: selectedJob.total_count },
              { label: 'Pending Count', value: selectedJob.pending_count },
              { label: 'Running Count', value: selectedJob.running_count },
              { label: 'Success Count', value: selectedJob.success_count },
              { label: 'Failed Count', value: selectedJob.failed_count },
              { label: 'Created At', value: selectedJob.created_at },
              { label: 'Updated At', value: selectedJob.updated_at },
            ].map((item, index, arr) => (
              <div 
                key={index}
                style={{ 
                  display: 'flex', 
                  borderBottom: (index < arr.length - 1) ? '1px solid #f0f0f0' : 'none',
                  flexWrap: 'wrap'
                }}
              >
                <div style={{
                  width: '100%',
                  padding: '12px 16px',
                  fontWeight: 'bold',
                  background: '#fafafa',
                  borderRight: '1px solid #f0f0f0',
                  flex: '0 0 150px'
                }}>
                  {item.label}
                </div>
                <div style={{
                  width: '100%',
                  padding: '12px 16px',
                  flex: 1,
                  overflowWrap: 'break-word',
                  wordWrap: 'break-word',
                }}>
                  {item.value}
                </div>
              </div>
            ))}
          </div>
        )}
      </Modal>
    </div>
  );
};

export default FfmpegJobList;
