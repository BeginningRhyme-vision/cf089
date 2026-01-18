import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Checkbox, Tag, message, Space, Popconfirm, Descriptions } from 'antd';
import { PlayCircleOutlined, PauseCircleOutlined, StopOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

const cellStyle = {
  maxWidth: 180,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  display: 'inline-block' 
};

const JobList = () => {
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

  const fetchJobs = async (page = 1, pageSize = 10) => {
    setLoading(true);
    try {
      const res = await api.get(`/jobs/?page=${page}&limit=${pageSize}`);
      setJobs(res.data);
      
      if (res.headers && res.headers['x-total-count']) {
        setPagination(prev => ({
          ...prev,
          current: page,
          pageSize: pageSize,
          total: parseInt(res.headers['x-total-count'])
        }));
      } else {
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

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const payload = {
        ...values,
        delete_source: !!values.delete_source,
        is_incremental: !!values.is_incremental
      };
      await api.post('/jobs/', payload);
      message.success('Job created');
      setIsModalOpen(false);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      console.error(error);
    }
  };

  const handleAction = async (jobId, action) => {
    try {
      await api.post(`/jobs/${jobId}/${action}`);
      message.success(`Job ${action}ed`);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error(`Failed to ${action} job`);
    }
  };

  const handleDelete = async (jobId) => {
    try {
      await api.delete(`/jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to delete job');
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
    { title: 'Metadata ID', dataIndex: 'metadata_id', key: 'metadata_id', width: 100, responsive: ['md'] },
    { 
      title: 'Source', 
      dataIndex: 'src_dir', 
      key: 'src_dir',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      ) 
    },
    { 
      title: 'Dest', 
      dataIndex: 'dst_dir', 
      key: 'dst_dir',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
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
          <Button 
            icon={<EyeOutlined />} 
            size="small" 
            onClick={() => { setSelectedJob(record); setDetailVisible(true); }}
          >
            Details
          </Button>
          {(record.status === 'PENDING' || record.status === 'PAUSED' || record.status === 'STOPPED' || record.status === 'FAILED') && (
            <Button icon={<PlayCircleOutlined />} size="small" onClick={() => handleAction(record.job_id, 'start')}>Start</Button>
          )}
          {record.status === 'RUNNING' && (
            <Button icon={<StopOutlined />} size="small" danger onClick={() => handleAction(record.job_id, 'stop')}>Stop</Button>
          )}
          {record.failed_count > 0 && (
            <Button icon={<ReloadOutlined />} size="small" onClick={() => handleAction(record.job_id, 'retry')}>Retry Failed</Button>
          )}
          <Popconfirm 
            title="Are you sure delete this job?"  
            onConfirm={() => handleDelete(record.job_id)} 
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
          New Transfer Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={() => fetchJobs(pagination.current, pagination.pageSize)}>Refresh</Button>
      </div>
      
      <Table 
        columns={columns} 
        dataSource={jobs} 
        rowKey="job_id" 
        loading={loading}
        scroll={{ x: 'max-content' }}
        pagination={{
          current: pagination.current,
          pageSize: pagination.pageSize,
          total: pagination.total,
          onChange: (page, pageSize) => setPagination(prev => ({ ...prev, current: page, pageSize })),
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} jobs`
        }}
      />

      <Modal 
        title="Create Transfer Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical" initialValues={{ delete_source: false, is_incremental: false }}>
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
          <Form.Item name="include" label="Include Pattern (Glob)" tooltip="Example: *.jpg">
            <Input placeholder="*.jpg" />
          </Form.Item>
          <Form.Item name="exclude" label="Exclude Pattern (Glob)" tooltip="Example: temp/*">
            <Input placeholder="temp/*" />
          </Form.Item>
          <Form.Item name="delete_source" valuePropName="checked">
            <Checkbox>Delete source files after transfer</Checkbox>
          </Form.Item>
          <Form.Item name="is_incremental" valuePropName="checked">
            <Checkbox>Incremental Transfer (Continuous Sync)</Checkbox>
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
                  <Input type="number" />
                </Form.Item>
              ) : null
            }
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Job Details"
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailVisible(false)}>Close</Button>,
          selectedJob && selectedJob.failed_count > 0 && (
            <Button key="retry" icon={<ReloadOutlined />} onClick={() => handleAction(selectedJob.job_id, 'retry')}>Retry Failed</Button>
          )
        ]}
        width={700}
      >
        {selectedJob && (
          <div style={{ border: '1px solid #f0f0f0' }}>
            {[
              { label: 'Job ID', value: selectedJob.job_id },
              { label: 'Client/Metadata ID', value: selectedJob.metadata_id },
              { label: 'Source', value: selectedJob.src_dir, fullWidth: true },
              { label: 'Destination', value: selectedJob.dst_dir, fullWidth: true },
              { label: 'Include', value: selectedJob.include || '-' },
              { label: 'Exclude', value: selectedJob.exclude || '-' },
              { label: 'Delete Source', value: selectedJob.delete_source ? 'Yes' : 'No' },
              { label: 'Incremental', value: selectedJob.is_incremental ? 'Yes' : 'No' },
              ...(selectedJob.is_incremental ? [{ label: 'Periodic Interval', value: `${selectedJob.periodic_interval} s` }] : []),
              { label: 'Status', value: <Tag color={statusColors[selectedJob.status]}>{selectedJob.status}</Tag> },
              { label: 'Total Count', value: selectedJob.total_count },
              { label: 'Pending Count', value: selectedJob.pending_count },
              { label: 'Running Count', value: selectedJob.running_count },
              { label: 'Success Count', value: selectedJob.success_count },
              { label: 'Failed Count', value: selectedJob.failed_count },
              { label: 'Start Time', value: selectedJob.start_time || '-' },
              { label: 'End Time', value: selectedJob.end_time || '-' },
              { label: 'Duration', value: `${selectedJob.duration_seconds} seconds` },
              { label: 'Execution Count', value: selectedJob.execution_count },
              { label: 'Result Message', value: selectedJob.result_message || 'N/A', fullWidth: true },
              { label: 'Created At', value: selectedJob.created_at },
              { label: 'Updated At', value: selectedJob.updated_at },
            ].map((item, index) => (
              <div 
                key={index}
                style={{ 
                  display: 'flex', 
                  borderBottom: (index < 21) ? '1px solid #f0f0f0' : 'none',
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

export default JobList;
