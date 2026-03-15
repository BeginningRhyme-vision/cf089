import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Tag, message, Space, Upload, Popconfirm, Card, Row, Col } from 'antd';
import { ReloadOutlined, PlusOutlined, EyeOutlined, UploadOutlined, DeleteOutlined, UndoOutlined } from '@ant-design/icons';
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

const YoutubeJobList = () => {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [fileList, setFileList] = useState([]);
  const [machineNames, setMachineNames] = useState([]);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
    total: 0
  });
  const [form] = Form.useForm();
  const navigate = useNavigate();
  const [submitDisabled, setSubmitDisabled] = useState(false);
  const [submitDisabledTime, setSubmitDisabledTime] = useState(0);


  const parseTaskInput = (raw) => {
    if (!raw) return [];
    return raw
      .split(/[\n\r\t ,;|]+/)
      .map((v) => v.trim())
      .filter((v) => v.length > 0);
  };
  const [queueStats, setQueueStats] = useState(null);

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

  const fetchQueueStats = async () => {
    try {
      const res = await api.get('/youtube-jobs/queue-stats');
      console.log('Queue stats response:', res.data);
      setQueueStats(res.data);
    } catch (error) {
      console.error('Failed to load queue stats:', error);
      setQueueStats({ error: true, stats: [] });
    }
  };

  useEffect(() => {
    fetchJobs(pagination.current, pagination.pageSize);
    fetchMachineNames();
    fetchQueueStats();
  }, [pagination.current, pagination.pageSize]);

  const handleTableChange = (page, pageSize) => {
    setPagination(prev => ({
      ...prev,
      current: page,
      pageSize: pageSize
    }));
  };

  const fetchMachineNames = async () => {
    try {
      const res = await api.get('/worker-cookie-configs/machine-names');
      if (res.data && res.data.machine_names) {
        setMachineNames(res.data.machine_names);
      }
    } catch (error) {
      // 如果获取失败，不影响主流程，只是没有主机名列表
      console.warn('Failed to fetch machine names:', error);
    }
  };

  const handleCreate = async () => {
    // 检查是否在冷却期内
    if (submitDisabled) {
      const remainingSeconds = Math.ceil((60000 - (Date.now() - submitDisabledTime)) / 1000);
      message.warning(`Please wait ${remainingSeconds} seconds before submitting again`);
      return;
    }

    try {
      const values = await form.validateFields();
      
      // 禁用提交按钮，设置1分钟冷却期
      setSubmitDisabled(true);
      setSubmitDisabledTime(Date.now());
      
      // 设置定时器，1分钟后重新启用
      setTimeout(() => {
        setSubmitDisabled(false);
        setSubmitDisabledTime(0);
      }, 60000); // 60秒 = 1分钟
      
      // If file is present, use Multipart/Form-Data
      if (fileList.length > 0) {
        const formData = new FormData();
        formData.append('r2_prefix', values.r2_prefix);
        formData.append('download_mode', values.download_mode || 'both');
        formData.append('video_selection_strategy', values.video_selection_strategy || 'highest_quality');
        if (values.machine_name) {
          formData.append('machine_name', values.machine_name);
        }
        
        // 获取文件对象（Ant Design Upload 组件的文件对象结构）
        const file = fileList[0].originFileObj || fileList[0];
        if (!file) {
          message.error('File not found. Please select a file again.');
          setSubmitDisabled(false);
          setSubmitDisabledTime(0);
          return;
        }
        formData.append('file', file);
        
        if (values.file_url) {
            formData.append('file_url', values.file_url);
        }
        if (values.urls) {
            formData.append('tasks', values.urls);
        }

        console.log('Submitting form with file:', file.name, 'size:', file.size);
        await api.post('/youtube-jobs/', formData, {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
          timeout: 300000, // 5分钟超时，因为文件可能很大
        });
      } else {
        // Use JSON
        let tasks = [];
        
        // Parse URLs from text area
        if (values.urls) {
            const textUrls = parseTaskInput(values.urls);
            tasks = [...tasks, ...textUrls];
        }

        if (tasks.length === 0 && !values.file_url) {
            message.error('Please enter URLs, provide a File URL, or upload a file');
            setSubmitDisabled(false);
            setSubmitDisabledTime(0);
            return;
        }

        const payload = {
            r2_prefix: values.r2_prefix,
            download_mode: values.download_mode || 'both',
            video_selection_strategy: values.video_selection_strategy || 'highest_quality',
            tasks: tasks,
            file_url: values.file_url
        };
        if (values.machine_name) {
          payload.machine_name = values.machine_name;
        }

        await api.post('/youtube-jobs/', payload);
      }

      message.success('Job(s) created successfully. Tasks are being added to the queue...');
      setIsModalOpen(false);
      form.resetFields();
      setFileList([]);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      console.error(error);
      
      // 检查是否是验证错误
      if (error.response?.status === 400 && error.response?.data?.validation_errors) {
        const validationData = error.response.data;
        const errors = validationData.validation_errors;
        
        // 构建详细的错误消息
        let errorMsg = `${validationData.error}\n\n`;
        errorMsg += `Total lines: ${validationData.total_lines}\n`;
        errorMsg += `Valid: ${validationData.valid_count}, Errors: ${validationData.error_count}\n\n`;
        errorMsg += 'Validation errors:\n';
        
        errors.forEach((err, index) => {
          errorMsg += `Line ${err.line_number}: ${err.content}\n`;
          errorMsg += `  → ${err.message}\n`;
          if (err.fixed) {
            errorMsg += `  ✓ Auto-fixed to: ${err.fixed_url}\n`;
          }
          if (index < errors.length - 1) {
            errorMsg += '\n';
          }
        });
        
        // 显示错误对话框
        Modal.error({
          title: 'File Format Validation Failed',
          width: 600,
          content: (
            <div style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace', fontSize: '12px' }}>
              {errorMsg}
            </div>
          ),
        });
      } else {
      message.error('Failed to create job: ' + (error.response?.data?.error || error.message));
      }
      
      // 如果出错，立即恢复按钮状态
      setSubmitDisabled(false);
      setSubmitDisabledTime(0);
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

  const handleResetOffset = async (jobId) => {
    try {
      const res = await api.post(`/youtube-jobs/${jobId}/reset-offset`);
      message.success(`Offset reset successfully. Old offset: ${res.data.old_offset}, New offset: ${res.data.new_offset}`);
      fetchJobs(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to reset offset: ' + (error.response?.data?.error || error.message));
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
      title: 'Machine Name', 
      dataIndex: 'machine_name', 
      key: 'machine_name',
      width: 150,
      render: (name) => name ? <Tag color="cyan">{name}</Tag> : <Tag color="default">All</Tag>,
      responsive: ['lg']
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
      title: 'Success Size',
      dataIndex: 'success_size_bytes',
      key: 'success_size_bytes',
      width: 120,
      render: (value) => formatBytes(value),
      responsive: ['md']
    },
    { 
      title: 'Action',
      key: 'action',
      width: 280,
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
            title="Reset offset to 0? This will restart task fetching from the beginning."  
            onConfirm={() => handleResetOffset(record.id)} 
            okText="Yes" 
            cancelText="No"
          >
            <Button 
              icon={<UndoOutlined />} 
              size="small"
              type="default"
            >
              Reset Offset
            </Button>
          </Popconfirm>
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
      {/* Redis 队列统计展示 */}
      <Card 
        title={`Redis 队列统计 ${queueStats && queueStats.total_queues ? `(${queueStats.total_queues} 个队列, ${queueStats.total_machines || 0} 台机器)` : ''}`}
        style={{ marginBottom: 24 }}
        size="small"
        extra={
          <Button 
            size="small" 
            icon={<ReloadOutlined />} 
            onClick={fetchQueueStats}
          >
            刷新
          </Button>
        }
      >
        {!queueStats ? (
          <div style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
            加载中...
          </div>
        ) : queueStats.error ? (
          <div style={{ textAlign: 'center', color: '#ff4d4f', padding: '20px' }}>
            加载队列统计失败，请检查后端接口
          </div>
        ) : queueStats.stats && queueStats.stats.length > 0 ? (
          <div>
            {queueStats.stats
              .sort((a, b) => (a.job_id || 0) - (b.job_id || 0)) // 按 job_id 排序
              .map((stat) => {
                // 对下载队列按队列名称排序
                const sortedDownloadQueues = [...(stat.download_queues || [])].sort((a, b) => {
                  const nameA = a.queue_name || '';
                  const nameB = b.queue_name || '';
                  return nameA.localeCompare(nameB);
                });
                
                // 对元数据队列按队列名称排序
                const sortedMetadataQueues = [...(stat.metadata_queues || [])].sort((a, b) => {
                  const nameA = a.queue_name || '';
                  const nameB = b.queue_name || '';
                  return nameA.localeCompare(nameB);
                });
                
                return (
                  <Card 
                    key={stat.job_id} 
                    size="small" 
                    style={{ marginBottom: 16 }}
                    title={
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span style={{ fontSize: '16px', fontWeight: 'bold' }}>
                          Job #{stat.job_id}
                        </span>
                        <span style={{ fontSize: '14px', color: '#fa8c16', fontWeight: 'bold' }}>
                          总计: {stat.total || 0}
                        </span>
                      </div>
                    }
                  >
                    <Row gutter={[16, 16]}>
                      {/* 下载队列 */}
                      <Col xs={24} md={12}>
                        <div style={{ marginBottom: 12 }}>
                          <div style={{ fontSize: '14px', fontWeight: 'bold', marginBottom: 8, color: '#1890ff' }}>
                            下载队列 (总计: {stat.download_ready || 0})
                          </div>
                          {sortedDownloadQueues.length > 0 ? (
                            <div style={{ paddingLeft: 8 }}>
                              {sortedDownloadQueues.map((queue, idx) => (
                                <div key={idx} style={{ marginBottom: 4, fontSize: '12px', color: '#666' }}>
                                  <span style={{ fontFamily: 'monospace', color: '#1890ff' }}>
                                    {queue.queue_name}
                                  </span>
                                  <span style={{ marginLeft: 8, fontWeight: 'bold' }}>
                                    {queue.count || 0} 个任务
                                  </span>
                                  {queue.machine_name && (
                                    <Tag size="small" style={{ marginLeft: 8 }}>
                                      {queue.machine_name}
                                    </Tag>
                                  )}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div style={{ paddingLeft: 8, color: '#999', fontSize: '12px' }}>无数据</div>
                          )}
                        </div>
                      </Col>
                      
                      {/* 元数据队列 */}
                      <Col xs={24} md={12}>
                        <div style={{ marginBottom: 12 }}>
                          <div style={{ fontSize: '14px', fontWeight: 'bold', marginBottom: 8, color: '#52c41a' }}>
                            元数据队列 (总计: {stat.metadata_retry || 0})
                          </div>
                          {sortedMetadataQueues.length > 0 ? (
                            <div style={{ paddingLeft: 8 }}>
                              {sortedMetadataQueues.map((queue, idx) => (
                                <div key={idx} style={{ marginBottom: 4, fontSize: '12px', color: '#666' }}>
                                  <span style={{ fontFamily: 'monospace', color: '#52c41a' }}>
                                    {queue.queue_name}
                                  </span>
                                  <span style={{ marginLeft: 8, fontWeight: 'bold' }}>
                                    {queue.count || 0} 个任务
                                  </span>
                                  {queue.machine_name && (
                                    <Tag size="small" style={{ marginLeft: 8 }}>
                                      {queue.machine_name}
                                    </Tag>
                                  )}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div style={{ paddingLeft: 8, color: '#999', fontSize: '12px' }}>无数据</div>
                          )}
                        </div>
                      </Col>
                    </Row>
                  </Card>
                );
              })}
          </div>
        ) : (
          <div style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
            暂无队列数据
          </div>
        )}
      </Card>

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
        <Button icon={<ReloadOutlined />} onClick={() => { fetchJobs(pagination.current, pagination.pageSize); fetchQueueStats(); }}>Refresh</Button>
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
        okButtonProps={{ disabled: submitDisabled }}
        okText={submitDisabled ? `Wait ${Math.ceil((60000 - (Date.now() - submitDisabledTime)) / 1000)}s` : 'Create'}
        onCancel={() => { 
          setIsModalOpen(false); 
          form.resetFields(); 
          setFileList([]);
          setSubmitDisabled(false);
          setSubmitDisabledTime(0);
        }}
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
                <Select.Option value="hd_priority">HD Priority (1080P &gt; 720P &gt; 1080P+)</Select.Option>
                <Select.Option value="ultra_priority">Ultra Priority (1080P+ &gt; 1080P)</Select.Option>
                <Select.Option value="min_1080p">Minimum 1080P (Skip if not available)</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item
            name="machine_name"
            label="Machine Name (Optional)"
            help="绑定到特定主机，留空表示所有主机都可以处理"
          >
            <Select
              allowClear
              placeholder="选择主机名或留空（所有主机）"
              showSearch
              filterOption={(input, option) =>
                (option?.children ?? '').toLowerCase().includes(input.toLowerCase())
              }
            >
              {machineNames.map(name => (
                <Select.Option key={name} value={name}>{name}</Select.Option>
              ))}
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
