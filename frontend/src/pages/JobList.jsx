import React, { useEffect, useMemo, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Checkbox, Tag, message, Space, Popconfirm, Card, Row, Col } from 'antd';
import { PlayCircleOutlined, StopOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

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

const chartColors = ['#1677ff', '#52c41a', '#faad14', '#eb2f96', '#13c2c2', '#722ed1', '#fa541c', '#2f54eb', '#a0d911', '#f5222d'];

const buildConicGradient = (items, valueKey) => {
  const total = items.reduce((sum, item) => sum + (item[valueKey] || 0), 0);
  if (total <= 0) return '#f0f0f0';
  let start = 0;
  const parts = items.map((item, index) => {
    const value = item[valueKey] || 0;
    const percent = (value / total) * 100;
    const end = start + percent;
    const color = chartColors[index % chartColors.length];
    const segment = `${color} ${start.toFixed(2)}% ${end.toFixed(2)}%`;
    start = end;
    return segment;
  });
  return `conic-gradient(${parts.join(',')})`;
};

const PiePanel = ({ title, data, valueKey, formatter }) => {
  const total = data.reduce((sum, item) => sum + (item[valueKey] || 0), 0);
  return (
    <Card size="small" title={title} bodyStyle={{ padding: 12 }}>
      {total <= 0 ? (
        <div style={{ textAlign: 'center', color: '#999', padding: '24px 0' }}>暂无数据</div>
      ) : (
        <div style={{ display: 'flex', gap: 16 }}>
          <div
            style={{
              width: 160,
              height: 160,
              borderRadius: '50%',
              background: buildConicGradient(data, valueKey),
              flexShrink: 0
            }}
          />
          <div style={{ minWidth: 0, flex: 1, maxHeight: 180, overflow: 'auto' }}>
            {data.map((item, index) => {
              const value = item[valueKey] || 0;
              const percent = total > 0 ? ((value / total) * 100).toFixed(1) : '0.0';
              return (
                <div key={`${item.label}-${index}`} style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                  <span style={{ width: 10, height: 10, background: chartColors[index % chartColors.length], borderRadius: 2, marginRight: 8 }} />
                  <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={item.label}>{item.label}</span>
                  <span style={{ marginLeft: 8, color: '#666' }}>{formatter(value)} ({percent}%)</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </Card>
  );
};

const DailyBarPanel = ({ title, data }) => {
  const maxCount = Math.max(1, ...data.map(item => item.count));
  const maxSize = Math.max(1, ...data.map(item => item.size));
  return (
    <Card size="small" title={title} bodyStyle={{ padding: 12 }}>
      {data.length === 0 ? (
        <div style={{ textAlign: 'center', color: '#999', padding: '24px 0' }}>暂无数据</div>
      ) : (
        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, height: 220, overflowX: 'auto' }}>
          {data.map(item => (
            <div key={item.date} style={{ minWidth: 48, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
              <div style={{ height: 170, display: 'flex', alignItems: 'flex-end', gap: 4 }}>
                <div
                  title={`数量 ${item.count}`}
                  style={{ width: 14, height: `${Math.max(4, (item.count / maxCount) * 160)}px`, background: '#1677ff', borderRadius: '4px 4px 0 0' }}
                />
                <div
                  title={`大小 ${formatBytes(item.size)}`}
                  style={{ width: 14, height: `${Math.max(4, (item.size / maxSize) * 160)}px`, background: '#52c41a', borderRadius: '4px 4px 0 0' }}
                />
              </div>
              <div style={{ fontSize: 12, color: '#999', marginTop: 6 }}>{item.date.slice(5)}</div>
            </div>
          ))}
        </div>
      )}
      <div style={{ marginTop: 8, display: 'flex', gap: 16, color: '#666' }}>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#1677ff', marginRight: 6 }} />数量</span>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#52c41a', marginRight: 6 }} />大小</span>
      </div>
    </Card>
  );
};

const JobList = () => {
  const [jobs, setJobs] = useState([]);
  const [statsJobs, setStatsJobs] = useState([]);
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

  const getMetadataName = (record) => {
    if (record?.metadata?.client_name) {
      return record.metadata.client_name;
    }
    const matched = metadataList.find(m => m.id === record?.metadata_id);
    return matched?.client_name || '-';
  };

  const getDestSecondary = (dstDir) => {
    if (!dstDir) return '-';
    const parts = dstDir.split('/').filter(Boolean);
    if (parts.length >= 2) return `${parts[0]}/${parts[1]}`;
    return parts[0] || '-';
  };

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

  const fetchStatsJobs = async () => {
    try {
      const all = [];
      const limit = 200;
      let page = 1;
      let total = 0;
      while (true) {
        const res = await api.get(`/jobs/?page=${page}&limit=${limit}`);
        const list = Array.isArray(res.data) ? res.data : [];
        all.push(...list);
        total = Number(res.headers?.['x-total-count'] || 0);
        if (list.length < limit || (total > 0 && all.length >= total) || page >= 50) {
          break;
        }
        page += 1;
      }
      setStatsJobs(all);
    } catch (error) {
      setStatsJobs([]);
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
    fetchStatsJobs();
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
      fetchStatsJobs();
    } catch (error) {
      console.error(error);
    }
  };

  const handleAction = async (jobId, action) => {
    try {
      await api.post(`/jobs/${jobId}/${action}`);
      message.success(`Job ${action}ed`);
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsJobs();
    } catch (error) {
      message.error(`Failed to ${action} job`);
    }
  };

  const handleDelete = async (jobId) => {
    try {
      await api.delete(`/jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsJobs();
    } catch (error) {
      message.error('Failed to delete job');
    }
  };

  const handleDuplicateJob = async () => {
    if (!selectedJob) return;
    try {
      const payload = {
        metadata_id: selectedJob.metadata_id,
        src_dir: selectedJob.src_dir,
        dst_dir: selectedJob.dst_dir,
        include: selectedJob.include || '',
        exclude: selectedJob.exclude || '',
        delete_source: !!selectedJob.delete_source,
        is_incremental: !!selectedJob.is_incremental
      };
      if (selectedJob.is_incremental) {
        payload.periodic_interval = selectedJob.periodic_interval > 0 ? selectedJob.periodic_interval : 600;
      }
      const res = await api.post('/jobs/', payload);
      message.success(`复制任务已创建（ID: ${res.data?.job_id || '-' }）`);
      setDetailVisible(false);
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsJobs();
    } catch (error) {
      message.error('复制任务创建失败');
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

  const metadataNameMap = useMemo(() => {
    const map = new Map();
    metadataList.forEach(item => {
      map.set(item.id, item.client_name || `meta-${item.id}`);
    });
    return map;
  }, [metadataList]);

  const chartData = useMemo(() => {
    const source = statsJobs.length > 0 ? statsJobs : jobs;
    const byMeta = new Map();
    const byDest = new Map();
    const byDay = new Map();

    source.forEach(job => {
      const metaId = job.metadata_id;
      const metaName = job?.metadata?.client_name || metadataNameMap.get(metaId) || `meta-${metaId ?? '-'}`;
      const size = Number(job.success_size_bytes || 0);
      const count = 1;

      const metaKey = `${metaId || '-'}|${metaName}`;
      if (!byMeta.has(metaKey)) byMeta.set(metaKey, { label: metaName, count: 0, size: 0 });
      const metaAgg = byMeta.get(metaKey);
      metaAgg.count += count;
      metaAgg.size += size;

      const dest2 = getDestSecondary(job.dst_dir);
      const destLabel = `${metaName} | ${dest2}`;
      if (!byDest.has(destLabel)) byDest.set(destLabel, { label: destLabel, count: 0, size: 0 });
      const destAgg = byDest.get(destLabel);
      destAgg.count += count;
      destAgg.size += size;

      const day = (job.created_at || '').slice(0, 10) || '-';
      if (!byDay.has(day)) byDay.set(day, { date: day, count: 0, size: 0 });
      const dayAgg = byDay.get(day);
      dayAgg.count += count;
      dayAgg.size += size;
    });

    const toTop = (arr) => arr.sort((a, b) => b.size - a.size).slice(0, 10);
    const daily = Array.from(byDay.values())
      .filter(item => item.date !== '-')
      .sort((a, b) => a.date.localeCompare(b.date))
      .slice(-14);

    return {
      meta: toTop(Array.from(byMeta.values())),
      dest: toTop(Array.from(byDest.values())),
      daily
    };
  }, [jobs, statsJobs, metadataNameMap]);

  const columns = [
    { title: 'ID', dataIndex: 'job_id', key: 'job_id', width: 60 },
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
      <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
        <Col xs={24} xl={12}>
          <PiePanel title="按 Metadata 展示 Transfer 数量" data={chartData.meta} valueKey="count" formatter={(value) => `${value}`} />
        </Col>
        <Col xs={24} xl={12}>
          <PiePanel title="按 Metadata 展示 Transfer 大小" data={chartData.meta} valueKey="size" formatter={formatBytes} />
        </Col>
        <Col xs={24} xl={12}>
          <PiePanel title="按 Destination 二级目录展示 Transfer 数量" data={chartData.dest} valueKey="count" formatter={(value) => `${value}`} />
        </Col>
        <Col xs={24} xl={12}>
          <PiePanel title="按 Destination 二级目录展示 Transfer 大小" data={chartData.dest} valueKey="size" formatter={formatBytes} />
        </Col>
        <Col span={24}>
          <DailyBarPanel title="每天任务 Transfer 数量与大小（近14天）" data={chartData.daily} />
        </Col>
      </Row>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setIsModalOpen(true); }}>
          New Transfer Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={() => { fetchJobs(pagination.current, pagination.pageSize); fetchStatsJobs(); }}>Refresh</Button>
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
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {selectedJob && (
              <Button type="primary" size="small" onClick={handleDuplicateJob}>新建复制任务</Button>
            )}
            <span>Job Details</span>
          </div>
        }
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
              { label: 'Client/Metadata Name', value: getMetadataName(selectedJob) },
              { label: 'Source', value: selectedJob.src_dir, fullWidth: true },
              { label: 'Destination', value: selectedJob.dst_dir, fullWidth: true },
              { label: 'Include', value: selectedJob.include || '-' },
              { label: 'Exclude', value: selectedJob.exclude || '-' },
              { label: 'Delete Source', value: selectedJob.delete_source ? 'Yes' : 'No' },
              { label: 'Incremental', value: selectedJob.is_incremental ? 'Yes' : 'No' },
              ...(selectedJob.is_incremental ? [{ label: 'Periodic Interval', value: `${selectedJob.periodic_interval} s` }] : []),
              { label: 'Status', value: <Tag color={statusColors[selectedJob.status]}>{selectedJob.status}</Tag> },
              { label: 'Success Size', value: formatBytes(selectedJob.success_size_bytes) },
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
