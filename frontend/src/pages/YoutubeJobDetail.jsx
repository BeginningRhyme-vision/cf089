import React, { useEffect, useState, useCallback } from 'react';
import { Table, Button, Card, Row, Col, Statistic, Tag, Breadcrumb, message } from 'antd';
import { ReloadOutlined, ArrowLeftOutlined } from '@ant-design/icons';
import { useParams, useNavigate, Link } from 'react-router-dom';
import api from '../api';

const modalDetailStyle = {
  overflowWrap: 'break-word',
  wordWrap: 'break-word',
  wordBreak: 'normal',
  whiteSpace: 'normal',
};

const YoutubeJobDetail = () => {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const [job, setJob] = useState(null);
  const [records, setRecords] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50, total: 0 });

  const fetchJob = useCallback(async () => {
    try {
      const res = await api.get(`/youtube-jobs/${jobId}`);
      setJob(res.data);
    } catch (error) {
      console.error('Failed to load job info');
    }
  }, [jobId]);

  const fetchRecords = useCallback(async (page = 1, pageSize = 50) => {
    setLoading(true);
    try {
              // Use new Batch Fetch endpoint
      
      const res = await api.post('/tasks/fetch', {
        job_id: parseInt(jobId),
        limit: pageSize,
        offset: (page - 1) * pageSize
      });
      
      // Backend returns { tasks: [...], total: ... }
      setRecords(res.data.tasks || []);
      setPagination({
        current: page,
        pageSize: pageSize,
        total: res.data.total || 0
      });
    } catch (error) {
      console.error(error);
      message.error('Failed to load records');
    } finally {
      setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    fetchJob();
    fetchRecords(1, 50);
  }, [fetchJob, fetchRecords]);

  // Polling
  useEffect(() => {
    const interval = setInterval(() => {
        fetchJob();
        
        // Silent poll for tasks
        api.post('/tasks/fetch', {
            job_id: parseInt(jobId),
            limit: pagination.pageSize,
            offset: (pagination.current - 1) * pagination.pageSize
        }).then(res => {
             setRecords(res.data.tasks || []);
             setPagination(prev => ({
                ...prev,
                total: res.data.total || 0
             }));
        }).catch(e => console.error(e));

    }, 5000);
    return () => clearInterval(interval);
  }, [jobId, pagination.current, pagination.pageSize, fetchJob]);

  const handleTableChange = (newPagination) => {
    fetchRecords(newPagination.current, newPagination.pageSize);
  };

  const handleRetry = async () => {
    try {
      const res = await api.post(`/youtube-jobs/${jobId}/retry`);
      message.success(`Retried ${res.data.reset_count} tasks`);
      fetchJob();
      fetchRecords(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to retry tasks');
    }
  };

  const statusColors = {
    PENDING: 'default',
    RUNNING: 'processing',
    COMPLETED: 'success',
    FAILED: 'error',
    PAUSED: 'warning',
    STOPPED: 'warning'
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
    { title: 'Video ID', dataIndex: 'video_id', key: 'video_id', width: 120 },
    { title: 'Title', dataIndex: 'title', key: 'title', ellipsis: true, width: 250 },
    { title: 'URL', dataIndex: 'url', key: 'url', ellipsis: true, width: 250 },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      width: 100,
      render: (status) => <Tag color={statusColors[status] || 'default'}>{status}</Tag>
    },
    { 
      title: 'Error Message', 
      dataIndex: 'error_message', 
      key: 'error_message', 
      ellipsis: true,
      width: 300,
      render: (text) => text ? <span style={{color: 'red'}} title={text}>{text}</span> : '-'
    },
    { title: 'Updated At', dataIndex: 'updated_at', key: 'updated_at', width: 180 },
  ];

  return (
    <div>
       <Breadcrumb style={{ marginBottom: 16 }}>
        <Breadcrumb.Item>
            <Link to="/youtube-jobs">Youtube Jobs</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>Job #{jobId}</Breadcrumb.Item>
      </Breadcrumb>

      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2>
          Job Details: <span style={modalDetailStyle}>{job?.r2_prefix}</span>
          {job && <Tag color={statusColors[job.status] || 'default'} style={{ marginLeft: 12 }}>{job.status}</Tag>}
        </h2>
        <div>
          {job && <span style={{ marginRight: 16, color: '#888' }}>Created: {new Date(job.created_at).toLocaleString()}</span>}
          {job && job.failed_count > 0 && (
            <Button onClick={handleRetry} style={{ marginRight: 8 }} danger>Retry Failed</Button>
          )}
          <Button icon={<ReloadOutlined />} onClick={() => { fetchJob(); fetchRecords(pagination.current, pagination.pageSize); }}>Refresh</Button>
        </div>
      </div>

      {job && (
        <Row gutter={16} style={{ marginBottom: 24 }}>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic title="Total" value={job.total_count} />
            </Card>
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic title="Success" value={job.success_count} valueStyle={{ color: '#3f8600' }} />
            </Card>
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic title="Failed" value={job.failed_count} valueStyle={{ color: '#cf1322' }} />
            </Card>
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic title="Running" value={job.running_count} valueStyle={{ color: '#faad14' }} />
            </Card>
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic title="Pending" value={job.pending_count} valueStyle={{ color: '#1890ff' }} />
            </Card>
          </Col>
          <Col xs={12} sm={8} md={4}>
            <Card size="small">
              <Statistic 
                title="Job Status" 
                value={job.status} 
                formatter={(val) => <Tag color={statusColors[val] || 'default'}>{val}</Tag>} 
              />
            </Card>
          </Col>
        </Row>
      )}

      <Table 
        columns={columns} 
        dataSource={records} 
        rowKey="id" 
        loading={loading} 
        pagination={pagination}
        onChange={handleTableChange}
        size="small"
      />
    </div>
  );
};

export default YoutubeJobDetail;
