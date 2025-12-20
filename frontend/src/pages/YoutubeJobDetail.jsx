import React, { useEffect, useState, useCallback } from 'react';
import { Table, Button, Card, Row, Col, Statistic, Tag, Breadcrumb, message } from 'antd';
import { ReloadOutlined, ArrowLeftOutlined } from '@ant-design/icons';
import { useParams, useNavigate, Link } from 'react-router-dom';
import api from '../api';

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
      const res = await api.get(`/youtube-jobs/${jobId}/records`, {
        params: { page, size: pageSize }
      });
      setRecords(res.data.items);
      setPagination({
        current: res.data.page,
        pageSize: res.data.size,
        total: res.data.total
      });
    } catch (error) {
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
        // Poll current page without setting loading state to avoid flicker?
        // But fetchRecords sets loading. 
        // Let's create a silent fetch or just accept the flicker for now or modify fetchRecords.
        // For simplicity, let's just re-fetch. Ideally we separate 'loading' state for initial load vs updates.
        // But since we are reusing fetchRecords, it will trigger loading.
        // Let's modify fetchRecords to accept a 'silent' param?
        // Or just define a separate poll function.
        
        api.get(`/youtube-jobs/${jobId}/records`, {
            params: { page: pagination.current, size: pagination.pageSize }
        }).then(res => {
             setRecords(res.data.items);
             setPagination(prev => ({
                ...prev,
                total: res.data.total
             }));
        }).catch(e => console.error(e));

    }, 5000);
    return () => clearInterval(interval);
  }, [jobId, pagination.current, pagination.pageSize, fetchJob]);

  const handleTableChange = (newPagination) => {
    fetchRecords(newPagination.current, newPagination.pageSize);
  };

  const statusColors = {
    PENDING: 'default',
    RUNNING: 'processing',
    COMPLETED: 'success',
    FAILED: 'error'
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
    { title: 'Video ID', dataIndex: 'video_id', key: 'video_id', width: 120 },
    { title: 'Title', dataIndex: 'title', key: 'title', ellipsis: true },
    { title: 'URL', dataIndex: 'url', key: 'url', ellipsis: true },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      width: 100,
      render: (status) => <Tag color={statusColors[status]}>{status}</Tag>
    },
    { 
      title: 'Error Message', 
      dataIndex: 'error_message', 
      key: 'error_message', 
      ellipsis: true,
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
        <h2>Job Details: {job?.r2_prefix}</h2>
        <Button icon={<ReloadOutlined />} onClick={() => { fetchJob(); fetchRecords(pagination.current, pagination.pageSize); }}>Refresh</Button>
      </div>

      {job && (
        <Row gutter={16} style={{ marginBottom: 24 }}>
          <Col span={6}>
            <Card>
              <Statistic title="Total" value={job.total_count} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="Success" value={job.success_count} valueStyle={{ color: '#3f8600' }} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="Failed" value={job.failed_count} valueStyle={{ color: '#cf1322' }} />
            </Card>
          </Col>
          <Col span={6}>
            <Card>
              <Statistic title="Pending" value={job.pending_count} valueStyle={{ color: '#1890ff' }} />
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
