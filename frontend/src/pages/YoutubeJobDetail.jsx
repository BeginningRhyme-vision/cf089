import React, { useEffect, useState } from 'react';
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

  const fetchJobDetails = async () => {
    setLoading(true);
    try {
      const jobRes = await api.get(`/youtube-jobs/${jobId}`);
      setJob(jobRes.data);
      
      const recordsRes = await api.get(`/youtube-jobs/${jobId}/records`);
      setRecords(recordsRes.data);
    } catch (error) {
      message.error('Failed to load job details');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobDetails();
    const interval = setInterval(fetchJobDetails, 5000);
    return () => clearInterval(interval);
  }, [jobId]);

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
        <Button icon={<ReloadOutlined />} onClick={fetchJobDetails}>Refresh</Button>
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
        pagination={{ pageSize: 50 }}
        size="small"
      />
    </div>
  );
};

export default YoutubeJobDetail;
