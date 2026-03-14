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
  const [queueStats, setQueueStats] = useState(null);
  const [pagination, setPagination] = useState({ 
    current: 1, 
    pageSize: 100,  // 增加默认每页显示数量
    total: 0,
    showSizeChanger: true,  // 显示每页数量选择器
    showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} tasks`,  // 显示总数信息
    pageSizeOptions: ['50', '100', '200', '500', '1000']  // 每页数量选项
  });

  const fetchJob = useCallback(async () => {
    try {
      const res = await api.get(`/youtube-jobs/${jobId}`);
      setJob(res.data);
    } catch (error) {
      console.error('Failed to load job info');
    }
  }, [jobId]);

  const fetchQueueStats = useCallback(async () => {
    try {
      const res = await api.get('/youtube-jobs/queue-stats');
      console.log('Queue stats response:', res.data);
      setQueueStats(res.data);
    } catch (error) {
      console.error('Failed to load queue stats:', error);
      // 即使失败也设置一个空对象，以便显示错误提示
      setQueueStats({ error: true, stats: [] });
    }
  }, []);

  const fetchRecords = useCallback(async (page = 1, pageSize = 100) => {
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
    fetchRecords(1, pagination.pageSize);
    fetchQueueStats();
  }, [fetchJob, fetchRecords, fetchQueueStats]);

  // Polling
  useEffect(() => {
    let timeoutId;
    let isMounted = true;
    
    const poll = async () => {
      if (!isMounted) return;
      
      try {
        fetchJob();
        fetchQueueStats();
        
        // Silent poll for tasks
        const res = await api.post('/tasks/fetch', {
            job_id: parseInt(jobId),
            limit: pagination.pageSize,
            offset: (pagination.current - 1) * pagination.pageSize
        });
        
        if (isMounted) {
             setRecords(res.data.tasks || []);
             setPagination(prev => ({
                ...prev,
                total: res.data.total || 0
             }));
        }
      } catch (e) {
        console.error(e);
      }
      
      // 每次查询完成后等待 1 秒，然后再进行下一次查询
      if (isMounted) {
        await new Promise(resolve => setTimeout(resolve, 1000));
        if (isMounted) {
          timeoutId = setTimeout(poll, 4000); // 总共 5 秒间隔（1秒延迟 + 4秒等待）
        }
      }
    };
    
    // 首次调用
    timeoutId = setTimeout(poll, 0);
    
    return () => {
      isMounted = false;
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [jobId, pagination.current, pagination.pageSize, fetchJob]);

  const handleTableChange = (newPagination) => {
    fetchRecords(newPagination.current, newPagination.pageSize);
  };

  const handleRetry = async () => {
    try {
      const res = await api.post(`/youtube-jobs/${jobId}/retry-non-completed`);
      message.success(`Retry completed: ${res.data.queued_count} tasks queued, ${res.data.skipped_count} tasks skipped (already in queue)`);
      fetchJob();
      fetchRecords(pagination.current, pagination.pageSize);
    } catch (error) {
      message.error('Failed to retry tasks: ' + (error.response?.data?.error || error.message));
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

      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2>
          Job Details: <span style={modalDetailStyle}>{job?.r2_prefix}</span>
          {job && <Tag color={statusColors[job.status] || 'default'} style={{ marginLeft: 12 }}>{job.status}</Tag>}
        </h2>
        <div>
          {job && <span style={{ marginRight: 16, color: '#888' }}>Created: {new Date(job.created_at).toLocaleString()}</span>}
          {job && (job.failed_count > 0 || job.pending_count > 0 || job.running_count > 0) && (
            <Button onClick={handleRetry} style={{ marginRight: 8 }} danger>Retry Non-Completed</Button>
          )}
          <Button icon={<ReloadOutlined />} onClick={() => { fetchJob(); fetchRecords(pagination.current, pagination.pageSize); fetchQueueStats(); }}>Refresh</Button>
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
        pagination={{
          ...pagination,
          showSizeChanger: true,
          showTotal: (total, range) => `Showing ${range[0]}-${range[1]} of ${total} tasks`,
          pageSizeOptions: ['50', '100', '200', '500', '1000']
        }}
        onChange={handleTableChange}
        size="small"
        scroll={{ x: 'max-content' }}
      />
    </div>
  );
};

export default YoutubeJobDetail;
