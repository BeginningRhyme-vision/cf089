import React, { useEffect, useState } from 'react';
import { Table, Button, Modal, Form, Input, message, Popconfirm } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined } from '@ant-design/icons';
import api from '../api';

const cellStyle = {
  maxWidth: 200,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  display: 'inline-block' 
};

const MetadataList = () => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingItem, setEditingItem] = useState(null);
  const [form] = Form.useForm();

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await api.get('/metadata/');
      setData(res.data);
    } catch (error) {
      message.error('Failed to load metadata');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleAdd = () => {
    setEditingItem(null);
    form.resetFields();
    setIsModalOpen(true);
  };

  const handleEdit = (record) => {
    setEditingItem(record);
    form.setFieldsValue({
      client_name: record.client_name,
      endpoint: record.endpoint,
      ak: record.ak,
      sk: '', // Don't show existing secret
    });
    setIsModalOpen(true);
  };

  const handleDelete = async (id) => {
    try {
      await api.delete(`/metadata/${id}`);
      message.success('Deleted successfully');
      fetchData();
    } catch (error) {
      message.error('Delete failed');
    }
  };

  const handleOk = async () => {
    try {
      const values = await form.validateFields();
      if (editingItem) {
        await api.put(`/metadata/${editingItem.id}`, values);
        message.success('Updated successfully');
      } else {
        await api.post('/metadata/', values);
        message.success('Created successfully');
      }
      setIsModalOpen(false);
      fetchData();
    } catch (error) {
      console.error(error);
    }
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', key: 'id', width: 80 },
    { 
      title: 'Client Name', 
      dataIndex: 'client_name', 
      key: 'client_name',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
    { 
      title: 'Endpoint', 
      dataIndex: 'endpoint', 
      key: 'endpoint',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
    { 
      title: 'Access Key', 
      dataIndex: 'ak', 
      key: 'ak',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
    {
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <span>
          <Button icon={<EditOutlined />} size="small" onClick={() => handleEdit(record)} style={{ marginRight: 8 }} />
          <Popconfirm title="Sure to delete?" onConfirm={() => handleDelete(record.id)}>
            <Button icon={<DeleteOutlined />} size="small" danger />
          </Popconfirm>
        </span>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
          Add Client Metadata
        </Button>
      </div>
      <Table columns={columns} dataSource={data} rowKey="id" loading={loading} />
      
      <Modal 
        title={editingItem ? "Edit Metadata" : "Add Metadata"} 
        open={isModalOpen} 
        onOk={handleOk} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="client_name" label="Client Name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="endpoint" label="Endpoint URL" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="ak" label="Access Key" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="sk" label="Secret Key" rules={[{ required: !editingItem }]}>
            <Input.Password placeholder={editingItem ? "Leave blank to keep unchanged" : ""} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default MetadataList;
