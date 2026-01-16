import { create } from 'zustand'
import Cookies from 'js-cookie'

export const useAuthStore = create((set) => {
  // 检查是否为开发环境，如果是则使用模拟用户数据
  const isDev = process.env.NODE_ENV === 'development';
  let initialToken = Cookies.get('token') || null;
  let initialUser = Cookies.get('user') ? JSON.parse(Cookies.get('user')) : null;
  
  // 如果在开发环境中且没有认证信息，则使用模拟数据
  if (isDev && !initialToken && !initialUser) {
    initialToken = 'mock-token-dev';
    initialUser = {
      id: 1,
      name: 'Development User',
      email: 'dev@example.com',
      avatar_url: 'https://via.placeholder.com/150'
    };
    
    // 设置cookie，这样刷新页面后仍然保持登录状态
    Cookies.set('token', initialToken, { expires: 7 });
    Cookies.set('user', JSON.stringify(initialUser), { expires: 7 });
  }

  return ({
    token: initialToken,
    user: initialUser,
    setAuth: (token, user) => {
      Cookies.set('token', token, { expires: 7 }) // 7 days
      Cookies.set('user', JSON.stringify(user), { expires: 7 })
      set({ token, user })
    },
    logout: () => {
      Cookies.remove('token')
      Cookies.remove('user')
      set({ token: null, user: null })
    },
  })
});