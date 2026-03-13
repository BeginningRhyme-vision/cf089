import { create } from 'zustand'
import Cookies from 'js-cookie'

export const useAuthStore = create((set) => ({
  token: Cookies.get('token') || null,
  user: Cookies.get('user') ? JSON.parse(Cookies.get('user')) : null,
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
}))
