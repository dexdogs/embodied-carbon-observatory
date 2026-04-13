/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        bg:        '#080f0f',
        surface:   '#0d1a1a',
        panel:     '#1a2e2e',
        border:    '#1f3a3a',
        teal:      '#00e5c8',
        amber:     '#f5a623',
        red:       '#ff4444',
        muted:     '#4a7070',
        text:      '#c8e8e0',
      },
      fontFamily: {
        mono:    ['IBM Plex Mono', 'monospace'],
        display: ['DM Serif Display', 'serif'],
        body:    ['DM Sans', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
