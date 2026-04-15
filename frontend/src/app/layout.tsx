import type { Metadata, Viewport } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Embodied Carbon Observatory',
  description: 'Temporal supply chain carbon data for US building materials. Track which plants are decarbonizing, at what rate, and what\'s driving it.',
  keywords: ['embodied carbon', 'building materials', 'supply chain', 'EPD', 'LCA', 'decarbonization'],
  openGraph: {
    title: 'Embodied Carbon Observatory',
    description: 'Which US building material plants are actually decarbonizing?',
    type: 'website',
  },
}

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 5,
  themeColor: '#080f0f',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=5, viewport-fit=cover" />
      </head>
      <body>{children}</body>
    </html>
  )
}
