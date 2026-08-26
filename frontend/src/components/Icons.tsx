import type { SVGProps } from 'react';

type Props = SVGProps<SVGSVGElement>;

const defaults: Props = {
  width: 18,
  height: 18,
  viewBox: '0 0 24 24',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.8,
  strokeLinecap: 'round',
  strokeLinejoin: 'round',
  'aria-hidden': true,
};

export function RefreshIcon(props: Props) {
  return <svg {...defaults} {...props}><path d="M20 7v5h-5"/><path d="M4 17v-5h5"/><path d="M6.1 8.1a7 7 0 0 1 11.2-2.2L20 8M4 16l2.7 2.1a7 7 0 0 0 11.2-2.2"/></svg>;
}

export function CloseIcon(props: Props) {
  return <svg {...defaults} {...props}><path d="M6 6l12 12M18 6 6 18"/></svg>;
}

export function ArrowIcon(props: Props) {
  return <svg {...defaults} {...props}><path d="M5 12h14M14 7l5 5-5 5"/></svg>;
}

export function PulseIcon(props: Props) {
  return <svg {...defaults} {...props}><path d="M3 12h4l2-6 4 12 2-6h6"/></svg>;
}

export function AlertIcon(props: Props) {
  return <svg {...defaults} {...props}><path d="M12 3 2.7 19h18.6L12 3Z"/><path d="M12 9v4M12 16.5h.01"/></svg>;
}

export function DatabaseIcon(props: Props) {
  return <svg {...defaults} {...props}><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>;
}
