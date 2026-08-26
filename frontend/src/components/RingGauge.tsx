import { useEffect, useRef } from 'react';
import { clampPercent, formatNumber, waterlineSeverity } from '../lib/format';

interface Props {
  percent: number;
  depth: number;
  capacity: number;
}

const COLORS = {
  healthy: { wave: '#55f28e', deep: '#1d8f55', glow: 'rgba(85, 242, 142, .24)' },
  warning: { wave: '#f8bb52', deep: '#9d6819', glow: 'rgba(248, 187, 82, .25)' },
  danger: { wave: '#ff4f67', deep: '#a5213a', glow: 'rgba(255, 79, 103, .28)' },
};

export function RingGauge({ percent, depth, capacity }: Props) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const targetRef = useRef(clampPercent(percent));
  const displayedRef = useRef(clampPercent(percent));

  useEffect(() => {
    targetRef.current = clampPercent(percent);
  }, [percent]);

  useEffect(() => {
    const wrapper = wrapperRef.current;
    const canvas = canvasRef.current;
    if (!wrapper || !canvas) return;
    const context = canvas.getContext('2d');
    if (!context) return;

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let width = 0;
    let height = 0;
    let frame = 0;
    let start = performance.now();

    const resize = () => {
      const rect = wrapper.getBoundingClientRect();
      width = Math.max(1, Math.floor(rect.width));
      height = Math.max(1, Math.floor(rect.height));
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      context.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const drawGrid = () => {
      context.strokeStyle = 'rgba(105, 145, 132, .12)';
      context.lineWidth = 1;
      for (let x = 0; x <= width; x += 32) {
        context.beginPath();
        context.moveTo(x + 0.5, 0);
        context.lineTo(x + 0.5, height);
        context.stroke();
      }
      for (let y = 0; y <= height; y += 32) {
        context.beginPath();
        context.moveTo(0, y + 0.5);
        context.lineTo(width, y + 0.5);
        context.stroke();
      }
    };

    const wavePath = (waterY: number, phase: number, amplitude: number, offset: number) => {
      context.beginPath();
      context.moveTo(0, height);
      context.lineTo(0, waterY);
      for (let x = 0; x <= width + 8; x += 8) {
        const y = waterY + Math.sin(x * 0.027 + phase + offset) * amplitude;
        context.lineTo(x, y);
      }
      context.lineTo(width, height);
      context.closePath();
    };

    const render = (now: number) => {
      const target = targetRef.current;
      displayedRef.current = reducedMotion
        ? target
        : displayedRef.current + (target - displayedRef.current) * 0.075;
      if (Math.abs(target - displayedRef.current) < 0.01) displayedRef.current = target;

      const value = clampPercent(displayedRef.current);
      const severity = waterlineSeverity(value);
      const colors = COLORS[severity];
      const waterY = height - (height * value) / 100;
      const phase = reducedMotion ? 0 : (now - start) / 850;

      context.clearRect(0, 0, width, height);
      drawGrid();

      const gradient = context.createLinearGradient(0, waterY, 0, height);
      gradient.addColorStop(0, colors.wave);
      gradient.addColorStop(0.35, colors.deep);
      gradient.addColorStop(1, 'rgba(5, 18, 14, .88)');
      context.save();
      context.globalAlpha = 0.34;
      wavePath(waterY + 8, phase * 0.74, 9, 2.1);
      context.fillStyle = colors.deep;
      context.fill();
      context.restore();

      wavePath(waterY, phase, 6, 0);
      context.fillStyle = gradient;
      context.shadowBlur = 25;
      context.shadowColor = colors.glow;
      context.fill();
      context.shadowBlur = 0;

      context.beginPath();
      for (let x = 0; x <= width + 8; x += 8) {
        const y = waterY + Math.sin(x * 0.027 + phase) * 6;
        if (x === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      }
      context.strokeStyle = colors.wave;
      context.lineWidth = 1.5;
      context.stroke();

      context.fillStyle = 'rgba(220, 235, 229, .48)';
      context.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
      context.fillText('100%', 12, 18);
      context.fillText('50%', 12, height / 2 - 7);
      context.fillText('0%', 12, height - 10);

      frame = window.requestAnimationFrame(render);
    };

    const observer = new ResizeObserver(resize);
    observer.observe(wrapper);
    resize();
    frame = window.requestAnimationFrame(render);

    return () => {
      observer.disconnect();
      window.cancelAnimationFrame(frame);
      start = 0;
    };
  }, []);

  const clamped = clampPercent(percent);
  const severity = waterlineSeverity(clamped);
  return (
    <div className={`ring-gauge ring-gauge--${severity}`}>
      <div className="ring-gauge__canvas-wrap" ref={wrapperRef}>
        <canvas ref={canvasRef} aria-hidden="true" />
        <div className="ring-gauge__readout">
          <span className="ring-gauge__label">CURRENT WATERLINE</span>
          <strong>{clamped.toFixed(1)}<small>%</small></strong>
          <span className={`severity-label severity-label--${severity}`}>{severity.toUpperCase()}</span>
        </div>
      </div>
      <div className="ring-gauge__footer">
        <span><small>QUEUE DEPTH</small><b>{formatNumber(depth)}</b></span>
        <span className="ring-gauge__scale" aria-hidden="true"><i style={{ width: `${clamped}%` }} /></span>
        <span className="align-right"><small>CAPACITY</small><b>{formatNumber(capacity)}</b></span>
      </div>
      <p className="sr-only" role="status">
        Ring Buffer 当前水位 {clamped.toFixed(1)}%，状态 {severity}，深度 {depth}，容量 {capacity}。
      </p>
    </div>
  );
}
