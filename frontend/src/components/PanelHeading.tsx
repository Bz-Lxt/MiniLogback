import type { ReactNode } from 'react';

interface Props {
  index: string;
  eyebrow: string;
  title: string;
  titleId?: string;
  children?: ReactNode;
}

export function PanelHeading({ index, eyebrow, title, titleId, children }: Props) {
  return (
    <div className="panel-heading">
      <span className="panel-heading__index">{index}</span>
      <div>
        <span className="eyebrow">{eyebrow}</span>
        <h2 id={titleId}>{title}</h2>
      </div>
      {children && <div className="panel-heading__aside">{children}</div>}
    </div>
  );
}
