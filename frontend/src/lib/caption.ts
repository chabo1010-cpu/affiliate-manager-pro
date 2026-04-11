interface CaptionValues {
  advertising: string | boolean;
  oldPrice: string;
  currentPrice: string;
  oldIcon: string;
  newIcon: string;
  textBlock: string;
  extraText: string;
  link: string;
}

export function buildCaption(values: CaptionValues) {
  const parts: string[] = [];
  if (values.advertising) {
    parts.push('🔸 Anzeige / Partnerlink');
  }

  if (values.oldPrice) {
    parts.push(`${values.oldIcon} ${values.oldPrice}`);
  }

  parts.push(`${values.newIcon} ${values.currentPrice}`);

  if (values.textBlock) {
    parts.push(values.textBlock);
  }

  if (values.extraText) {
    parts.push(values.extraText);
  }

  if (values.link) {
    parts.push(values.link);
  }

  return parts.filter(Boolean).join('\n');
}
