export function buildCaption(values) {
  const parts = [];
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
