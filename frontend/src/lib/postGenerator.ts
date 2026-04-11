export interface PostConfig {
  productTitle?: string;
  freiText?: string;
  textBaustein?: string;
  alterPreis?: string;
  neuerPreis?: string;
  amazonLink?: string;
  werbung?: boolean;
  extraOptions?: string[];
  rabattgutscheinCode?: string;
}

export const DEAL_IMAGE_RENDER = {
  width: 1200,
  height: 1200,
  fit: 'contain'
} as const;

function cleanText(value?: string): string {
  return value?.trim() || '';
}

function stripOptionPrefix(value?: string): string {
  return cleanText(value).replace(/^[A-Z]\s+/, '').trim();
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function normalizePrice(value?: string): string {
  return cleanText(value).replace(/\s+/g, '');
}

function uniqueLines(lines: string[]): string[] {
  const seen = new Set<string>();
  return lines.filter((line) => {
    const normalized = line.trim().toLowerCase();
    if (!normalized || seen.has(normalized)) {
      return false;
    }

    seen.add(normalized);
    return true;
  });
}

function buildOptionLines(config: PostConfig): string[] {
  const lines: string[] = [];
  const mainOption = stripOptionPrefix(config.textBaustein);

  if (mainOption && !/ohne optionen/i.test(mainOption)) {
    lines.push(`✔️ ${mainOption}`);
  }

  for (const option of config.extraOptions || []) {
    const trimmedOption = cleanText(option);
    if (!trimmedOption) {
      continue;
    }

    if (trimmedOption === 'Rabattgutschein') {
      lines.push('🏷️ Rabattgutschein aktiv');
      continue;
    }

    if (/coupon/i.test(trimmedOption)) {
      lines.push(`✔️ ${trimmedOption}`);
      continue;
    }

    lines.push(`• ${trimmedOption}`);
  }

  const freeTextLines = cleanText(config.freiText)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => `• ${line}`);

  lines.push(...freeTextLines);

  return uniqueLines(lines);
}

function buildBaseLines(config: PostConfig, channel: 'telegram' | 'whatsapp'): string[] {
  const title = cleanText(config.productTitle) || 'Amazon Produkt';
  const price = normalizePrice(config.neuerPreis);
  const link = cleanText(config.amazonLink);
  const optionLines = buildOptionLines(config);

  const bold = (value: string) => (channel === 'telegram' ? `<b>${escapeHtml(value)}</b>` : `*${value}*`);
  const italic = (value: string) => (channel === 'telegram' ? `<i>${escapeHtml(value)}</i>` : `_${value}_`);
  const plain = (value: string) => (channel === 'telegram' ? escapeHtml(value) : value);

  const sections: string[] = [];
  sections.push(bold(title));

  const priceLinkBlock: string[] = [];
  if (price) {
    priceLinkBlock.push(`🔥 Jetzt ${bold(price)}`);
  }

  if (link) {
    priceLinkBlock.push(`➡️ ${plain(link)}`);
  }

  if (priceLinkBlock.length > 0) {
    sections.push(priceLinkBlock.join('\n'));
  }

  if (optionLines.length > 0) {
    sections.push(optionLines.map(plain).join('\n'));
  }

  sections.push(italic('Anzeige/Partnerlink'));
  return sections.filter(Boolean);
}

export function generatePostText(config: PostConfig) {
  const telegramCaption = buildBaseLines(config, 'telegram').join('\n\n');
  const whatsappText = buildBaseLines(config, 'whatsapp').join('\n\n');
  const rabattgutscheinCode = cleanText(config.rabattgutscheinCode);
  const couponFollowUp = rabattgutscheinCode ? `🏷️ Rabattgutschein: ${rabattgutscheinCode}` : '';

  return {
    telegramCaption,
    whatsappText,
    couponFollowUp,
    productTitle: cleanText(config.productTitle) || 'Amazon Produkt'
  };
}

export function normalizeDealImageUrl(imageUrl?: string): string {
  const trimmed = cleanText(imageUrl);
  if (!trimmed) {
    return '';
  }

  if (/images-amazon\.com|media-amazon\.com|ssl-images-amazon/i.test(trimmed)) {
    return trimmed.replace(/\._[^.]+_\./, `._SL${DEAL_IMAGE_RENDER.width}_.`);
  }

  return trimmed;
}

export function copyToClipboard(text: string): Promise<boolean> {
  return navigator.clipboard
    .writeText(text)
    .then(() => true)
    .catch(() => false);
}

export function formatForTelegram(text: string): string {
  return text;
}

export function formatForWhatsApp(text: string): string {
  return text;
}

export function downloadAsText(text: string, filename: string = 'post.txt'): void {
  const element = document.createElement('a');
  element.setAttribute('href', `data:text/plain;charset=utf-8,${encodeURIComponent(text)}`);
  element.setAttribute('download', filename);
  element.style.display = 'none';
  document.body.appendChild(element);
  element.click();
  document.body.removeChild(element);
}
