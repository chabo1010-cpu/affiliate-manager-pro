import { useMemo, useState } from 'react';
import Layout from '../components/layout/Layout';
import { DEAL_IMAGE_RENDER, generatePostText, normalizeDealImageUrl } from '../lib/postGenerator';
import { Toast, useToast } from '../components/Toast';
import './GeneratorPoster.css';

const textOptions = [
  { value: 'A Ohne Optionen', label: 'A Ohne Optionen' },
  { value: 'B Automatischer Kassenrabatt', label: 'B Automatischer Kassenrabatt' },
  { value: 'C Coupon', label: 'C Coupon' },
  { value: 'D Werbeaktion', label: 'D Werbeaktion' },
  { value: 'E Coupon + Werbeaktion aktivieren', label: 'E Coupon + Werbeaktion aktivieren' },
  { value: 'F Voll Spar Abo', label: 'F Voll Spar Abo' },
  { value: 'G Ab 4 Stueck nochmals 5% Ersparnis', label: 'G Ab 4 Stueck nochmals 5% Ersparnis' },
  { value: 'H 15% Rabatt ab 50 EUR mit Prime', label: 'H 15% Rabatt ab 50 EUR mit Prime' },
  { value: 'I Blitzangebot', label: 'I Blitzangebot' },
  { value: 'J Zeitlich begrenztes Angebot', label: 'J Zeitlich begrenztes Angebot' }
];

const extraOptions = [
  'Ueber "Andere Verkaeufer" in den Warenkorb legen',
  'Derzeit vorbestellbar',
  'Eventuell Verkaeufer wechseln',
  'Lieferzeit beachten',
  'Rabattgutschein',
  'Spar Abo',
  'Ueber "Alle Angebote" in den Warenkorb legen',
  'Verschiedene Ausfuehrungen',
  'Verschiedene Farben',
  'Verschiedene Groessen',
  'Verschiedene Groessen und Farben',
  'Zzgl. Pfand'
];

const oldIconOptions = ['Statt', 'Vorher', 'Alt'];
const newIconOptions = ['Jetzt', 'Deal', 'Neu'];
const amazonScrapeApiUrl =
  `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'}/api/scrape`;
const telegramApiUrl =
  `${import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000'}/api/telegram/send`;

function GeneratorPosterPage() {
  const [amazonLink, setAmazonLink] = useState('');
  const [advertising, setAdvertising] = useState(false);
  const [textBlock, setTextBlock] = useState(textOptions[0].value);
  const [expandedAdvanced, setExpandedAdvanced] = useState(false);
  const [selectedExtras, setSelectedExtras] = useState<string[]>([]);
  const [oldPrice, setOldPrice] = useState('39,99 EUR');
  const [currentPrice, setCurrentPrice] = useState('24,90 EUR');
  const [oldIcon, setOldIcon] = useState(oldIconOptions[1]);
  const [newIcon, setNewIcon] = useState(newIconOptions[0]);
  const [extraText, setExtraText] = useState('Sofort verfuegbar, Versand heute.');
  const [publishing, setPublishing] = useState(false);
  const [scraping, setScraping] = useState(false);
  const [hasScraped, setHasScraped] = useState(false);
  const [scrapedImageUrl, setScrapedImageUrl] = useState('');
  const [scrapedTitle, setScrapedTitle] = useState('');
  const [rabattgutscheinCode, setRabattgutscheinCode] = useState('');
  const [formError, setFormError] = useState('');
  const { toast, showToast } = useToast();
  const rabattgutscheinAktiv = selectedExtras.includes('Rabattgutschein');

  const combinedExtraText = useMemo(() => extraText.trim(), [extraText]);

  const generatedPost = useMemo(
    () =>
      generatePostText({
        productTitle: scrapedTitle,
        freiText: combinedExtraText,
        textBaustein: textBlock,
        alterPreis: oldPrice,
        neuerPreis: currentPrice,
        amazonLink,
        werbung: advertising,
        extraOptions: selectedExtras,
        rabattgutscheinCode
      }),
    [scrapedTitle, combinedExtraText, textBlock, oldPrice, currentPrice, amazonLink, advertising, selectedExtras, rabattgutscheinCode]
  );

  const validateBeforePublish = () => {
    if (!textBlock.trim()) {
      return 'Bitte eine Hauptoption waehlen.';
    }

    if (rabattgutscheinAktiv && !rabattgutscheinCode.trim()) {
      return 'Rabattgutschein fehlt.';
    }

    return '';
  };

  const handleScrape = async () => {
    console.log('BUTTON CLICK');
    console.log('STATE amazonLink:', amazonLink);

    if (scraping) return;

    const finalAmazonLink = (amazonLink || '').trim();
    console.log('FINAL URL:', finalAmazonLink);

    if (!finalAmazonLink) {
      setHasScraped(false);
      setScrapedImageUrl('');
      setScrapedTitle('');
      setFormError('');
      showToast('Bitte zuerst einen Amazon-Link eingeben.');
      return;
    }

    const payload = { url: finalAmazonLink };
    console.log('SCRAPE PAYLOAD:', payload);
    setScraping(true);

    try {
      const response = await fetch('http://localhost:4000/api/scrape', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      const rawResponse = await response.text();
      let data: {
        success?: boolean;
        error?: string;
        code?: string;
        message?: string;
        image?: string;
        title?: string;
        price?: string;
        oldPrice?: string;
        link?: string;
      } = {};

      try {
        data = rawResponse ? JSON.parse(rawResponse) : {};
      } catch {
        data = { error: rawResponse || 'Unbekannte Scrape-Antwort' };
      }

      console.log('SCRAPE STATUS:', response.status);
      console.log('SCRAPE RESPONSE:', data);

      if (!response.ok) {
        setHasScraped(false);
        setScrapedImageUrl('');
        setScrapedTitle('');
        setFormError('');
        showToast(
          data.error ||
            data.message ||
            data.code ||
            `Scrape fehlgeschlagen (${response.status}). Bitte Backend pruefen.`
        );
        return;
      }

      const normalizedImageUrl = normalizeDealImageUrl(data.image || '');
      setScrapedImageUrl(normalizedImageUrl);
      setScrapedTitle(data.title || '');
      setOldPrice(data.oldPrice || oldPrice);
      setCurrentPrice(data.price || currentPrice);
      setHasScraped(true);
      setFormError('');
      showToast(
        normalizedImageUrl
          ? 'Amazon Link erfolgreich gescrapt und Produktbild geladen'
          : 'Amazon Link erfolgreich gescrapt, aber ohne Produktbild'
      );

      if (!data.title) {
        showToast('Produkttitel konnte nicht gelesen werden. Fallback wird verwendet.', 2600);
      }
    } finally {
      setScraping(false);
    }
  };

  const handlePublish = async () => {
    if (publishing) return;

    if (!hasScraped) {
      showToast('Bitte zuerst erfolgreich den Amazon Link scrapen');
      return;
    }

    const validationError = validateBeforePublish();
    if (validationError) {
      setFormError(validationError);
      showToast(validationError);
      return;
    }

    if (!generatedPost.telegramCaption || generatedPost.telegramCaption.trim().length === 0) {
      showToast('Bitte fuellen Sie das Formular aus');
      return;
    }

    setFormError('');
    setPublishing(true);
    try {
      const response = await fetch(telegramApiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          text: generatedPost.telegramCaption,
          imageUrl: scrapedImageUrl || undefined,
          amazonLink,
          rabattgutscheinCode: rabattgutscheinAktiv ? rabattgutscheinCode.trim() : undefined
        })
      });

      const rawResponse = await response.text();
      let data: { message?: string; error?: string; code?: string } = {};

      try {
        data = rawResponse ? JSON.parse(rawResponse) : {};
      } catch {
        data = { error: rawResponse || 'Unbekannte Backend-Antwort' };
      }

      if (!response.ok) {
        const backendMessage =
          data.error || data.message || `Backend-Fehler (${response.status}) beim Telegram-Versand`;
        setFormError(backendMessage);
        showToast(backendMessage);
        return;
      }

      showToast(
        rabattgutscheinAktiv
          ? 'Post und Rabattgutschein zu Telegram gesendet'
          : 'Post zu Telegram gesendet'
      );
    } catch (error) {
      console.error('Telegram send error:', error);
      setFormError(
        error instanceof Error
          ? `Telegram-Verbindungsfehler: ${error.message}`
          : 'Telegram-Verbindungsfehler'
      );
      showToast(
        error instanceof Error
          ? `Telegram-Verbindungsfehler: ${error.message}`
          : 'Telegram-Verbindungsfehler'
      );
    } finally {
      setPublishing(false);
    }
  };

  const handleToggleExtra = (option: string) => {
    setSelectedExtras((prev) => {
      const next = prev.includes(option) ? prev.filter((item) => item !== option) : [...prev, option];
      if (!next.includes('Rabattgutschein')) {
        setRabattgutscheinCode('');
        setFormError('');
      }
      return next;
    });
  };

  return (
    <Layout showSidebar={false}>
      <div className="generator-desktop-page">
        <div className="generator-desktop-shell">
          <section className="generator-panel generator-intro-panel">
            <div className="generator-panel-header">
              <h1>Generator Poster</h1>
            </div>

            <div className="generator-base-fields">
              <label className="generator-form-field">
                <span>Amazon Link</span>
                <input
                  type="text"
                  value={amazonLink}
                  onChange={(e) => {
                    console.log('INPUT CHANGE', e.target.value);
                    setAmazonLink(e.target.value);
                    setHasScraped(false);
                    setScrapedImageUrl('');
                    setScrapedTitle('');
                    setFormError('');
                  }}
                  placeholder="https://amazon.de/..."
                />
              </label>

              {hasScraped && (
                <div className="generator-scrape-meta">
                  <p>
                    Produkttitel: <strong>{generatedPost.productTitle}</strong>
                  </p>
                  <p>
                    Bild vorbereitet: {DEAL_IMAGE_RENDER.width}x{DEAL_IMAGE_RENDER.height} {DEAL_IMAGE_RENDER.fit}
                  </p>
                </div>
              )}

              <button
                type="button"
                className="generator-action-button secondary"
                onClick={handleScrape}
                disabled={scraping}
              >
                {scraping ? 'Amazon Link wird gescrapt...' : 'Scrap Amazon Link'}
              </button>

              <label className="generator-checkbox-row">
                <input
                  type="checkbox"
                  checked={advertising}
                  onChange={(e) => setAdvertising(e.target.checked)}
                />
                <span>Werbung</span>
              </label>
            </div>
          </section>

          {hasScraped && (
            <>
              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Textbausteine</h2>
                </div>

                <div className="generator-vertical-list">
                  {textOptions.map((option) => (
                    <label key={option.value} className="generator-list-row">
                      <input
                        type="radio"
                        name="textBlock"
                        value={option.value}
                        checked={textBlock === option.value}
                        onChange={() => setTextBlock(option.value)}
                      />
                      <span>{option.label}</span>
                    </label>
                  ))}
                </div>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header generator-panel-header-inline">
                  <h2>Erweiterte Ansicht</h2>
                  <button
                    type="button"
                    className="generator-action-button secondary compact"
                    onClick={() => setExpandedAdvanced((prev) => !prev)}
                  >
                    {expandedAdvanced ? 'Ausblenden' : 'Einblenden'}
                  </button>
                </div>

                {expandedAdvanced && (
                  <>
                    <div className="generator-vertical-list">
                      {extraOptions.map((option) => (
                        <label key={option} className="generator-list-row">
                          <input
                            type="checkbox"
                            checked={selectedExtras.includes(option)}
                            onChange={() => handleToggleExtra(option)}
                          />
                          <span>{option}</span>
                        </label>
                      ))}
                    </div>

                    {rabattgutscheinAktiv && (
                      <div className="generator-inline-field">
                        <label className="generator-form-field">
                          <span>Rabattgutschein eingeben</span>
                          <input
                            className={formError ? 'generator-coupon-input has-error' : 'generator-coupon-input'}
                            type="text"
                            value={rabattgutscheinCode}
                            onChange={(e) => {
                              setRabattgutscheinCode(e.target.value);
                              setFormError('');
                            }}
                            placeholder="z. B. YE67K4BD"
                          />
                        </label>
                        <p className="generator-field-hint">
                          Wird nach dem Hauptpost automatisch als zweite Nachricht gesendet.
                        </p>
                        {formError && <p className="generator-form-error">{formError}</p>}
                      </div>
                    )}
                  </>
                )}
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Preise</h2>
                </div>

                <div className="generator-price-grid">
                  <div className="generator-column-fields">
                    <label className="generator-form-field">
                      <span>Icon Alt</span>
                      <select value={oldIcon} onChange={(e) => setOldIcon(e.target.value)}>
                        {oldIconOptions.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                    </label>

                    <label className="generator-form-field">
                      <span>Icon Neu</span>
                      <select value={newIcon} onChange={(e) => setNewIcon(e.target.value)}>
                        {newIconOptions.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>

                  <div className="generator-column-fields">
                    <label className="generator-form-field">
                      <span>Alter Preis</span>
                      <input
                        type="text"
                        value={oldPrice}
                        onChange={(e) => setOldPrice(e.target.value)}
                        placeholder="39,99 EUR"
                      />
                    </label>

                    <label className="generator-form-field">
                      <span>Aktueller Preis</span>
                      <input
                        type="text"
                        value={currentPrice}
                        onChange={(e) => setCurrentPrice(e.target.value)}
                        placeholder="24,90 EUR"
                      />
                    </label>
                  </div>
                </div>
              </section>

              <section className="generator-panel">
                <div className="generator-panel-header">
                  <h2>Freitext</h2>
                </div>

                <label className="generator-form-field">
                  <span>Zusatztext</span>
                  <textarea
                    value={extraText}
                    onChange={(e) => setExtraText(e.target.value)}
                    placeholder="z. B. Sofort verfuegbar, Versand heute."
                    rows={7}
                  />
                </label>
              </section>

              <section className="generator-panel generator-submit-panel">
                {formError && <p className="generator-form-error">{formError}</p>}
                <button
                  type="button"
                  className="generator-action-button primary"
                  onClick={handlePublish}
                  disabled={publishing}
                >
                  {publishing ? 'Wird gesendet...' : 'Zu Telegram veroeffentlichen'}
                </button>
              </section>
            </>
          )}
        </div>

        {toast && <Toast message={toast.message} duration={toast.duration} />}
      </div>
    </Layout>
  );
}

export default GeneratorPosterPage;
