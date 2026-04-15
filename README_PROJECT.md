# Affiliate Manager Pro - Projektdokumentation

Diese Datei ist die zentrale technische Systemdokumentation des Projekts. Sie beschreibt den aktuellen Stand der Architektur, die wichtigsten Module und die produktiven Flows im Code.

## Ziel des Systems

Affiliate Manager Pro ist ein Websystem zum Erstellen, Importieren, Verwalten und Veroeffentlichen von Deals auf mehreren Plattformen.

Aktuelle Kernziele:

- manuelle Deal-Erstellung ueber den Generator
- automatischer Deal-Import ueber den Copybot
- zentrale Publishing Queue fuer geplante Verarbeitung
- Worker-basierte Ausspielung pro Plattform
- optionales Direct Publish fuer sofortiges Telegram-Posting

Geplante Plattformen:

- Telegram
- WhatsApp
- Facebook

Aktuell produktiv bzw. funktional im Code:

- Telegram Versand
- Generator
- Publishing Queue
- Copybot Basis
- Review- und Historienlogik

Teilweise vorbereitet, aber noch nicht vollstaendig produktiv:

- WhatsApp Worker
- Facebook Worker
- Direct Publish fuer weitere Plattformen ausser Telegram

## Systemueberblick

Es gibt zwei strikt getrennte Inhaltspfade:

1. Generator
   Ein Benutzer erstellt einen Deal manuell.

2. Copybot
   Das System verarbeitet Deals aus definierten Quellen automatisch.

Beide Pfade erzeugen Publishing-Daten, aber sie sind fachlich getrennt und verwenden unterschiedliche Services.

## Architektur

### Frontend

Pfad: `frontend/`

Technik:

- React
- TypeScript
- Vite

Wichtige Seiten:

- `generator`
- `copybot`
- `publishing`
- `settings`
- `deal-history`
- `logs`

### Backend

Pfad: `backend/`

Technik:

- Node.js
- Express
- better-sqlite3

Der Einstiegspunkt ist `backend/index.js`.

Wichtige API-Gruppen:

- `/api/deals`
- `/api/telegram`
- `/api/copybot`
- `/api/publishing`
- `/api/posts`
- `/api/settings`
- `/api/history`

### Datenbank

Datei:

- `backend/data/deals.db`

Wichtige Tabellen:

- `deals_history`
- `generator_posts`
- `publishing_queue`
- `publishing_targets`
- `publishing_logs`
- `sources`
- `imported_deals`
- `copybot_logs`
- `pricing_rules`
- `sampling_rules`
- `app_settings`

## Generator

### Zweck

Der Generator ist der manuelle Deal-Erstellungsbereich. Der Benutzer erstellt einen Beitrag aktiv im UI.

### Aktueller Flow

1. Amazon-Link im Generator eingeben
2. Produktdaten per Scrape laden
3. Deal-Historie und Repost-Sperre pruefen
4. Textbausteine, Preise, Freitext und Gutschein konfigurieren
5. Bildquelle pro Kanal festlegen
6. Deal entweder direkt veroeffentlichen oder in die Queue legen

### Eigenschaften

- manuelle Deal-Erstellung
- keine automatische Screenshot-Erstellung
- optional eigener Bild-Upload
- kanalabhaengige Bildquellen
- Repost-Pruefung vor dem Versand
- Gutschein-Code fuer Telegram Copy Button

### Bildsystem im Generator

Verfuegbare Quellen:

- `standard`
  Bedeutet das im Generator vorhandene Standardbild, aktuell typischerweise das geladene Produktbild
- `upload`
  Benutzer laedt eigenes Bild oder Screenshot hoch
- `none`
  Es wird kein Bild verwendet
- `link_preview`
  Nur fuer Facebook vorgesehen

Wichtige Regel:

- Das System erstellt keine Screenshots automatisch.

### Generator-Ausgaenge

Der Generator hat zwei getrennte Ausgabemodi:

#### 1. Publishing Queue

API:

- `POST /api/publishing/generator`

Verhalten:

- erstellt einen Eintrag in `generator_posts`
- erstellt einen Queue-Eintrag in `publishing_queue`
- erstellt Zielzeilen in `publishing_targets`
- Verarbeitung erfolgt spaeter durch Worker

#### 2. Direct Publish

API:

- `POST /api/posts/direct`

Verhalten:

- speichert ebenfalls einen Generator-Post
- veroeffentlicht aktuell nur Telegram sofort
- WhatsApp und Facebook sind im Ergebnisobjekt nur vorbereitet

Wichtige Regel:

- Direct Publish ersetzt die Queue nicht, sondern ist eine zusaetzliche Option.

### Wichtige Generator-Dateien

- `frontend/src/pages/GeneratorPoster.tsx`
- `backend/services/generatorService.js`
- `backend/services/directPublisher.js`
- `backend/services/publisherService.js`

## Copybot

### Zweck

Der Copybot ist der automatische Import- und Bewertungsbereich fuer externe Deal-Quellen.

### Grundprinzip

Der Copybot arbeitet getrennt vom Generator und verwendet eigene Datenstrukturen:

- Quellenverwaltung
- Preisregeln
- Sampling-Regeln
- Review Queue
- Copybot Logs

### Quellen

Aktuell unterstuetzte Quellentypen im Datenmodell:

- Telegram
- WhatsApp

Die Plattform wird in `sources.platform` gespeichert.

Verfuegbare Verwaltungsfunktionen:

- Quelle anlegen
- Quelle bearbeiten
- Quelle aktivieren oder deaktivieren
- Quelle testen

### Bewertungslogik

Beim Import eines Deals bewertet der Copybot unter anderem:

- Seller-Typ: `AMAZON`, `FBA`, `FBM`
- Rabatt
- Deal-Score
- Keepa-Status
- Vergleichs- oder Idealo-Status
- Sampling-Regeln
- Tageslimits
- Blacklist-Keywords
- optionale Seller-Rating-Regeln
- Repost-Sperre aus der Deal-Historie

### Entscheidungsstatus

Ein importierter Deal landet typischerweise in einem dieser Zustaende:

- `posted`
- `review`
- `rejected`
- `approved`
- `blocked`

Hinweis:

- Wenn Repost-Schutz greift, wird ein sonst auto-freigegebener Deal in `review` verschoben.

### Copybot-Ausgang

Wenn ein Deal auto-freigegeben wird, erzeugt der Copybot einen Queue-Eintrag ueber `enqueueCopybotPublishing`.

Der Copybot veroeffentlicht nicht direkt selbst, sondern uebergibt an das zentrale Publishing-System.

### Review Queue

Deals mit unklarer oder nicht ausreichender Bewertung landen in der Review Queue.

Aktionen:

- Freigeben
- Verwerfen

Bei Freigabe wird erneut ein Publishing-Queue-Eintrag erzeugt.

### Wichtige Copybot-Dateien

- `frontend/src/pages/Copybot.tsx`
- `backend/routes/copybot.js`
- `backend/services/copybotService.js`
- `backend/services/dealHistoryService.js`

## Publishing Queue

### Zweck

Die Publishing Queue ist die zentrale Uebergabeschicht zwischen Inhaltserstellung und Plattform-Posting.

Eingaenge:

- Generator
- Copybot

### Datenmodell

`publishing_queue`

- Quelle des Eintrags
- Status
- vollstaendige Payload
- Retry-Zaehler
- naechster Retry-Zeitpunkt

`publishing_targets`

- Zielkanal pro Queue-Eintrag
- Aktivierungsstatus
- Bildquelle
- Bearbeitungsstatus
- Fehlertext

`publishing_logs`

- technische Verarbeitungsschritte
- Worker-Ergebnisse
- Fehlermeldungen

### Queue-Status

Typische Statuswerte:

- `queued`
- `processing`
- `posted`
- `retry`
- `failed`

### Wichtige Queue-Funktionen

- Queue-Eintrag anlegen
- Zielkanaele ableiten
- Worker-Status abrufen
- Logs anzeigen
- Retry anstossen

### Wichtige Queue-Dateien

- `frontend/src/pages/Publishing.tsx`
- `backend/routes/publishing.js`
- `backend/services/publisherService.js`

## Worker-System

### Grundprinzip

Worker verarbeiten Eintraege aus `publishing_targets`. Jeder Zielkanal wird separat abgearbeitet.

Aktuell vorhandene Worker-Services:

- `telegramWorkerService`
- `whatsappWorkerService`
- `facebookWorkerService`

### Telegram Worker

Dateien:

- `backend/services/telegramWorkerService.js`
- `backend/services/telegramSenderService.js`

Verhalten:

- sendet Text oder Bild plus Text an Telegram
- unterstuetzt Copy-Button fuer Gutschein-Code
- schreibt erfolgreiche Posts in `deals_history`

Status:

- funktional

### WhatsApp Worker

Datei:

- `backend/services/whatsappWorkerService.js`

Verhalten:

- validiert Payload
- simuliert aktuell nur die Verarbeitung
- postet noch nicht real auf WhatsApp

Status:

- vorbereitet, aber nicht produktiv

### Facebook Worker

Datei:

- `backend/services/facebookWorkerService.js`

Verhalten:

- liest Facebook-Einstellungen aus `app_settings`
- bereitet Session-basierte Verarbeitung vor
- verwendet vorhandenen Text, Link und optional vorhandenes Bild
- erstellt keine Screenshots

Status:

- vorbereitet, aber nicht produktiv

### Worker-Start

API:

- `POST /api/publishing/workers/run`

Optional kann kanalbezogen gefiltert werden.

## Historie und Repost-Schutz

### Zweck

Die Deal-Historie verhindert unerwuenschte Reposts innerhalb eines konfigurierbaren Cooldowns.

### Kernlogik

Die Pruefung arbeitet mit:

- ASIN
- normalisierter Amazon-URL
- finaler URL

### Wichtige Funktionen

- Deal-Historie speichern
- letzten Post finden
- Min- und Max-Preis der letzten 6 Monate berechnen
- Cooldown pruefen

### Relevante Dateien

- `backend/services/dealHistoryService.js`
- `backend/routes/deals.js`

## Services im Backend

Aktuell wichtige Services:

- `generatorService`
  Einstieg fuer Generator-Queue-Eintraege
- `copybotService`
  Quellen, Regeln, Review und Import-Verarbeitung
- `publisherService`
  Queue, Targets, Worker-Laeufe, Retry und Logs
- `directPublisher`
  Sofortveroeffentlichung aus dem Generator
- `telegramWorkerService`
  Queue-basierter Telegram-Versand
- `telegramSenderService`
  direkter API-Aufruf an Telegram
- `whatsappWorkerService`
  vorbereiteter WhatsApp-Worker
- `facebookWorkerService`
  vorbereiteter Facebook-Worker
- `dealHistoryService`
  Repost-Schutz, Historie und globale Posting-Einstellungen

## Einstellungen

Zentrale Einstellungen liegen in `app_settings`.

Wichtige Felder:

- `repostCooldownEnabled`
- `repostCooldownHours`
- `telegramCopyButtonText`
- `copybotEnabled`
- `facebookEnabled`
- `facebookSessionMode`
- `facebookDefaultRetryLimit`
- `facebookDefaultTarget`

## Aktueller fachlicher Stand

### Bereits umgesetzt

- Generator mit Scrape, History-Pruefung und Bildquellen je Kanal
- Queue-basierter Publishing-Flow
- Direct Publish als Zusatzoption im Generator
- Telegram Versand
- Copybot mit Quellen, Preisregeln, Sampling und Review Queue
- Publishing Logs und Worker Status
- Facebook-Grundeinstellungen im Publishing-Bereich
- Payload-Limits im Backend auf `10mb`

### Noch offen oder nur vorbereitet

- echte WhatsApp-Veroeffentlichung
- echte Facebook-Veroeffentlichung
- weitergehende Automatisierung externer Quellen
- Ausbau von Worker-Orchestrierung und Scheduling

## Wichtige Regeln

- Generator und Copybot bleiben strikt getrennt.
- Generator kann entweder direkt veroeffentlichen oder die Queue nutzen.
- Copybot veroeffentlicht ueber die Queue und nicht direkt.
- Es gibt keine automatische Screenshot-Erstellung.
- Bilder werden nur aus vorhandenen Quellen oder durch Benutzer-Upload verwendet.
- Worker posten autonom pro Zielkanal.

## Einstieg fuer Entwickler

Fuer neue Entwickler ist die sinnvollste Reihenfolge:

1. `README.md`
2. diese Datei `README_PROJECT.md`
3. `frontend/src/pages/GeneratorPoster.tsx`
4. `frontend/src/pages/Copybot.tsx`
5. `frontend/src/pages/Publishing.tsx`
6. `backend/services/publisherService.js`
7. `backend/services/copybotService.js`
8. `backend/services/dealHistoryService.js`

## Aenderungsregel fuer diese Datei

`README_PROJECT.md` muss aktualisiert werden, wenn sich eines dieser Themen aendert:

- neuer Service
- neue Route
- neuer Posting-Flow
- Aenderung an Generator oder Copybot
- Aenderung an Queue oder Worker
- Aenderung an Statuslogik oder Datenmodell
