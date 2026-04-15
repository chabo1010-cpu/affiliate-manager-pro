# Affiliate Manager Pro

Mobile-first Affiliate Posting Web-App mit React/Vite im Frontend und Node/Express im Backend.

## Projektdokumentation

Die zentrale technische Systemdokumentation liegt in `README_PROJECT.md`.

Dort sind aktuell dokumentiert:

- Generator
- Copybot
- Publishing Queue
- Worker-System
- Services, Datenmodell und Systemregeln

## Start

1. Frontend:
   - `cd frontend`
   - `npm install`
   - `npm run dev`

2. Backend:
   - `cd backend`
   - `npm install`
   - `npm run dev`

## Struktur

- `frontend/`: React-App mit Routing, UI und Workflows fuer Generator, Copybot und Publishing
- `backend/`: Express-API mit Services, SQLite-Datenbank und Worker-Logik

## Funktionen

- Login mit Rollen: `admin`, `editor`, `poster`, `viewer`
- Generator mit Queue oder Direct Publish
- Copybot mit Quellen, Review und Regelwerk
- Publishing Queue mit Worker-System
- Einstellungen, Historie, Logs und Admin-Bereiche
