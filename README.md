# Ideadigital Bot Render Backend

Backend separado para ejecutar el bot de Telegram en Render.

## Variables de entorno

- `APP_URL`
- `PANEL_URL`
- `BOT_TOKEN`
- `WEBHOOK_SECRET`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASS`

## Endpoints

- `GET /`
- `GET /health`
- `GET /telegram/set-webhook`
- `POST /telegram/webhook`

## Deploy en Render

1. Sube esta carpeta a un repositorio de GitHub.
2. En Render crea un `Web Service`.
3. Usa:
   - Build Command: `npm install`
   - Start Command: `npm start`
4. Agrega las variables de entorno.
   - `APP_URL`: la URL publica del servicio en Render
   - `PANEL_URL`: `https://ideadigitalbots.xo.je`
   - `DB_HOST`: `sql305.infinityfree.com`
   - `DB_PORT`: `3306`
   - `DB_NAME`: `if0_41581285_ideadigitalbot`
   - `DB_USER`: `if0_41581285`
5. Despliega.
6. Abre `/telegram/set-webhook` para registrar el webhook.
