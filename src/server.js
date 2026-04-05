const express = require("express");
const crypto = require("crypto");
const config = require("./config");
const { sendMessage, setWebhook } = require("./telegram");
const { testDbConnection, findLatestRaffleWinner } = require("./db");

const app = express();
app.use(express.json());

function isSecretValid(req) {
  if (!config.webhookSecret) {
    return true;
  }

  const incoming = req.get("X-Telegram-Bot-Api-Secret-Token") || "";
  return crypto.timingSafeEqual(
    Buffer.from(incoming),
    Buffer.from(config.webhookSecret)
  );
}

app.get("/", async (_req, res) => {
  const dbStatus = await testDbConnection();
  res.json({
    service: "Ideadigital Bot Telegram Backend",
    status: "ok",
    db: dbStatus,
    webhookPath: "/telegram/webhook",
    setWebhookPath: "/telegram/set-webhook"
  });
});

app.get("/health", async (_req, res) => {
  const dbStatus = await testDbConnection();
  res.json({
    ok: true,
    db: dbStatus
  });
});

app.get("/telegram/set-webhook", async (_req, res) => {
  const webhookUrl = `${config.appUrl}/telegram/webhook`;
  const result = await setWebhook(webhookUrl);
  res.json({
    webhook_url: webhookUrl,
    telegram: result
  });
});

app.post("/telegram/webhook", async (req, res) => {
  if (!isSecretValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid secret token." });
  }

  const update = req.body || {};
  const message = update.message || {};
  const chatId = message.chat && message.chat.id;
  const text = (message.text || "").trim();
  const from = message.from || {};
  const firstName = from.first_name || from.username || "there";

  if (!chatId) {
    return res.json({ ok: true, skipped: true });
  }

  if (text === "/start") {
    await sendMessage(
      chatId,
      `Welcome to <b>Ideadigital Bot</b>, ${firstName}.\n\nUse /help to see the available commands.`
    );
    return res.json({ ok: true, command: "/start" });
  }

  if (text === "/help") {
    await sendMessage(
      chatId,
      [
        "Available commands:",
        "/start - Start the bot",
        "/help - Show this help",
        "/panel - Open your control panel",
        "/sorteo - Show the latest saved raffle winner"
      ].join("\n")
    );
    return res.json({ ok: true, command: "/help" });
  }

  if (text === "/panel") {
    await sendMessage(
      chatId,
      `Open your panel here:\n${config.panelUrl}/dashboard.php`
    );
    return res.json({ ok: true, command: "/panel" });
  }

  if (text.indexOf("/sorteo") === 0) {
    const winner = await findLatestRaffleWinner();

    if (!winner) {
      await sendMessage(
        chatId,
        "There is no raffle winner saved yet in the database."
      );
      return res.json({ ok: true, command: "/sorteo", winner: null });
    }

    const mention = winner.winner_telegram_username
      ? `@${String(winner.winner_telegram_username).replace(/^@/, "")}`
      : winner.winner_display_name;

    await sendMessage(
      chatId,
      `Raffle winner for <b>${winner.title}</b>:\n${mention}`
    );
    return res.json({ ok: true, command: "/sorteo", winner });
  }

  await sendMessage(chatId, `I received: <code>${escapeHtml(text || "(empty)")}</code>`);
  return res.json({ ok: true, command: "echo" });
});

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

app.listen(config.port, "0.0.0.0", () => {
  console.log(`Ideadigital Bot backend listening on ${config.port}`);
});
