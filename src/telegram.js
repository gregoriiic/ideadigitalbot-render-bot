const config = require("./config");

function apiUrl(method) {
  return `https://api.telegram.org/bot${config.botToken}/${method}`;
}

async function telegramRequest(method, payload = {}) {
  if (!config.botToken) {
    return { ok: false, description: "BOT_TOKEN is missing." };
  }

  const response = await fetch(apiUrl(method), {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  return response.json();
}

async function sendMessage(chatId, text, extra = {}) {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "HTML",
    ...extra
  });
}

async function setWebhook(webhookUrl) {
  return telegramRequest("setWebhook", {
    url: webhookUrl,
    secret_token: config.webhookSecret || undefined,
    allowed_updates: ["message"]
  });
}

module.exports = {
  sendMessage,
  setWebhook
};
