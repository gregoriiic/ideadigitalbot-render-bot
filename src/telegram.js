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

async function editMessageText(chatId, messageId, text, extra = {}) {
  return telegramRequest("editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    parse_mode: "HTML",
    ...extra
  });
}

async function answerCallbackQuery(callbackQueryId, text) {
  return telegramRequest("answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
    show_alert: false
  });
}

async function getChatMember(chatId, userId) {
  return telegramRequest("getChatMember", {
    chat_id: chatId,
    user_id: userId
  });
}

async function getChatAdministrators(chatId) {
  return telegramRequest("getChatAdministrators", {
    chat_id: chatId
  });
}

async function setWebhook(webhookUrl) {
  return telegramRequest("setWebhook", {
    url: webhookUrl,
    secret_token: config.webhookSecret || undefined,
    allowed_updates: ["message", "callback_query"]
  });
}

module.exports = {
  sendMessage,
  editMessageText,
  answerCallbackQuery,
  getChatMember,
  getChatAdministrators,
  setWebhook
};
