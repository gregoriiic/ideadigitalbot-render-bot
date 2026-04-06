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

async function deleteMessage(chatId, messageId) {
  return telegramRequest("deleteMessage", {
    chat_id: chatId,
    message_id: messageId
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

async function restrictChatMember(chatId, userId, untilDate) {
  return telegramRequest("restrictChatMember", {
    chat_id: chatId,
    user_id: userId,
    until_date: untilDate,
    permissions: {
      can_send_messages: false,
      can_send_audios: false,
      can_send_documents: false,
      can_send_photos: false,
      can_send_videos: false,
      can_send_video_notes: false,
      can_send_voice_notes: false,
      can_send_polls: false,
      can_send_other_messages: false,
      can_add_web_page_previews: false,
      can_change_info: false,
      can_invite_users: false,
      can_pin_messages: false,
      can_manage_topics: false
    }
  });
}

async function banChatMember(chatId, userId) {
  return telegramRequest("banChatMember", {
    chat_id: chatId,
    user_id: userId,
    revoke_messages: true
  });
}

module.exports = {
  sendMessage,
  editMessageText,
  deleteMessage,
  answerCallbackQuery,
  getChatMember,
  getChatAdministrators,
  setWebhook,
  restrictChatMember,
  banChatMember
};
