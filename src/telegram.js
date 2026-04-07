const config = require("./config");
const { currentBot } = require("./botContext");

function resolveBotToken(explicitToken = "") {
  if (explicitToken) {
    return explicitToken;
  }

  const bot = currentBot();
  if (bot && bot.bot_token) {
    return bot.bot_token;
  }

  return config.botToken;
}

function apiUrl(method, explicitToken = "") {
  const token = resolveBotToken(explicitToken);
  return `https://api.telegram.org/bot${token}/${method}`;
}

async function telegramRequest(method, payload = {}, explicitToken = "") {
  const token = resolveBotToken(explicitToken);

  if (!token) {
    return { ok: false, description: "BOT_TOKEN is missing." };
  }

  const response = await fetch(apiUrl(method, token), {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  return response.json();
}

async function sendMessage(chatId, text, extra = {}, explicitToken = "") {
  return telegramRequest("sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "HTML",
    ...extra
  }, explicitToken);
}

async function editMessageText(chatId, messageId, text, extra = {}, explicitToken = "") {
  return telegramRequest("editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    parse_mode: "HTML",
    ...extra
  }, explicitToken);
}

async function deleteMessage(chatId, messageId, explicitToken = "") {
  return telegramRequest("deleteMessage", {
    chat_id: chatId,
    message_id: messageId
  }, explicitToken);
}

async function copyMessage(chatId, fromChatId, messageId, extra = {}, explicitToken = "") {
  return telegramRequest("copyMessage", {
    chat_id: chatId,
    from_chat_id: fromChatId,
    message_id: messageId,
    ...extra
  }, explicitToken);
}

async function answerCallbackQuery(callbackQueryId, text, explicitToken = "") {
  return telegramRequest("answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text,
    show_alert: false
  }, explicitToken);
}

async function getChatMember(chatId, userId, explicitToken = "") {
  return telegramRequest("getChatMember", {
    chat_id: chatId,
    user_id: userId
  }, explicitToken);
}

async function getChatAdministrators(chatId, explicitToken = "") {
  return telegramRequest("getChatAdministrators", {
    chat_id: chatId
  }, explicitToken);
}

async function setWebhook(webhookUrl, explicitToken = "") {
  return telegramRequest("setWebhook", {
    url: webhookUrl,
    secret_token: config.webhookSecret || undefined,
    allowed_updates: ["message", "callback_query"]
  }, explicitToken);
}

async function deleteWebhook(explicitToken = "") {
  return telegramRequest("deleteWebhook", {
    drop_pending_updates: false
  }, explicitToken);
}

async function restrictChatMember(chatId, userId, untilDate, explicitToken = "") {
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
  }, explicitToken);
}

async function allowChatMember(chatId, userId, explicitToken = "") {
  return telegramRequest("restrictChatMember", {
    chat_id: chatId,
    user_id: userId,
    permissions: {
      can_send_messages: true,
      can_send_audios: true,
      can_send_documents: true,
      can_send_photos: true,
      can_send_videos: true,
      can_send_video_notes: true,
      can_send_voice_notes: true,
      can_send_polls: true,
      can_send_other_messages: true,
      can_add_web_page_previews: true,
      can_change_info: false,
      can_invite_users: true,
      can_pin_messages: false,
      can_manage_topics: false
    }
  }, explicitToken);
}

async function banChatMember(chatId, userId, explicitToken = "") {
  return telegramRequest("banChatMember", {
    chat_id: chatId,
    user_id: userId,
    revoke_messages: true
  }, explicitToken);
}

async function setMyCommands(commands, scope = null, languageCode = "", explicitToken = "") {
  const payload = {
    commands: Array.isArray(commands) ? commands : []
  };

  if (scope) {
    payload.scope = scope;
  }

  if (languageCode) {
    payload.language_code = languageCode;
  }

  return telegramRequest("setMyCommands", payload, explicitToken);
}

module.exports = {
  sendMessage,
  editMessageText,
  deleteMessage,
  copyMessage,
  answerCallbackQuery,
  getChatMember,
  getChatAdministrators,
  setWebhook,
  deleteWebhook,
  restrictChatMember,
  allowChatMember,
  banChatMember,
  setMyCommands
};
