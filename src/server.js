const express = require("express");
const crypto = require("crypto");
const config = require("./config");
const {
  getSupportedLocales,
  getLocaleLabel,
  getDefaultGroupSettings,
  normalizeLocale,
  translate
} = require("./i18n");
const {
  sendMessage,
  editMessageText,
  deleteMessage,
  answerCallbackQuery,
  getChatMember,
  getChatAdministrators,
  setWebhook
} = require("./telegram");
const {
  testDbConnection,
  ensureSchema,
  listGroups,
  ensureGroupSettings,
  getGroupSettings,
  updateGroupSettings,
  getUserState,
  setUserState,
  clearUserState,
  createRaffleRound,
  getActiveRaffleRound,
  getRaffleRoundById,
  setRaffleRoundMessage,
  getRaffleEntries,
  addRaffleEntry,
  clearRaffleEntries,
  saveRaffleWinner
} = require("./db");

const app = express();
app.use(express.json());

const ACTION_TO_FIELD = {
  welcome: "welcome_message",
  warning: "warning_message",
  rules: "raffle_rules_text",
  raffle_intro: "raffle_intro_text",
  language: "group_language"
};

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

app.get("/api/public/group/:chatId/raffle", async (req, res) => {
  try {
    res.set("Access-Control-Allow-Origin", config.panelUrl || "*");
    res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.set("Access-Control-Allow-Headers", "Content-Type");

    const chatId = Number(req.params.chatId);
    if (!Number.isFinite(chatId)) {
      return res.status(400).json({ ok: false, message: "Invalid chat id." });
    }

    const settings = await ensureGroupSettings(chatId);
    const round = await getActiveRaffleRound(chatId);
    const entries = round ? await getRaffleEntries(round.id) : [];

    return res.json({
      ok: true,
      group: {
        chat_id: chatId,
        chat_title: settings.chat_title || String(chatId)
      },
      raffle: {
        active: Boolean(round && round.status === "active"),
        round_id: round ? round.id : null,
        count: entries.length,
        entries: entries.map((entry) => ({
          user_id: entry.user_id,
          username: entry.username || null,
          first_name: entry.first_name || null,
          joined_at: entry.joined_at || null
        }))
      }
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.options("/api/public/group/:chatId/raffle", (req, res) => {
  res.set("Access-Control-Allow-Origin", config.panelUrl || "*");
  res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");
  return res.status(204).end();
});

app.get("/api/panel/group/:chatId/settings", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const chatId = Number(req.params.chatId);
    if (!Number.isFinite(chatId)) {
      return res.status(400).json({ ok: false, message: "Invalid chat id." });
    }

    const settings = await ensureGroupSettings(chatId);
    return res.json({ ok: true, settings });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/groups", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const groups = await listGroups();
    return res.json({ ok: true, groups });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/api/panel/group/:chatId/settings", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const chatId = Number(req.params.chatId);
    if (!Number.isFinite(chatId)) {
      return res.status(400).json({ ok: false, message: "Invalid chat id." });
    }

    const current = await ensureGroupSettings(chatId);
    const body = req.body || {};
    const patch = {};

    if (typeof body.group_language === "string") {
      patch.group_language = normalizeLocale(body.group_language);
    }

    if (typeof body.welcome_message === "string") {
      patch.welcome_message = body.welcome_message.trim();
    }

    if (typeof body.warning_message === "string") {
      patch.warning_message = body.warning_message.trim();
    }

    if (typeof body.raffle_rules_text === "string") {
      patch.raffle_rules_text = body.raffle_rules_text.trim();
    }

    if (typeof body.raffle_intro_text === "string") {
      patch.raffle_intro_text = body.raffle_intro_text.trim();
    }

    const settings = await updateGroupSettings(chatId, patch);
    return res.json({ ok: true, previous: current, settings });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
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
  try {
    if (!isSecretValid(req)) {
      return res.status(403).json({ ok: false, message: "Invalid secret token." });
    }

    const update = req.body || {};

    if (update.callback_query) {
      await handleCallbackQuery(update.callback_query);
      return res.json({ ok: true, update: "callback_query" });
    }

    if (update.message) {
      await handleMessage(update.message);
      return res.json({ ok: true, update: "message" });
    }

    return res.json({ ok: true, skipped: true });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

function isSecretValid(req) {
  if (!config.webhookSecret) {
    return true;
  }

  const incoming = req.get("X-Telegram-Bot-Api-Secret-Token") || "";
  const expected = config.webhookSecret;

  if (incoming.length !== expected.length) {
    return false;
  }

  return crypto.timingSafeEqual(Buffer.from(incoming), Buffer.from(expected));
}

function isPanelTokenValid(req) {
  if (!config.panelApiToken) {
    return false;
  }

  const incoming = req.get("X-Panel-Token") || "";
  const expected = config.panelApiToken;

  if (incoming.length !== expected.length) {
    return false;
  }

  return crypto.timingSafeEqual(Buffer.from(incoming), Buffer.from(expected));
}

async function handleMessage(message) {
  const chat = message.chat || {};
  const from = message.from || {};
  const text = (message.text || "").trim();

  if (chat.type === "private") {
    if (text) {
      await handlePrivateText(message, text);
    }
    return;
  }

  await ensureGroupSettings(chat.id, chat.title || "");

  if (Array.isArray(message.new_chat_members) && message.new_chat_members.length > 0) {
    await handleWelcomeMessage(chat, message.new_chat_members);
    return;
  }

  if (!text.startsWith("/")) {
    return;
  }

  const command = extractCommand(text);

  if (command === "/help") {
    const settings = await ensureGroupSettings(chat.id, chat.title || "");
    await sendMessage(chat.id, buildGroupHelpText(settings));
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/panelbot") {
    await handlePanelBotCommand(chat, from);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/reglas") {
    const settings = await ensureGroupSettings(chat.id, chat.title || "");
    await sendMessage(chat.id, settings.raffle_rules_text || tForSettings(settings, "rules_empty"));
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/staff") {
    await handleStaffCommand(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  if (command === "/nsorteo") {
    await handleNewRaffle(chat, from);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/sortear") {
    await handleDrawWinner(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/reset") {
    await handleResetRaffle(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/warn") {
    await handleWarnCommand(chat.id, chat.title || "", message);
    await cleanupCommandMessage(message);
  }
}

async function handlePrivateText(message, text) {
  const from = message.from || {};
  const chatId = message.chat.id;
  const command = extractCommand(text);

  if (command === "/start") {
    const startArg = text.split(/\s+/)[1] || "";

    if (startArg.indexOf("manage_") === 0) {
      const targetChatId = Number(startArg.replace("manage_", ""));
      await handlePrivateManageStart(chatId, from.id, targetChatId);
      return;
    }

    await sendMessage(chatId, buildPrivateWelcomeText("es"), buildPrivateHomeKeyboard("es"));
    return;
  }

  if (command === "/help") {
    await sendMessage(chatId, buildPrivateHelpText("es"), buildPrivateHomeKeyboard("es"));
    return;
  }

  if (command === "/settings") {
    await showPrivateGroups(chatId, from.id, "es");
    return;
  }

  const state = await getUserState(from.id);
  if (!state) {
    return;
  }

  const targetChatId = Number(state.group_chat_id);
  const actionKey = state.action_key;
  const field = ACTION_TO_FIELD[actionKey];

  if (!field) {
    await clearUserState(from.id);
    return;
  }

  if (!(await isGroupAdmin(targetChatId, from.id))) {
    await clearUserState(from.id);
    const fallbackSettings = await ensureGroupSettings(targetChatId);
    await sendMessage(chatId, tForSettings(fallbackSettings, "private_no_longer_admin"));
    return;
  }

  let updated;
  if (actionKey === "language") {
    updated = await updateGroupSettings(targetChatId, {
      group_language: normalizeLocale(text)
    });
  } else {
    updated = await updateGroupSettings(targetChatId, {
      [field]: text
    });
  }

  await clearUserState(from.id);
  await sendMessage(
    chatId,
    `${escapeHtml(tForSettings(updated, "private_saved", { group: updated.chat_title || "Grupo" }))}\n\n${buildSettingPreview(actionKey, actionKey === "language" ? getLocaleLabel(updated.group_language) : updated[field], updated)}`,
    buildManageKeyboard(targetChatId, updated)
  );
}

async function handlePrivateManageStart(privateChatId, userId, targetChatId) {
  const member = await getChatMember(targetChatId, userId);

  if (!member.ok || !isMemberAdminStatus(member.result.status)) {
    await sendMessage(privateChatId, tForLocale("es", "private_not_admin"));
    return;
  }

  const title = member.result.chat ? member.result.chat.title : "";
  const settings = await ensureGroupSettings(targetChatId, title);
  await sendMessage(
    privateChatId,
    buildConfigMenuText(settings),
    buildConfigCategoryKeyboard(targetChatId, settings, "main")
  );
}

async function handleCallbackQuery(callback) {
  const data = callback.data || "";

  if (data.indexOf("raffle_join:") === 0) {
    await handleRaffleJoin(callback);
    return;
  }

  if (data.indexOf("home:") === 0) {
    await handleHomeCallback(callback);
    return;
  }

  if (data.indexOf("cfgmenu:") === 0) {
    await handleConfigMenuCallback(callback);
    return;
  }

  if (data.indexOf("cfg:") === 0) {
    await handleConfigCallback(callback);
    return;
  }
}

async function handleHomeCallback(callback) {
  const action = String(callback.data || "").split(":")[1] || "";
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;
  const locale = "es";

  if (action === "groups") {
    await answerCallbackQuery(callback.id, "Abriendo grupos");
    await showPrivateGroups(privateChatId, userId, locale);
    return;
  }

  if (action === "languages") {
    await answerCallbackQuery(callback.id, "Idiomas");
    await sendMessage(
      privateChatId,
      `<b>${escapeHtml(tForLocale(locale, "preview_language"))}</b>\n${escapeHtml(formatLocaleOptions(locale))}`,
      buildPrivateHomeKeyboard(locale)
    );
    return;
  }

  if (action === "help") {
    await answerCallbackQuery(callback.id, tForLocale(locale, "preview_ready"));
    await sendMessage(privateChatId, buildPrivateHelpText(locale), buildPrivateHomeKeyboard(locale));
  }
}

async function showPrivateGroups(privateChatId, userId, locale = "es") {
  const groups = await listGroups();
  const available = [];

  for (const group of groups) {
    if (!group || !Number.isFinite(group.chat_id)) {
      continue;
    }

    const member = await getChatMember(group.chat_id, userId);
    if (member.ok && isMemberAdminStatus(member.result.status)) {
      available.push(group);
    }
  }

  if (!available.length) {
    await sendMessage(
      privateChatId,
      "Aun no tienes grupos sincronizados. Agrega el bot a un grupo y usa /PanelBot dentro del grupo para empezar.",
      buildPrivateHomeKeyboard(locale)
    );
    return;
  }

  const keyboard = {
    reply_markup: {
      inline_keyboard: available.map((group) => [
        {
          text: group.chat_title || "Grupo sin nombre",
          url: `https://t.me/${config.botUsername}?start=manage_${group.chat_id}`
        }
      ])
    }
  };

  await sendMessage(
    privateChatId,
    "<b>Configuracion de grupos</b>\nSelecciona un grupo para administrarlo.",
    keyboard
  );
}

async function handleConfigMenuCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const page = parts[1] || "main";
  const targetChatId = Number(parts[2]);
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  if (page === "close") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "cancelled"));
    await sendMessage(privateChatId, "Panel cerrado.", buildPrivateHomeKeyboard("es"));
    return;
  }

  if (page === "pending") {
    await answerCallbackQuery(callback.id, "Disponible pronto");
    return;
  }

  const pageToRender = page === "more" ? "more" : "main";
  await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
  await sendMessage(
    privateChatId,
    buildConfigMenuText(settings),
    buildConfigCategoryKeyboard(targetChatId, settings, pageToRender)
  );
}

async function handleConfigCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const action = parts[2];
  const userId = callback.from.id;
  const privateChatId = callback.message.chat.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  if (action === "preview") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await sendMessage(
      privateChatId,
      buildConfigPreview(settings),
      buildManageKeyboard(targetChatId, settings)
    );
    return;
  }

  if (action === "cancel") {
    await clearUserState(userId);
    await answerCallbackQuery(callback.id, tForSettings(settings, "cancelled"));
    await sendMessage(privateChatId, tForSettings(settings, "edition_cancelled"), buildManageKeyboard(targetChatId, settings));
    return;
  }

  await setUserState(userId, targetChatId, action);
  await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
  await sendMessage(privateChatId, buildEditPrompt(action, settings));
}

async function handleRaffleJoin(callback) {
  if (!(await isDatabaseAvailable())) {
    await answerCallbackQuery(callback.id, tForLocale("es", "database_unavailable"));
    return;
  }

  const rawRoundId = String(callback.data || "").split(":")[1] || "";
  let round = null;

  if (rawRoundId.indexOf(":") >= 0) {
    round = await getRaffleRoundById(rawRoundId);
  } else if (callback.message && callback.message.chat) {
    round = await getActiveRaffleRound(callback.message.chat.id);
  }

  if (!round || round.status !== "active") {
    const localeSettings = callback.message && callback.message.chat
      ? await ensureGroupSettings(callback.message.chat.id)
      : { group_language: "es" };
    await answerCallbackQuery(callback.id, tForSettings(localeSettings, "raffle_inactive"));
    return;
  }

  const roundId = round.id;
  const entryResult = await addRaffleEntry(roundId, callback.from);
  const entries = await getRaffleEntries(roundId);
  const settings = await ensureGroupSettings(round.chat_id);
  const messageText = buildRaffleMessage(round.chat_id, settings, entries);
  const replyMarkup = buildRaffleKeyboard(roundId, entries.length, settings);

  if (round.message_id) {
    await editMessageText(round.chat_id, round.message_id, messageText, replyMarkup);
  }

  if (entryResult.duplicate) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "duplicate_entry"));
    return;
  }

  await answerCallbackQuery(callback.id, tForSettings(settings, "join_success", { count: entries.length }));
}

async function handlePanelBotCommand(chat, from) {
  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");

  if (!config.botUsername) {
    await sendMessage(
      chat.id,
      tForSettings(settings, "panelbot_missing_username")
    );
    return;
  }

  const manageUrl = `https://t.me/${config.botUsername}?start=manage_${chat.id}`;
  await sendMessage(
    chat.id,
    tForSettings(settings, "panelbot_open_text"),
    {
      reply_markup: {
        inline_keyboard: [
          [{ text: tForSettings(settings, "panelbot_open_button"), url: manageUrl }]
        ]
      }
    }
  );
}

async function handleNewRaffle(chat, from) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chat.id, tForLocale("es", "raffle_db_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const round = await createRaffleRound(chat.id, from.id);
  const entries = await getRaffleEntries(round.id);
  const response = await sendMessage(
    chat.id,
    buildRaffleMessage(chat.id, settings, entries),
    buildRaffleKeyboard(round.id, entries.length, settings)
  );

  if (response.ok && response.result && response.result.message_id) {
    await setRaffleRoundMessage(round.id, response.result.message_id);
  }
}

async function handleDrawWinner(chatId, chatTitle = "") {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "raffle_db_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chatId, chatTitle || "");
  const round = await getActiveRaffleRound(chatId);

  if (!round) {
    await sendMessage(chatId, tForSettings(settings, "no_active_raffle"));
    return;
  }

  const entries = await getRaffleEntries(round.id);
  if (!entries.length) {
    await sendMessage(chatId, tForSettings(settings, "no_raffle_entries"));
    return;
  }

  const winner = entries[Math.floor(Math.random() * entries.length)];
  await saveRaffleWinner(round.id, winner);

  const mention = winner.username
    ? `@${String(winner.username).replace(/^@/, "")}`
    : escapeHtml(winner.first_name || String(winner.user_id));

  const participantCount = String(entries.length).padStart(2, "0");

  await sendMessage(
    chatId,
    tForSettings(settings, "winner_message", { count: participantCount, winner: `<b>${mention}</b>` })
  );
}

async function handleResetRaffle(chatId, chatTitle = "") {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "raffle_db_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chatId, chatTitle || "");
  const round = await getActiveRaffleRound(chatId);

  if (!round) {
    await sendMessage(chatId, tForSettings(settings, "no_raffle_to_reset"));
    return;
  }

  await clearRaffleEntries(round.id);
  const entries = await getRaffleEntries(round.id);

  if (round.message_id) {
    await editMessageText(
      chatId,
      round.message_id,
      buildRaffleMessage(chatId, settings, entries),
      buildRaffleKeyboard(round.id, 0, settings)
    );
  }

  await sendMessage(chatId, tForSettings(settings, "raffle_reset_done"));
}

async function handleWarnCommand(chatId, chatTitle = "", message) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "database_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chatId, chatTitle || "");

  if (!message.reply_to_message || !message.reply_to_message.from) {
    await sendMessage(chatId, tForSettings(settings, "warn_reply_required"));
    return;
  }

  const target = message.reply_to_message.from;
  const rendered = renderTemplate(settings.warning_message, {
    first_name: target.first_name || tForSettings(settings, "user_fallback"),
    username: target.username ? `@${target.username}` : target.first_name || tForSettings(settings, "user_fallback"),
    group: message.chat.title || tForSettings(settings, "group_title_fallback")
  });

  await sendMessage(chatId, rendered);
}

async function handleStaffCommand(chatId, chatTitle = "") {
  const settings = await ensureGroupSettings(chatId, chatTitle || "");
  const admins = await getChatAdministrators(chatId);
  if (!admins.ok || !Array.isArray(admins.result)) {
    await sendMessage(chatId, tForSettings(settings, "staff_unavailable"));
    return;
  }

  const lines = [`<b>${escapeHtml(tForSettings(settings, "group_staff_title"))}</b>`];

  admins.result.forEach((item) => {
    const user = item.user || {};
    const title = classifyAdmin(item, settings);
    const name = user.username ? `@${user.username}` : [user.first_name, user.last_name].filter(Boolean).join(" ");
    lines.push(`- <b>${escapeHtml(title)}</b>: ${escapeHtml(name || String(user.id || ""))}`);
  });

  await sendMessage(chatId, lines.join("\n"));
}

async function handleWelcomeMessage(chat, newMembers) {
  if (!(await isDatabaseAvailable())) {
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const template = settings.welcome_message || getDefaultGroupSettings(getGroupLocale(settings)).welcome_message;

  for (const user of newMembers) {
    const text = renderTemplate(template, {
      first_name: user.first_name || tForSettings(settings, "user_fallback"),
      full_name: [user.first_name, user.last_name].filter(Boolean).join(" "),
      username: user.username ? `@${user.username}` : user.first_name || tForSettings(settings, "user_fallback"),
      group: chat.title || tForSettings(settings, "group_title_fallback")
    });

    await sendMessage(chat.id, text);
  }
}

async function isGroupAdmin(chatId, userId) {
  const member = await getChatMember(chatId, userId);
  return member.ok && isMemberAdminStatus(member.result.status);
}

async function cleanupCommandMessage(message) {
  const chat = message && message.chat ? message.chat : null;
  const messageId = message ? message.message_id : null;

  if (!chat || !messageId || chat.type === "private") {
    return;
  }

  try {
    await deleteMessage(chat.id, messageId);
  } catch (_error) {
    return;
  }
}

function isMemberAdminStatus(status) {
  return status === "administrator" || status === "creator";
}

function buildManageKeyboard(chatId, settings = { group_language: "es" }) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: tForSettings(settings, "edit_welcome"), callback_data: `cfg:${chatId}:welcome` },
          { text: tForSettings(settings, "edit_warning"), callback_data: `cfg:${chatId}:warning` }
        ],
        [
          { text: tForSettings(settings, "edit_rules"), callback_data: `cfg:${chatId}:rules` },
          { text: tForSettings(settings, "edit_raffle_text"), callback_data: `cfg:${chatId}:raffle_intro` }
        ],
        [
          { text: tForSettings(settings, "edit_language"), callback_data: `cfg:${chatId}:language` },
          { text: tForSettings(settings, "preview_button"), callback_data: `cfg:${chatId}:preview` }
        ],
        [
          { text: tForSettings(settings, "cancel_button"), callback_data: `cfg:${chatId}:cancel` }
        ]
      ]
    }
  };
}

function buildConfigMenuText(settings) {
  return [
    "<b>CONFIGURACION</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    "Elige cual de los ajustes quieres editar."
  ].join("\n");
}

function buildConfigCategoryKeyboard(chatId, settings, page = "main") {
  if (page === "more") {
    return {
      reply_markup: {
        inline_keyboard: [
          [{ text: "📁 Temas / Topics", callback_data: `cfgmenu:pending:${chatId}` }],
          [{ text: "🔤 Palabras Prohibidas", callback_data: `cfgmenu:pending:${chatId}` }],
          [{ text: "🕘 Mensajes Recurrentes", callback_data: `cfgmenu:pending:${chatId}` }],
          [{ text: "👥 Gestion de Miembros", callback_data: `cfgmenu:pending:${chatId}` }],
          [{ text: "🫥 Usuarios enmascarados", callback_data: `cfgmenu:pending:${chatId}` }],
          [{ text: "📱 Comandos Personales", callback_data: `cfgmenu:pending:${chatId}` }],
          [
            { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` },
            { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` },
            { text: "🌐 Lang", callback_data: `cfg:${chatId}:language` }
          ]
        ]
      }
    };
  }

  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "📜 Reglamento", callback_data: `cfg:${chatId}:rules` },
          { text: "🛡️ Antispam", callback_data: `cfgmenu:pending:${chatId}` }
        ],
        [
          { text: "💬 Bienvenida", callback_data: `cfg:${chatId}:welcome` },
          { text: "🌊 Anti-flood", callback_data: `cfgmenu:pending:${chatId}` }
        ],
        [
          { text: "🧠 Captcha", callback_data: `cfgmenu:pending:${chatId}` },
          { text: "🧪 Filtros", callback_data: `cfgmenu:pending:${chatId}` }
        ],
        [
          { text: "🚨 Advertencias", callback_data: `cfg:${chatId}:warning` },
          { text: "🎁 Sorteo", callback_data: `cfg:${chatId}:raffle_intro` }
        ],
        [
          { text: "👥 Staff", callback_data: `cfgmenu:pending:${chatId}` },
          { text: "🔗 Enlace del grupo", callback_data: `cfgmenu:pending:${chatId}` }
        ],
        [
          { text: "🌐 Lang", callback_data: `cfg:${chatId}:language` },
          { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` },
          { text: "▶️ Mas", callback_data: `cfgmenu:more:${chatId}` }
        ]
      ]
    }
  };
}

function buildPrivateWelcomeText(locale = "es") {
  return [
    `<b>${escapeHtml(tForLocale(locale, "private_welcome_title"))}</b>`,
    "",
    "Este es tu centro privado para administrar el bot y tus grupos.",
    "",
    "Agregame a un grupo como administrador y usa los botones de abajo para configurar idiomas, mensajes y sorteos."
  ].join("\n");
}

function buildPrivateHomeKeyboard(locale = "es") {
  const addUrl = config.botUsername ? `https://t.me/${config.botUsername}?startgroup=true` : config.panelUrl;

  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "➕ Agregame a un grupo ➕", url: addUrl }
        ],
        [
          { text: "⚙️ Configuracion de grupos", callback_data: "home:groups" }
        ],
        [
          { text: "🧩 Panel web", url: `${config.panelUrl}/dashboard.php` },
          { text: "🌐 Languages", callback_data: "home:languages" }
        ],
        [
          { text: "ℹ️ Ayuda", callback_data: "home:help" }
        ]
      ]
    }
  };
}

function buildPrivateHelpText(locale = "es") {
  return [
    `<b>${escapeHtml(tForLocale(locale, "private_help_title"))}</b>`,
    "",
    escapeHtml(tForLocale(locale, "private_help_body"))
  ].join("\n");
}

function buildGroupHelpText(settings = { group_language: "es" }) {
  return [
    `<b>${escapeHtml(tForSettings(settings, "group_help_title"))}</b>`,
    escapeHtml(tForSettings(settings, "group_help_body"))
  ].join("\n");
}

function buildRaffleMessage(chatId, settings, entries) {
  const intro = settings.raffle_intro_text || getDefaultGroupSettings(getGroupLocale(settings)).raffle_intro_text;
  const listUrl = buildRaffleListUrl(chatId);
  const listLine = listUrl
    ? `<a href="${escapeHtml(listUrl)}">${escapeHtml(tForSettings(settings, "raffle_view_list"))}</a>`
    : escapeHtml(tForSettings(settings, "raffle_view_list"));

  return [
    `<b>${escapeHtml(tForSettings(settings, "raffle_active_title"))}</b>`,
    escapeHtml(intro),
    "",
    `<b>${escapeHtml(tForSettings(settings, "raffle_registered_label", { count: entries.length }))}</b>`,
    listLine,
    "",
    escapeHtml(tForSettings(settings, "raffle_rules_hint"))
  ].join("\n");
}

function buildRaffleKeyboard(roundId, count, settings = { group_language: "es" }) {
  return {
    reply_markup: {
      inline_keyboard: [
        [{ text: tForSettings(settings, "join_button", { count: String(count).padStart(2, "0") }), callback_data: `raffle_join:${roundId}` }]
      ]
    }
  };
}

function buildEditPrompt(action, settings) {
  const labels = {
    welcome: tForSettings(settings, "prompt_welcome"),
    warning: tForSettings(settings, "prompt_warning"),
    rules: tForSettings(settings, "prompt_rules"),
    raffle_intro: tForSettings(settings, "prompt_raffle_intro"),
    language: `${tForSettings(settings, "prompt_language")}\n\n${formatLocaleOptions(getGroupLocale(settings))}`
  };

  const currentValue = {
    welcome: settings.welcome_message,
    warning: settings.warning_message,
    rules: settings.raffle_rules_text,
    raffle_intro: settings.raffle_intro_text,
    language: `${getGroupLocale(settings).toUpperCase()} - ${getLocaleLabel(getGroupLocale(settings))}`
  };

  return [
    labels[action] || "Send the new content.",
    "",
    `<b>${escapeHtml(tForSettings(settings, "current_value"))}</b>`,
    escapeHtml(currentValue[action] || "(empty)"),
    "",
    action === "language" ? "" : escapeHtml(tForSettings(settings, "placeholders"))
  ].join("\n");
}

function buildConfigPreview(settings) {
  return [
    `<b>${escapeHtml(tForSettings(settings, "preview_group"))}:</b> ${escapeHtml(settings.chat_title || String(settings.chat_id))}`,
    "",
    `<b>${escapeHtml(tForSettings(settings, "preview_welcome"))}:</b>\n${escapeHtml(settings.welcome_message || "")}`,
    "",
    `<b>${escapeHtml(tForSettings(settings, "preview_warning"))}:</b>\n${escapeHtml(settings.warning_message || "")}`,
    "",
    `<b>${escapeHtml(tForSettings(settings, "preview_rules"))}:</b>\n${escapeHtml(settings.raffle_rules_text || "")}`,
    "",
    `<b>${escapeHtml(tForSettings(settings, "preview_raffle_text"))}:</b>\n${escapeHtml(settings.raffle_intro_text || "")}`,
    "",
    `<b>${escapeHtml(tForSettings(settings, "preview_language"))}:</b>\n${escapeHtml(getLocaleLabel(getGroupLocale(settings)))}`
  ].join("\n");
}

function buildSettingPreview(action, value, settings = { group_language: "es" }) {
  const labels = {
    welcome: tForSettings(settings, "preview_welcome"),
    warning: tForSettings(settings, "preview_warning"),
    rules: tForSettings(settings, "preview_rules"),
    raffle_intro: tForSettings(settings, "preview_raffle_text"),
    language: tForSettings(settings, "preview_language")
  };

  return `<b>${escapeHtml(labels[action] || action)}</b>\n${escapeHtml(value || "")}`;
}

function getGroupLocale(settings) {
  return normalizeLocale(settings && settings.group_language ? settings.group_language : "es");
}

function tForLocale(locale, key, vars = {}) {
  return translate(normalizeLocale(locale), key, vars);
}

function tForSettings(settings, key, vars = {}) {
  return tForLocale(getGroupLocale(settings), key, vars);
}

function formatLocaleOptions(locale) {
  const current = normalizeLocale(locale);
  return Object.entries(getSupportedLocales())
    .map(([code, label]) => `${code === current ? "•" : "◦"} ${code.toUpperCase()} - ${label}`)
    .join("\n");
}

function renderTemplate(template, values) {
  return String(template || "")
    .replace(/\{first_name\}/g, values.first_name || "")
    .replace(/\{full_name\}/g, values.full_name || values.first_name || "")
    .replace(/\{username\}/g, values.username || values.first_name || "")
    .replace(/\{group\}/g, values.group || "");
}

function extractCommand(text) {
  const first = String(text || "").split(/\s+/)[0] || "";
  const base = first.split("@")[0];
  return base.toLowerCase();
}

function classifyAdmin(item, settings = { group_language: "es" }) {
  const status = item.status;
  const customTitle = item.custom_title;

  if (status === "creator") {
    return tForSettings(settings, "founder");
  }

  if (customTitle) {
    return customTitle;
  }

  return tForSettings(settings, "administrator");
}

function formatEntryName(entry) {
  return entry.username ? `@${String(entry.username).replace(/^@/, "")}` : entry.first_name || String(entry.user_id);
}

function buildRaffleListUrl(chatId) {
  if (!config.panelUrl) {
    return "";
  }

  return `${config.panelUrl}/raffle_live.php?chat_id=${encodeURIComponent(String(chatId))}`;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

async function isDatabaseAvailable() {
  const status = await testDbConnection();
  return Boolean(status && status.ok);
}

async function startServer() {
  const schemaStatus = await ensureSchema();
  console.log(schemaStatus.message);

  app.listen(config.port, "0.0.0.0", () => {
    console.log(`Ideadigital Bot backend listening on ${config.port}`);
  });
}

startServer().catch((error) => {
  console.error(error);
  process.exit(1);
});
