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
  copyMessage,
  answerCallbackQuery,
  getChatMember,
  getChatAdministrators,
  setWebhook,
  restrictChatMember,
  banChatMember
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
  getUserProfile,
  updateUserProfile,
  createRaffleRound,
  getActiveRaffleRound,
  getRaffleRoundById,
  setRaffleRoundMessage,
  getRaffleEntries,
  addRaffleEntry,
  clearRaffleEntries,
  saveRaffleWinner,
  createSupportTicket,
  attachSupportTicketMessage,
  getSupportTicketByReply,
  getOpenSupportTicketByUser,
  updateSupportTicket,
  closeSupportTicket
} = require("./db");

const app = express();
app.use(express.json());

const ACTION_TO_FIELD = {
  welcome: "welcome_message",
  warning: "warning_message",
  rules: "group_rules_text",
  raffle_intro: "raffle_intro_text",
  language: "group_language",
  antispam_duration: "antispam_duration_text",
  group_link_value: "group_link_value"
};

const spamTracker = new Map();
const activeTicketTimers = new Map();
const TICKET_INACTIVITY_MS = 10 * 60 * 1000;

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

    if (typeof body.group_rules_text === "string") {
      patch.group_rules_text = body.group_rules_text.trim();
    }

    if (typeof body.raffle_intro_text === "string") {
      patch.raffle_intro_text = body.raffle_intro_text.trim();
    }

    if (typeof body.antispam_enabled === "boolean") {
      patch.antispam_enabled = body.antispam_enabled;
    }

    if (typeof body.antispam_action === "string") {
      patch.antispam_action = body.antispam_action.trim();
    }

    if (typeof body.antispam_duration_text === "string") {
      patch.antispam_duration_text = body.antispam_duration_text.trim();
    }

    if (typeof body.group_link_enabled === "boolean") {
      patch.group_link_enabled = body.group_link_enabled;
    }

    if (typeof body.group_link_value === "string") {
      patch.group_link_value = body.group_link_value.trim();
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
    } else if (hasTicketRelayContent(message)) {
      await handlePrivateNonText(message);
    }
    return;
  }

  await ensureGroupSettings(chat.id, chat.title || "");

  if (Array.isArray(message.new_chat_members) && message.new_chat_members.length > 0) {
    await handleWelcomeMessage(chat, message.new_chat_members);
    return;
  }

  if (message.reply_to_message && (!text || !text.startsWith("/"))) {
    const replied = await handleSupportReply(chat, from, message);
    if (replied) {
      return;
    }
  }

  if (!text.startsWith("/")) {
    await maybeHandleAntispam(chat, from, message);
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

  if (command === "/reglas" || command === "/rules") {
    const settings = await ensureGroupSettings(chat.id, chat.title || "");
    await sendMessage(chat.id, settings.group_rules_text || tForSettings(settings, "rules_empty"));
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/staff") {
    await handleStaffCommand(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/link") {
    await handleGroupLinkCommand(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/ticket") {
    await handleTicketCommand(chat, from, message);
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

async function handlePrivateNonText(message) {
  const from = message.from || {};
  const state = await getUserState(from.id);

  if (state && state.action_key === "await_ticket_message") {
    await handlePrivateTicketMessage(message, "", state);
    return;
  }

  if (!state) {
    await handleOpenPrivateTicketContinuation(message, "");
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

    await showPrivateStart(chatId, from.id, "es");
    return;
  }

  if (command === "/help") {
    await sendMessage(chatId, buildPrivateHelpText("es"), buildPrivateHomeKeyboard("es"));
    return;
  }

  if (command === "/settings" || command === "/panel" || command === "/panelbot") {
    await showPrivateGroups(chatId, from.id, "es");
    return;
  }

  const state = await getUserState(from.id);
  if (!state) {
    await handleOpenPrivateTicketContinuation(message, text);
    return;
  }

  if (state.action_key === "await_ticket_message") {
    await handlePrivateTicketMessage(message, text, state);
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
  const panelMessageId = Number(state.panel_message_id);
  const confirmationText = [
    `<b>${escapeHtml(tForSettings(updated, "private_saved", { group: updated.chat_title || "Grupo" }))}</b>`,
    "",
    buildSettingPreview(
      actionKey,
      actionKey === "language" ? getLocaleLabel(updated.group_language) : updated[field],
      updated
    ),
    "",
    buildConfigMenuText(updated)
  ].join("\n");

  const edited = await editPanelMessage(
    chatId,
    panelMessageId,
    confirmationText,
    buildConfigCategoryKeyboard(targetChatId, updated, "main")
  );

  if (!edited) {
    await sendMessage(
      chatId,
      confirmationText,
      buildConfigCategoryKeyboard(targetChatId, updated, "main")
    );
  }
}

async function handlePrivateManageStart(privateChatId, userId, targetChatId, panelMessageId = null) {
  const member = await getChatMember(targetChatId, userId);

  if (!member.ok || !isMemberAdminStatus(member.result.status)) {
    await sendMessage(privateChatId, tForLocale("es", "private_not_admin"));
    return;
  }

  const title = member.result.chat ? member.result.chat.title : "";
  const settings = await ensureGroupSettings(targetChatId, title);
  const edited = await editPanelMessage(
    privateChatId,
    panelMessageId,
    buildConfigMenuText(settings),
    buildConfigCategoryKeyboard(targetChatId, settings, "main")
  );

  if (!edited) {
    await sendMessage(
      privateChatId,
      buildConfigMenuText(settings),
      buildConfigCategoryKeyboard(targetChatId, settings, "main")
    );
  }
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

  if (data.indexOf("supportpick:") === 0) {
    await handleSupportPickCallback(callback);
    return;
  }

  if (data.indexOf("supportcfg:") === 0) {
    await handleSupportConfigCallback(callback);
    return;
  }

  if (data.indexOf("pickgroup:") === 0) {
    await handlePickGroupCallback(callback);
    return;
  }

  if (data.indexOf("cfgmenu:") === 0) {
    await handleConfigMenuCallback(callback);
    return;
  }

  if (data.indexOf("antispam:") === 0) {
    await handleAntispamActionCallback(callback);
    return;
  }

  if (data.indexOf("grouplink:") === 0) {
    await handleGroupLinkActionCallback(callback);
    return;
  }

  if (data.indexOf("langpick:") === 0) {
    await handleLanguagePickCallback(callback);
    return;
  }

  if (data.indexOf("cfgedit:") === 0) {
    await handleConfigEditCallback(callback);
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
  const panelMessageId = callback.message.message_id;
  const locale = "es";

  if (action === "groups") {
    await answerCallbackQuery(callback.id, "Abriendo grupos");
    await showPrivateGroups(privateChatId, userId, locale, panelMessageId);
    return;
  }

  if (action === "languages") {
    await answerCallbackQuery(callback.id, "Idiomas");
    await editMessageText(
      privateChatId,
      panelMessageId,
      `<b>${escapeHtml(tForLocale(locale, "preview_language"))}</b>\n${escapeHtml(formatLocaleOptions(locale))}`,
      buildPrivateHomeKeyboard(locale)
    );
    return;
  }

  if (action === "support") {
    const groups = await getManageableGroups(userId);
    const profile = await getUserProfile(userId);
    await answerCallbackQuery(callback.id, "Customer service");
    await editMessageText(
      privateChatId,
      panelMessageId,
      buildSupportGroupIntroText(locale, groups, profile),
      buildSupportGroupIntroKeyboard(groups, profile)
    );
    return;
  }

  if (action === "close") {
    await answerCallbackQuery(callback.id, tForLocale(locale, "cancelled"));
    await editMessageText(privateChatId, panelMessageId, "Panel cerrado.", buildPrivateHomeKeyboard(locale));
    return;
  }

  if (action === "help") {
    await answerCallbackQuery(callback.id, tForLocale(locale, "preview_ready"));
    await editMessageText(privateChatId, panelMessageId, buildPrivateHelpText(locale), buildPrivateHomeKeyboard(locale));
  }
}

async function showPrivateStart(privateChatId, userId, locale = "es") {
  const groups = await getManageableGroups(userId);

  if (groups.length) {
    await sendMessage(
      privateChatId,
      [
        `<b>${escapeHtml(tForLocale(locale, "private_group_picker_title"))}</b>`,
        "",
        escapeHtml(tForLocale(locale, "private_group_picker_body"))
      ].join("\n"),
      buildPrivateGroupsKeyboard(groups, locale)
    );
    return;
  }

  await sendMessage(privateChatId, buildPrivateWelcomeText(locale), buildPrivateHomeKeyboard(locale));
}

async function showPrivateGroups(privateChatId, userId, locale = "es", panelMessageId = null) {
  const available = await getManageableGroups(userId);

  if (!available.length) {
    const edited = await editPanelMessage(
      privateChatId,
      panelMessageId,
      escapeHtml(tForLocale(locale, "private_no_groups")),
      buildPrivateHomeKeyboard(locale)
    );

    if (!edited) {
      await sendMessage(
        privateChatId,
        escapeHtml(tForLocale(locale, "private_no_groups")),
        buildPrivateHomeKeyboard(locale)
      );
    }
    return;
  }

  const pickerText = [
    `<b>${escapeHtml(tForLocale(locale, "private_group_picker_title"))}</b>`,
    "",
    escapeHtml(tForLocale(locale, "private_group_picker_body"))
  ].join("\n");

  const edited = await editPanelMessage(
    privateChatId,
    panelMessageId,
    pickerText,
    buildPrivateGroupsKeyboard(available, locale)
  );

  if (!edited) {
    await sendMessage(privateChatId, pickerText, buildPrivateGroupsKeyboard(available, locale));
  }
}

async function getManageableGroups(userId) {
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

  return available;
}

async function handlePickGroupCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;

  await answerCallbackQuery(callback.id, "Abriendo grupo");

  if (!Number.isFinite(targetChatId)) {
    return;
  }

  await handlePrivateManageStart(privateChatId, userId, targetChatId, callback.message.message_id);
}

async function handleSupportPickCallback(callback) {
  const targetChatId = Number(String(callback.data || "").split(":")[1]);
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;

  if (!Number.isFinite(targetChatId)) {
    return;
  }

  const groups = await getManageableGroups(userId);
  const targetGroup = groups.find((group) => Number(group.chat_id) === targetChatId);

  if (!targetGroup) {
    await answerCallbackQuery(callback.id, "Grupo no disponible");
    return;
  }

  const profile = await updateUserProfile(userId, {
    support_group_chat_id: targetGroup.chat_id,
    support_group_title: targetGroup.chat_title || "Grupo sin nombre"
  });

  await answerCallbackQuery(callback.id, "Grupo customer service configurado");
  await sendMessage(
    targetGroup.chat_id,
    "Grupo configurado como customer service."
  ).catch(() => null);

  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildSupportGroupIntroText("es", groups, profile),
    buildSupportGroupIntroKeyboard(groups, profile)
  );
}

async function handleSupportConfigCallback(callback) {
  const action = String(callback.data || "").split(":")[1] || "";
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;
  const groups = await getManageableGroups(userId);
  const profile = await getUserProfile(userId);

  if (action === "choose") {
    await answerCallbackQuery(callback.id, "Selecciona un grupo");
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildSupportGroupIntroText("es", groups, null),
      buildSupportGroupIntroKeyboard(groups, null)
    );
    return;
  }

  await answerCallbackQuery(callback.id, "Customer service");
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildSupportGroupIntroText("es", groups, profile),
    buildSupportGroupIntroKeyboard(groups, profile)
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
    await editMessageText(privateChatId, callback.message.message_id, "Panel cerrado.", buildPrivateHomeKeyboard("es"));
    return;
  }

  if (page === "pending") {
    await answerCallbackQuery(callback.id, "Disponible pronto");
    return;
  }

  if (page === "raffle") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildRaffleConfigText(settings),
      buildRaffleConfigKeyboard(targetChatId, settings)
    );
    return;
  }

  if (page === "antispam") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildAntispamConfigText(settings),
      buildAntispamConfigKeyboard(targetChatId, settings)
    );
    return;
  }

  if (page === "link") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildGroupLinkConfigText(settings),
      buildGroupLinkConfigKeyboard(targetChatId, settings)
    );
    return;
  }

  const pageToRender = page === "more" ? "more" : "main";
  await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
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
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildConfigPreview(settings),
      buildConfigCategoryKeyboard(targetChatId, settings, "main")
    );
    return;
  }

  if (action === "cancel") {
    await clearUserState(userId);
    await answerCallbackQuery(callback.id, tForSettings(settings, "cancelled"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      `${escapeHtml(tForSettings(settings, "edition_cancelled"))}\n\n${buildConfigMenuText(settings)}`,
      buildConfigCategoryKeyboard(targetChatId, settings, "main")
    );
    return;
  }

  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildConfigItemPreview(action, settings),
    buildConfigItemPreviewKeyboard(targetChatId, action)
  );
}

async function handleConfigEditCallback(callback) {
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

  await setUserState(userId, targetChatId, action, callback.message.message_id);
  await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));

  if (action === "language") {
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildEditPrompt(action, settings),
      buildLanguagePickerKeyboard(targetChatId)
    );
    return;
  }

  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildEditPrompt(action, settings),
    {
      reply_markup: {
        inline_keyboard: [
          [
            { text: "◀️ Volver", callback_data: `cfg:${targetChatId}:${action}` },
            { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
          ]
        ]
      }
    }
  );
}

async function handleLanguagePickCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const localeCode = parts[2] || "es";
  const userId = callback.from.id;
  const privateChatId = callback.message.chat.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  const updated = await updateGroupSettings(targetChatId, {
    group_language: normalizeLocale(localeCode)
  });

  await clearUserState(userId);
  await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    `${escapeHtml(tForSettings(updated, "language_updated", { language: getLocaleLabel(updated.group_language) }))}\n\n${buildConfigMenuText(updated)}`,
    buildConfigCategoryKeyboard(targetChatId, updated, "main")
  );
}

async function handleGroupLinkActionCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const action = parts[2] || "";
  const userId = callback.from.id;
  const privateChatId = callback.message.chat.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  if (action === "toggle") {
    const updated = await updateGroupSettings(targetChatId, {
      group_link_enabled: !Boolean(settings.group_link_enabled)
    });

    await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildGroupLinkConfigText(updated),
      buildGroupLinkConfigKeyboard(targetChatId, updated)
    );
    return;
  }

  if (action === "edit") {
    await setUserState(userId, targetChatId, "group_link_value", callback.message.message_id);
    await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildConfigItemPreview("group_link_value", settings),
      {
        reply_markup: {
          inline_keyboard: [
            [
              { text: "✏️ Editar", callback_data: `cfgedit:${targetChatId}:group_link_value` },
              { text: "◀️ Volver", callback_data: `cfgmenu:link:${targetChatId}` }
            ],
            [
              { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
            ]
          ]
        }
      }
    );
  }
}

async function handleAntispamActionCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const action = parts[2] || "";
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  let updated = settings;

  if (action === "toggle") {
    updated = await updateGroupSettings(targetChatId, {
      antispam_enabled: !Boolean(settings.antispam_enabled)
    });
  } else if (action === "cycle") {
    const order = ["warn", "mute", "kick"];
    const current = order.includes(settings.antispam_action) ? settings.antispam_action : "warn";
    const next = order[(order.indexOf(current) + 1) % order.length];
    updated = await updateGroupSettings(targetChatId, {
      antispam_action: next
    });
  } else if (action === "duration") {
    await setUserState(userId, targetChatId, "antispam_duration", callback.message.message_id);
    await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildEditPrompt("antispam_duration", settings),
      {
        reply_markup: {
          inline_keyboard: [
            [
              { text: "◀️ Volver", callback_data: `cfgmenu:antispam:${targetChatId}` },
              { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
            ]
          ]
        }
      }
    );
    return;
  }

  await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildAntispamConfigText(updated),
    buildAntispamConfigKeyboard(targetChatId, updated)
  );
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

  const rankedAdmins = admins.result
    .map((item) => ({
      rank: classifyAdmin(item, settings),
      mention: formatAdminMention(item)
    }))
    .sort((left, right) => staffRankWeight(right.rank) - staffRankWeight(left.rank));

  rankedAdmins.forEach((item) => {
    if (!item.mention) {
      return;
    }

    lines.push(`- <b>${escapeHtml(item.rank)}</b>: ${item.mention}`);
  });

  if (lines.length === 1) {
    lines.push(escapeHtml(tForSettings(settings, "staff_unavailable")));
  }

  await sendMessage(chatId, lines.join("\n"));
}

async function handleGroupLinkCommand(chatId, chatTitle = "") {
  const settings = await ensureGroupSettings(chatId, chatTitle || "");

  if (!settings.group_link_enabled || !settings.group_link_value) {
    return;
  }

  await sendMessage(
    chatId,
    `<b>Enlace del grupo</b>\n${escapeHtml(settings.group_link_value)}`
  );
}

async function handleTicketCommand(chat, from, message) {
  const profile = await getUserProfile(from.id);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;

  if (!supportChatId) {
    await sendMessage(chat.id, tForLocale("es", "ticket_support_missing"));
    return;
  }

  await setUserState(from.id, chat.id, "await_ticket_message");
  await sendMessage(from.id, tForLocale("es", "ticket_private_prompt"));
}

async function handlePrivateTicketMessage(message, text, state) {
  const from = message.from || {};
  const mainChatId = Number(state.group_chat_id);
  const profile = await getUserProfile(from.id);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;

  if (!supportChatId) {
    await clearUserState(from.id);
    await sendMessage(message.chat.id, tForLocale("es", "ticket_support_missing"));
    return;
  }

  const mainSettings = await ensureGroupSettings(mainChatId);
  const messagePreview = buildTicketMessagePreview(message, text);
  const ticket = await createSupportTicket(mainChatId, supportChatId, from, messagePreview);

  if (!ticket) {
    await clearUserState(from.id);
    await sendMessage(message.chat.id, tForLocale("es", "database_unavailable"));
    return;
  }

  const userLabel = from.username
    ? `@${String(from.username).replace(/^@/, "")}`
    : [from.first_name, from.last_name].filter(Boolean).join(" ") || String(from.id);

  const forwarded = await sendMessage(
    supportChatId,
    [
      `<b>${escapeHtml(tForLocale("es", "ticket_support_forward_title", { number: ticket.ticket_number }))}</b>`,
      escapeHtml(tForLocale("es", "ticket_support_from", { user: userLabel })),
      escapeHtml(tForLocale("es", "ticket_support_group", { group: mainSettings.chat_title || String(mainChatId) })),
      "",
      escapeHtml(tForLocale("es", "ticket_support_message", { message: messagePreview }))
    ].join("\n")
  );

  let hydratedTicket = ticket;
  if (forwarded && forwarded.ok && forwarded.result && forwarded.result.message_id) {
    const extraMessageIds = [];
    if (hasRelayableMedia(message)) {
      const copied = await copyMessage(
        supportChatId,
        message.chat.id,
        message.message_id,
        { reply_to_message_id: forwarded.result.message_id }
      );

      if (copied && copied.ok && copied.result && copied.result.message_id) {
        extraMessageIds.push(copied.result.message_id);
      }
    }

    hydratedTicket = await attachSupportTicketMessage(
      ticket.id,
      forwarded.result.message_id,
      extraMessageIds
    );
  }

  await clearUserState(from.id);
  scheduleTicketAutoClose(hydratedTicket || ticket);
  await sendMessage(
    message.chat.id,
    escapeHtml(tForLocale("es", "ticket_created", { number: ticket.ticket_number }))
  );
}

async function handleOpenPrivateTicketContinuation(message, text) {
  const from = message.from || {};
  const openTicket = await getOpenSupportTicketByUser(from.id);
  if (!openTicket || openTicket.status !== "open") {
    return;
  }

  const messagePreview = buildTicketMessagePreview(message, text);

  const userLabel = from.username
    ? `@${String(from.username).replace(/^@/, "")}`
    : [from.first_name, from.last_name].filter(Boolean).join(" ") || String(from.id);

  const forwarded = await sendMessage(
    openTicket.support_chat_id,
    [
      `<b>${escapeHtml(tForLocale("es", "ticket_support_followup_title", { number: openTicket.ticket_number }))}</b>`,
      escapeHtml(tForLocale("es", "ticket_support_from", { user: userLabel })),
      "",
      escapeHtml(messagePreview)
    ].join("\n"),
    openTicket.support_message_id
      ? { reply_to_message_id: openTicket.support_message_id }
      : {}
  );

  let updatedTicket = openTicket;
  if (forwarded && forwarded.ok && forwarded.result && forwarded.result.message_id) {
    const extraMessageIds = [];
    if (hasRelayableMedia(message)) {
      const copied = await copyMessage(
        openTicket.support_chat_id,
        message.chat.id,
        message.message_id,
        { reply_to_message_id: forwarded.result.message_id }
      );

      if (copied && copied.ok && copied.result && copied.result.message_id) {
        extraMessageIds.push(copied.result.message_id);
      }
    }

    updatedTicket = await attachSupportTicketMessage(
      openTicket.id,
      forwarded.result.message_id,
      extraMessageIds
    );
    updatedTicket = await updateSupportTicket(openTicket.id, {
      message_text: messagePreview,
      last_activity_at: new Date().toISOString()
    });
  } else {
    updatedTicket = await updateSupportTicket(openTicket.id, {
      message_text: messagePreview,
      last_activity_at: new Date().toISOString()
    });
  }

  scheduleTicketAutoClose(updatedTicket || openTicket);
}

async function handleSupportReply(chat, from, message) {
  if (!(await isGroupAdmin(chat.id, from.id))) {
    return false;
  }

  if (!message.reply_to_message || !message.reply_to_message.message_id) {
    return false;
  }

  const ticket = await getSupportTicketByReply(chat.id, message.reply_to_message.message_id);
  if (!ticket) {
    return false;
  }

  await sendMessage(
    ticket.user_id,
    [
      "<b>Respuesta de soporte</b>",
      "",
      escapeHtml(extractVisibleMessageText(message) || buildTicketMessagePreview(message, ""))
    ].join("\n")
  ).catch(() => null);

  if (hasRelayableMedia(message)) {
    await copyMessage(
      ticket.user_id,
      chat.id,
      message.message_id
    ).catch(() => null);
  }

  const updatedTicket = await updateSupportTicket(ticket.id, {
    last_activity_at: new Date().toISOString()
  });
  scheduleTicketAutoClose(updatedTicket || ticket);
  await sendMessage(chat.id, tForLocale("es", "ticket_reply_sent")).catch(() => null);
  return true;
}

function scheduleTicketAutoClose(ticket) {
  if (!ticket || !ticket.id || ticket.status !== "open") {
    return;
  }

  const existing = activeTicketTimers.get(ticket.id);
  if (existing) {
    clearTimeout(existing);
  }

  const lastActivityAt = ticket.last_activity_at || ticket.updated_at || ticket.created_at;
  const elapsed = Math.max(0, Date.now() - new Date(lastActivityAt).getTime());
  const waitMs = Math.max(1000, TICKET_INACTIVITY_MS - elapsed);

  const timer = setTimeout(async () => {
    activeTicketTimers.delete(ticket.id);
    const fresh = await getOpenSupportTicketByUser(ticket.user_id);
    if (!fresh || fresh.id !== ticket.id || fresh.status !== "open") {
      return;
    }

    const freshLastActivity = fresh.last_activity_at || fresh.updated_at || fresh.created_at;
    const idleFor = Date.now() - new Date(freshLastActivity).getTime();
    if (idleFor < TICKET_INACTIVITY_MS) {
      scheduleTicketAutoClose(fresh);
      return;
    }

    const closed = await closeSupportTicket(ticket.id, "inactive");
    await sendMessage(
      fresh.user_id,
      escapeHtml(tForLocale("es", "ticket_closed_inactive", { number: fresh.ticket_number }))
    ).catch(() => null);
    await sendMessage(
      fresh.support_chat_id,
      escapeHtml(tForLocale("es", "ticket_support_closed_inactive", { number: fresh.ticket_number }))
    ).catch(() => null);

    if (closed) {
      activeTicketTimers.delete(closed.id);
    }
  }, waitMs);

  activeTicketTimers.set(ticket.id, timer);
}

function extractVisibleMessageText(message) {
  return String(message.text || message.caption || "").trim();
}

function hasRelayableMedia(message) {
  return Boolean(
    message.photo ||
      message.video ||
      message.audio ||
      message.voice ||
      message.document ||
      message.animation ||
      message.video_note ||
      message.sticker
  );
}

function hasTicketRelayContent(message) {
  return Boolean(extractVisibleMessageText(message) || hasRelayableMedia(message));
}

function buildTicketMessagePreview(message, fallbackText = "") {
  const visibleText = extractVisibleMessageText(message) || String(fallbackText || "").trim();
  if (visibleText) {
    return visibleText;
  }

  if (message.photo) {
    return "[Imagen]";
  }

  if (message.video) {
    return "[Video]";
  }

  if (message.audio) {
    return "[Audio]";
  }

  if (message.voice) {
    return "[Nota de voz]";
  }

  if (message.document) {
    return "[Documento]";
  }

  if (message.animation) {
    return "[GIF]";
  }

  if (message.video_note) {
    return "[Video circular]";
  }

  if (message.sticker) {
    return "[Sticker]";
  }

  return "[Mensaje]";
}

function formatAdminMention(item) {
  const user = item.user || {};
  if (!user.id) {
    return "";
  }

  const label = user.username
    ? `@${String(user.username).replace(/^@/, "")}`
    : [user.first_name, user.last_name].filter(Boolean).join(" ") || String(user.id);

  return `<a href="tg://user?id=${user.id}">${escapeHtml(label)}</a>`;
}

function staffRankWeight(rank) {
  if (rank === "Propietario") {
    return 3;
  }

  if (rank === "Co-Lider") {
    return 2;
  }

  return 1;
}

function countAdminPermissions(item) {
  const keys = [
    "can_manage_chat",
    "can_delete_messages",
    "can_manage_video_chats",
    "can_restrict_members",
    "can_promote_members",
    "can_change_info",
    "can_invite_users",
    "can_pin_messages",
    "can_manage_topics",
    "can_post_messages",
    "can_edit_messages"
  ];

  return keys.reduce((total, key) => total + (item[key] ? 1 : 0), 0);
}

function classifyAdmin(item, settings = { group_language: "es" }) {
  const status = item.status;

  if (status === "creator") {
    return "Propietario";
  }

  const permissionCount = countAdminPermissions(item);

  if (permissionCount >= 8) {
    return "Propietario";
  }

  if (permissionCount >= 5) {
    return "Co-Lider";
  }

  return "Lider";
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

async function maybeHandleAntispam(chat, from, message) {
  if (!from || !from.id || !message || !message.text) {
    return;
  }

  if (await isGroupAdmin(chat.id, from.id)) {
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  if (!settings.antispam_enabled) {
    return;
  }

  const trackerKey = `${chat.id}:${from.id}`;
  const now = Date.now();
  const normalizedText = normalizeSpamText(message.text);
  const previous = spamTracker.get(trackerKey) || { timestamps: [], lastText: "", sameTextCount: 0 };
  const timestamps = [...previous.timestamps.filter((value) => now - value < 15000), now];
  const sameTextCount = previous.lastText === normalizedText ? previous.sameTextCount + 1 : 1;
  const state = {
    timestamps,
    lastText: normalizedText,
    sameTextCount
  };

  spamTracker.set(trackerKey, state);

  const repeatedBurst = sameTextCount >= 3;
  const floodBurst = timestamps.length >= 5;
  const linkBurst = /https?:\/\/|t\.me\/|www\./i.test(message.text) && timestamps.length >= 3;

  if (!repeatedBurst && !floodBurst && !linkBurst) {
    return;
  }

  await deleteMessage(chat.id, message.message_id).catch(() => null);
  await sendMessage(
    chat.id,
    renderTemplate(settings.warning_message || "{first_name}, no se permite enviar spam en este grupo.", {
      first_name: from.first_name || tForSettings(settings, "user_fallback"),
      username: from.username ? `@${from.username}` : from.first_name || tForSettings(settings, "user_fallback"),
      group: chat.title || tForSettings(settings, "group_title_fallback")
    })
  );

  if (settings.antispam_action === "mute") {
    const durationSeconds = parseDurationToSeconds(settings.antispam_duration_text || "24 h");
    const untilDate = Math.floor(Date.now() / 1000) + durationSeconds;
    await restrictChatMember(chat.id, from.id, untilDate).catch(() => null);
    return;
  }

  if (settings.antispam_action === "kick") {
    await banChatMember(chat.id, from.id).catch(() => null);
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
          { text: "🛡️ Antispam", callback_data: `cfgmenu:antispam:${chatId}` }
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
          { text: "🎁 Sorteo", callback_data: `cfgmenu:raffle:${chatId}` }
        ],
        [
          { text: "👥 Staff", callback_data: `cfgmenu:pending:${chatId}` },
          { text: "🔗 Enlace del grupo", callback_data: `cfgmenu:link:${chatId}` }
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

function buildRaffleConfigText(settings) {
  return [
    "<b>CONFIGURACION DEL SORTEO</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    "Desde aqui puedes editar el mensaje principal, las reglas y abrir la lista publica del sorteo."
  ].join("\n");
}

function buildRaffleConfigKeyboard(chatId, settings) {
  const listUrl = buildRaffleListUrl(chatId);
  const rows = [
    [
      { text: "📜 Editar reglas", callback_data: `cfg:${chatId}:rules` },
      { text: "📝 Editar mensaje", callback_data: `cfg:${chatId}:raffle_intro` }
    ],
    [
      { text: "🌐 Ver lista publica", url: listUrl || `${config.panelUrl}/raffle_live.php?chat_id=${chatId}` }
    ],
    [
      { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` },
      { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
    ]
  ];

  return {
    reply_markup: {
      inline_keyboard: rows
    }
  };
}

function buildAntispamConfigText(settings) {
  return [
    "<b>CONFIGURACION ANTISPAM</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Estado: <b>${settings.antispam_enabled ? "Activado" : "Desactivado"}</b>`,
    `Sancion: <b>${escapeHtml(formatAntispamAction(settings.antispam_action))}</b>`,
    `Duracion del mute: <b>${escapeHtml(settings.antispam_duration_text || "24 h")}</b>`,
    "",
    "Cuando detecta spam, el bot elimina el mensaje, advierte al usuario y aplica la sancion configurada."
  ].join("\n");
}

function buildAntispamConfigKeyboard(chatId, settings) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          {
            text: settings.antispam_enabled ? "🟢 Desactivar" : "🔴 Activar",
            callback_data: `antispam:${chatId}:toggle`
          },
          {
            text: `⚖️ ${formatAntispamAction(settings.antispam_action)}`,
            callback_data: `antispam:${chatId}:cycle`
          }
        ],
        [
          {
            text: `⏱️ Duracion: ${settings.antispam_duration_text || "24 h"}`,
            callback_data: `antispam:${chatId}:duration`
          }
        ],
        [
          { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` },
          { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildGroupLinkConfigText(settings) {
  return [
    "<b>ENLACE DEL GRUPO</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Estado: <b>${settings.group_link_enabled ? "Activado" : "Desactivado"}</b>`,
    `Enlace actual: <b>${escapeHtml(settings.group_link_value || "Sin enlace configurado")}</b>`,
    "",
    "Si esta activado, el comando /link enviara este enlace dentro del grupo."
  ].join("\n");
}

function buildGroupLinkConfigKeyboard(chatId, settings) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          {
            text: settings.group_link_enabled ? "🟢 Desactivar" : "🔴 Activar",
            callback_data: `grouplink:${chatId}:toggle`
          },
          {
            text: "✏️ Editar enlace",
            callback_data: `cfgedit:${chatId}:group_link_value`
          }
        ],
        [
          { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` },
          { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildLanguagePickerKeyboard(chatId) {
  const locales = Object.entries(getSupportedLocales());
  const rows = [];

  for (let index = 0; index < locales.length; index += 2) {
    const pair = locales.slice(index, index + 2).map(([code, label]) => ({
      text: `${code.toUpperCase()} ${label}`,
      callback_data: `langpick:${chatId}:${code}`
    }));
    rows.push(pair);
  }

  rows.push([
    { text: "◀️ Volver", callback_data: `cfg:${chatId}:language` },
    { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
  ]);

  return {
    reply_markup: {
      inline_keyboard: rows
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
          { text: "🛟 Customer service Group", callback_data: "home:support" }
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

function buildSupportGroupIntroText(locale = "es", groups = [], profile = null) {
  const selectedTitle = profile && profile.support_group_title
    ? profile.support_group_title
    : "";

  if (selectedTitle) {
    return [
      "<b>CUSTOMER SERVICE GROUP</b>",
      "",
      "Este es el grupo configurado actualmente para atender tickets y respuestas del soporte.",
      "",
      `<b>Grupo activo:</b> ${escapeHtml(selectedTitle)}`
    ].join("\n");
  }

  const groupLines = groups.length
    ? groups.map((group) => `- ${escapeHtml(group.chat_title || "Grupo sin nombre")}`)
    : ["- Aun no tienes grupos detectados con esta cuenta."];

  return [
    "<b>CUSTOMER SERVICE GROUP</b>",
    "",
    "Selecciona un unico grupo para usarlo como customer service.",
    "Cuando quede configurado, el bot enviara ahi los tickets de soporte.",
    "",
    "<b>Grupos disponibles:</b>",
    ...groupLines
  ].join("\n");
}

function buildSupportGroupIntroKeyboard(groups = [], profile = null) {
  const addUrl = config.botUsername ? `https://t.me/${config.botUsername}?startgroup=true` : config.panelUrl;
  const selectedId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;
  const selectedTitle = profile && profile.support_group_title ? profile.support_group_title : "";

  let groupRows = [];

  if (selectedId && selectedTitle) {
    groupRows = [
      [
        {
          text: `✅ ${selectedTitle}`,
          callback_data: "supportcfg:current"
        }
      ],
      [
        {
          text: "Cambiar grupo",
          callback_data: "supportcfg:choose"
        }
      ]
    ];
  } else {
    groupRows = groups.map((group) => [
      {
        text: group.chat_title || "Grupo sin nombre",
        callback_data: `supportpick:${group.chat_id}`
      }
    ]);
  }

  return {
    reply_markup: {
      inline_keyboard: [
        ...groupRows,
        [
          { text: "➕ Añadir al grupo", url: addUrl }
        ],
        [
          { text: "◀️ Volver", callback_data: "home:groups" },
          { text: "✅ Cerrar", callback_data: "home:close" }
        ]
      ]
    }
  };
}

function buildPrivateGroupsKeyboard(groups, locale = "es") {
  const addUrl = config.botUsername ? `https://t.me/${config.botUsername}?startgroup=true` : config.panelUrl;
  const groupRows = groups.map((group) => [
    {
      text: group.chat_title || "Grupo sin nombre",
      callback_data: `pickgroup:${group.chat_id}`
    }
  ]);

  return {
    reply_markup: {
      inline_keyboard: [
        ...groupRows,
        [
          { text: "Panel web", url: `${config.panelUrl}/dashboard.php` },
          { text: "Agregar grupo", url: addUrl }
        ],
        [
          { text: "◀️ Volver", callback_data: "home:close" },
          { text: "Ayuda", callback_data: "home:help" }
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
    rules: "Envia el nuevo reglamento del grupo.",
    raffle_intro: tForSettings(settings, "prompt_raffle_intro"),
    language: tForSettings(settings, "prompt_language"),
    antispam_duration: "Envia la duracion del mute. Ejemplo: 1 d 24 h 17 m",
    group_link_value: "Envia el enlace del grupo. Ejemplo: https://t.me/tu_grupo"
  };

  const currentValue = {
    welcome: settings.welcome_message,
    warning: settings.warning_message,
    rules: settings.group_rules_text,
    raffle_intro: settings.raffle_intro_text,
    language: `${getGroupLocale(settings).toUpperCase()} - ${getLocaleLabel(getGroupLocale(settings))}`,
    antispam_duration: settings.antispam_duration_text || "24 h"
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

function buildConfigItemPreview(action, settings) {
  const labels = {
    welcome: tForSettings(settings, "preview_welcome"),
    warning: tForSettings(settings, "preview_warning"),
    rules: tForSettings(settings, "preview_rules"),
    raffle_intro: tForSettings(settings, "preview_raffle_text"),
    language: tForSettings(settings, "preview_language"),
    antispam_duration: "Duracion del antispam",
    group_link_value: "Enlace del grupo"
  };

  const values = {
    welcome: settings.welcome_message || "",
    warning: settings.warning_message || "",
    rules: settings.group_rules_text || "",
    raffle_intro: settings.raffle_intro_text || "",
    language: `${getGroupLocale(settings).toUpperCase()} - ${getLocaleLabel(getGroupLocale(settings))}`,
    antispam_duration: settings.antispam_duration_text || "24 h",
    group_link_value: settings.group_link_value || "Sin enlace configurado."
  };

  return [
    `<b>${escapeHtml(labels[action] || action)}</b>`,
    "",
    escapeHtml(values[action] || ""),
    "",
    "Pulsa Editar si deseas cambiar este valor o Volver para regresar al menu."
  ].join("\n");
}

function buildConfigItemPreviewKeyboard(chatId, action) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "✏️ Editar", callback_data: `cfgedit:${chatId}:${action}` },
          { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` }
        ],
        [
          { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
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

function formatEntryName(entry) {
  return entry.username ? `@${String(entry.username).replace(/^@/, "")}` : entry.first_name || String(entry.user_id);
}

function buildRaffleListUrl(chatId) {
  if (!config.panelUrl) {
    return "";
  }

  return `${config.panelUrl}/raffle_live.php?chat_id=${encodeURIComponent(String(chatId))}`;
}

function normalizeSpamText(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function parseDurationToSeconds(value) {
  const input = String(value || "").toLowerCase();
  const parts = Array.from(input.matchAll(/(\d+)\s*([dhms])/g));

  if (!parts.length) {
    return 24 * 60 * 60;
  }

  return parts.reduce((total, item) => {
    const amount = Number(item[1]);
    const unit = item[2];

    if (unit === "d") {
      return total + amount * 24 * 60 * 60;
    }

    if (unit === "h") {
      return total + amount * 60 * 60;
    }

    if (unit === "m") {
      return total + amount * 60;
    }

    if (unit === "s") {
      return total + amount;
    }

    return total;
  }, 0);
}

function formatAntispamAction(action) {
  if (action === "mute") {
    return "Silenciar";
  }

  if (action === "kick") {
    return "Expulsar";
  }

  return "Advertir";
}

async function editPanelMessage(chatId, messageId, text, replyMarkup) {
  if (!messageId || !Number.isFinite(Number(messageId))) {
    return false;
  }

  try {
    const result = await editMessageText(chatId, Number(messageId), text, replyMarkup);
    return Boolean(result && result.ok);
  } catch (_error) {
    return false;
  }
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
