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
  deleteWebhook,
  restrictChatMember,
  allowChatMember,
  banChatMember,
  setMyCommands
} = require("./telegram");
const {
  testDbConnection,
  listBotsByOwner,
  listAllBots,
  registerBot,
  disconnectBot,
  updateBotSubscription,
  getBotByWebhookKey,
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
  closeSupportTicket,
  listSupportTicketsByGroup,
  getUserWarnings,
  incrementUserWarnings,
  resetUserWarnings,
  listWarningSnapshots,
  appendGroupActivityLog,
  listGroupActivityLogs
} = require("./db");
const { runWithBot, currentBot } = require("./botContext");

const app = express();
app.use(express.json());

const ACTION_TO_FIELD = {
  welcome: "welcome_message",
  welcome_autodelete: "welcome_autodelete_text",
  warning: "warning_message",
  rules: "group_rules_text",
  raffle_intro: "raffle_intro_text",
  language: "group_language",
  antispam_duration: "antispam_duration_text",
  group_link_value: "group_link_value",
  topics: "topics_policy",
  banned_words: "banned_words_text",
  repeated_messages: "repeated_messages_policy",
  member_permissions: "member_permissions_text",
  masked_users: "masked_users_policy",
  custom_commands: "custom_commands_text",
  translation_scope: "translation_scope",
  warn_limit: "warn_limit_text",
  warn_duration: "warn_duration_text",
  captcha_timeout: "captcha_timeout_text"
};

const spamTracker = new Map();
const repeatedMessageTracker = new Map();
const activeTicketTimers = new Map();
const activeWelcomeMessages = new Map();
const activeWelcomeDeleteTimers = new Map();
const commandSyncTracker = new Map();
const activeJoinChallenges = new Map();
const recentJoinTracker = new Map();
const activeRaidProtection = new Map();
let backgroundJobsStarted = false;
const TICKET_INACTIVITY_MS = 10 * 60 * 1000;
const COMMAND_SYNC_INTERVAL_MS = 10 * 60 * 1000;
const ANTI_RAID_JOIN_THRESHOLD = 5;
const ANTI_RAID_WINDOW_MS = 60 * 1000;
const ANTI_RAID_HOLD_MS = 10 * 60 * 1000;
const PREMIUM_REMINDER_INTERVAL_MS = 15 * 60 * 1000;
const FREE_CONFIG_ACTIONS = new Set([
  "welcome",
  "welcome_autodelete",
  "warning",
  "warn_limit",
  "warn_duration",
  "rules",
  "antispam_duration",
  "group_link_value",
  "banned_words",
  "repeated_messages",
  "log_channel_value"
]);

function addMonthsIso(months = 1) {
  const date = new Date();
  date.setMonth(date.getMonth() + Number(months || 1));
  return date.toISOString();
}

function formatPremiumDate(value) {
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) {
    return "No definida";
  }

  return parsed.toLocaleString("es-PE", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "America/Lima"
  });
}

function hoursUntil(value) {
  const parsed = new Date(value || "").getTime();
  if (!parsed || Number.isNaN(parsed)) {
    return null;
  }

  return (parsed - Date.now()) / (1000 * 60 * 60);
}

function isPremiumActive(bot = currentBot()) {
  if (!bot || !bot.id || bot.id === "default") {
    return true;
  }

  const premiumUntil = new Date(bot.premium_until || 0).getTime();
  return String(bot.subscription_status || "inactive") === "active" && premiumUntil > Date.now();
}

function premiumBlockText() {
  return [
    "<b>Suscripcion inactiva</b>",
    "",
    "Las funciones premium de este bot estan desactivadas porque la suscripcion vencio o aun no fue activada.",
    "Renueva el plan desde el panel web para seguir usando sorteos, tickets, bienvenida, staff y configuracion avanzada."
  ].join("\n");
}

async function logGroupActivitySafe(chatId, type, title, summary) {
  if (!Number.isFinite(Number(chatId))) {
    return;
  }

  await appendGroupActivityLog(Number(chatId), {
    type,
    title,
    summary
  }).catch(() => null);
}

function buildPrivateCommandMenu() {
  return [
    { command: "start", description: "Abrir panel privado del bot" },
    { command: "panel", description: "Abrir selector de grupos" },
    { command: "panelbot", description: "Abrir selector de grupos" },
    { command: "settings", description: "Abrir configuracion privada" }
  ];
}

function buildPublicGroupCommands(settings) {
  const locale = getGroupLocale(settings);
  if (locale === "en") {
    return [
      { command: "rules", description: "Show the group rules" },
      { command: "staff", description: "Show the admin team" },
      { command: "link", description: "Show the group link" },
      { command: "ticket", description: "Open a support ticket" }
    ];
  }

  return [
    { command: "rules", description: "Ver el reglamento del grupo" },
    { command: "staff", description: "Ver el staff del grupo" },
    { command: "link", description: "Ver el enlace del grupo" },
    { command: "ticket", description: "Abrir ticket de soporte" }
  ];
}

function buildAdminGroupCommands(settings) {
  const base = buildPublicGroupCommands(settings);
  const locale = getGroupLocale(settings);

  const adminOnly = locale === "en"
    ? [
        { command: "help", description: "Show available commands" },
        { command: "panelbot", description: "Open the private admin panel" },
        { command: "warn", description: "Warn the replied user" },
        { command: "warns", description: "Show warning count" },
        { command: "clearwarns", description: "Clear a user's warnings" },
        { command: "nsorteo", description: "Start a raffle message" },
        { command: "sortear", description: "Pick a raffle winner" },
        { command: "reset", description: "Reset raffle entries" },
        { command: "announce", description: "Broadcast to main groups" },
        { command: "gverify", description: "Verify linked main groups" },
        { command: "tickets", description: "View recent ticket history" },
        { command: "logs", description: "View recent activity logs" }
      ]
    : [
        { command: "help", description: "Ver los comandos disponibles" },
        { command: "panelbot", description: "Abrir panel privado de admin" },
        { command: "warn", description: "Advertir al usuario respondido" },
        { command: "warns", description: "Ver advertencias acumuladas" },
        { command: "clearwarns", description: "Limpiar advertencias de un usuario" },
        { command: "nsorteo", description: "Publicar un sorteo" },
        { command: "sortear", description: "Elegir ganador del sorteo" },
        { command: "reset", description: "Reiniciar el sorteo" },
        { command: "announce", description: "Enviar anuncio a grupos" },
        { command: "gverify", description: "Verificar grupos conectados" },
        { command: "tickets", description: "Ver historial reciente de tickets" },
        { command: "logs", description: "Ver actividad reciente del grupo" }
      ];

  return base.concat(adminOnly);
}

async function syncPrivateCommandMenu() {
  const bot = currentBot();
  const cacheKey = `${bot && bot.id ? bot.id : "default"}:private`;
  const lastSyncedAt = Number(commandSyncTracker.get(cacheKey) || 0);

  if (Date.now() - lastSyncedAt < COMMAND_SYNC_INTERVAL_MS) {
    return;
  }

  const result = await setMyCommands(buildPrivateCommandMenu(), { type: "all_private_chats" }).catch(() => null);
  if (result && result.ok) {
    commandSyncTracker.set(cacheKey, Date.now());
  }
}

async function syncGroupCommandMenus(chatId, settings) {
  if (!Number.isFinite(Number(chatId))) {
    return;
  }

  const bot = currentBot();
  const botKey = bot && bot.id ? bot.id : "default";
  const cacheKey = `${botKey}:group:${chatId}`;
  const lastSyncedAt = Number(commandSyncTracker.get(cacheKey) || 0);

  if (Date.now() - lastSyncedAt < COMMAND_SYNC_INTERVAL_MS) {
    await syncPrivateCommandMenu();
    return;
  }

  const publicCommands = buildPublicGroupCommands(settings);
  const adminCommands = buildAdminGroupCommands(settings);
  const groupScope = { type: "chat", chat_id: Number(chatId) };
  const adminScope = { type: "chat_administrators", chat_id: Number(chatId) };
  const globalGroupScope = { type: "all_group_chats" };
  const globalAdminScope = { type: "all_chat_administrators" };

  const results = await Promise.allSettled([
    setMyCommands(publicCommands, globalGroupScope),
    setMyCommands(adminCommands, globalAdminScope),
    setMyCommands(publicCommands, groupScope),
    setMyCommands(adminCommands, adminScope),
    syncPrivateCommandMenu()
  ]);

  const ok = results[0].status === "fulfilled" &&
    results[0].value &&
    results[0].value.ok &&
    results[1].status === "fulfilled" &&
    results[1].value &&
    results[1].value.ok &&
    results[2].status === "fulfilled" &&
    results[2].value &&
    results[2].value.ok &&
    results[3].status === "fulfilled" &&
    results[3].value &&
    results[3].value.ok;

  if (ok) {
    commandSyncTracker.set(cacheKey, Date.now());
  }
}

async function notifyPremiumBlocked(chatId, messageId = null) {
  await sendMessage(chatId, premiumBlockText()).catch(() => null);
  if (messageId) {
    await answerCallbackQuery(messageId, "Suscripcion inactiva").catch(() => null);
  }
}

function isPremiumConfigAction(action) {
  return !FREE_CONFIG_ACTIONS.has(String(action || ""));
}

function isPremiumCommand(command) {
  return ["/ticket", "/nsorteo", "/sortear", "/reset"].includes(String(command || ""));
}

function isPremiumCallbackData(data) {
  const value = String(data || "");

  if (value.indexOf("raffle_join:") === 0 || value.indexOf("supportpick:") === 0 || value.indexOf("supportcfg:") === 0) {
    return true;
  }

  if (value.indexOf("langpick:") === 0) {
    return true;
  }

  if (value.indexOf("cfgmenu:raffle:") === 0) {
    return true;
  }

  if (value.indexOf("cfgmenu:translation:") === 0) {
    return true;
  }

  if (value.indexOf("cfgmenu:") === 0) {
    return false;
  }

  if (value.indexOf("cfg:") === 0 || value.indexOf("cfgedit:") === 0) {
    const parts = value.split(":");
    const action = parts[2] || "";
    return isPremiumConfigAction(action);
  }

  return false;
}

async function resolvePanelBot(req) {
  const source = req.method === "GET" ? (req.query || {}) : (req.body || {});
  const ownerKey = String(source.owner_key || "").trim();
  const botId = String(source.bot_id || "").trim();

  if (!ownerKey || !botId) {
    return null;
  }

  const bots = await listBotsByOwner(ownerKey);
  return bots.find((item) => String(item.id) === botId) || null;
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
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
      const chatId = Number(req.params.chatId);
      if (!Number.isFinite(chatId)) {
        return res.status(400).json({ ok: false, message: "Invalid chat id." });
      }

      const settings = await ensureGroupSettings(chatId);
      let mergedSettings = { ...settings };

      if (bot && Number.isFinite(Number(bot.owner_telegram_id))) {
        const profile = await getUserProfile(Number(bot.owner_telegram_id));
        if (profile) {
          if (!mergedSettings.support_group_chat_id && profile.support_group_chat_id) {
            mergedSettings.support_group_chat_id = profile.support_group_chat_id;
          }
          if (!mergedSettings.support_group_title && profile.support_group_title) {
            mergedSettings.support_group_title = profile.support_group_title;
          }
        }
      }

      return res.json({ ok: true, settings: mergedSettings, bot_id: bot ? bot.id : "default" });
    });
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
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
      const groups = await listGroups();
      return res.json({ ok: true, groups, bot_id: bot ? bot.id : "default" });
    });
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
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
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

      if (typeof body.welcome_autodelete_text === "string") {
        patch.welcome_autodelete_text = body.welcome_autodelete_text.trim();
      }

      if (typeof body.warning_message === "string") {
        patch.warning_message = body.warning_message.trim();
      }

      if (typeof body.warn_limit_text === "string") {
        patch.warn_limit_text = body.warn_limit_text.trim();
      }

      if (typeof body.warn_action === "string") {
        patch.warn_action = body.warn_action.trim();
      }

      if (typeof body.warn_duration_text === "string") {
        patch.warn_duration_text = body.warn_duration_text.trim();
      }

      if (typeof body.captcha_mode === "string") {
        patch.captcha_mode = body.captcha_mode.trim();
      }

      if (typeof body.captcha_timeout_text === "string") {
        patch.captcha_timeout_text = body.captcha_timeout_text.trim();
      }

      if (typeof body.silent_actions_enabled === "boolean") {
        patch.silent_actions_enabled = body.silent_actions_enabled;
      }

      if (typeof body.log_channel_chat_id === "number" || body.log_channel_chat_id === null) {
        patch.log_channel_chat_id = body.log_channel_chat_id;
      }

      if (typeof body.log_channel_title === "string") {
        patch.log_channel_title = body.log_channel_title.trim();
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

      if (typeof body.topics_policy === "string") {
        patch.topics_policy = body.topics_policy.trim();
      }

      if (typeof body.banned_words_text === "string") {
        patch.banned_words_text = body.banned_words_text.trim();
      }

      if (typeof body.repeated_messages_policy === "string") {
        patch.repeated_messages_policy = body.repeated_messages_policy.trim();
      }

      if (typeof body.member_permissions_text === "string") {
        patch.member_permissions_text = body.member_permissions_text.trim();
      }

      if (typeof body.masked_users_policy === "string") {
        patch.masked_users_policy = body.masked_users_policy.trim();
      }

      if (typeof body.custom_commands_text === "string") {
        patch.custom_commands_text = body.custom_commands_text.trim();
      }

      if (typeof body.translation_enabled === "boolean") {
        patch.translation_enabled = body.translation_enabled;
      }

      if (typeof body.translation_scope === "string") {
        patch.translation_scope = body.translation_scope.trim();
      }

      const settings = await updateGroupSettings(chatId, patch);
      if (Object.keys(patch).length) {
        await appendGroupActivityLog(chatId, {
          type: "settings",
          title: "Configuracion actualizada",
          summary: `Campos actualizados: ${Object.keys(patch).join(", ")}`
        }).catch(() => null);
      }
      return res.json({ ok: true, previous: current, settings, bot_id: bot ? bot.id : "default" });
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/group/:chatId/logs", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
      const chatId = Number(req.params.chatId);
      if (!Number.isFinite(chatId)) {
        return res.status(400).json({ ok: false, message: "Invalid chat id." });
      }

      const logs = await listGroupActivityLogs(chatId, Number(req.query.limit || 25));
      return res.json({ ok: true, logs, bot_id: bot ? bot.id : "default" });
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/group/:chatId/tickets", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
      const chatId = Number(req.params.chatId);
      if (!Number.isFinite(chatId)) {
        return res.status(400).json({ ok: false, message: "Invalid chat id." });
      }

      const tickets = await listSupportTicketsByGroup(chatId);
      return res.json({ ok: true, tickets, bot_id: bot ? bot.id : "default" });
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/group/:chatId/warnings", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const bot = await resolvePanelBot(req);
    return runWithBot(bot, async () => {
      const chatId = Number(req.params.chatId);
      if (!Number.isFinite(chatId)) {
        return res.status(400).json({ ok: false, message: "Invalid chat id." });
      }

      const warnings = await listWarningSnapshots(chatId);
      return res.json({ ok: true, warnings, bot_id: bot ? bot.id : "default" });
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/bots", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const ownerKey = String(req.query.owner_key || "").trim();
    const bots = await listBotsByOwner(ownerKey);
    return res.json({ ok: true, bots });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.get("/api/panel/admin/subscriptions", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const bots = await listAllBots();
    return res.json({ ok: true, bots });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/api/panel/admin/subscriptions/:botId", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const botId = String(req.params.botId || "").trim();
    const months = Math.max(1, Number((req.body || {}).months || 1));
    const activatedBy = String((req.body || {}).activated_by || "owner").trim();
    const activatedAt = new Date().toISOString();
    const premiumUntil = addMonthsIso(months);

    const bot = await updateBotSubscription(botId, {
      subscription_status: "active",
      premium_activated_at: activatedAt,
      premium_until: premiumUntil,
      subscription_months: months,
      subscription_updated_by: activatedBy,
      premium_reminder_7d_at: null,
      premium_reminder_1d_at: null,
      premium_expired_notified_at: null
    });

    if (!bot) {
      return res.status(404).json({ ok: false, message: "Bot not found." });
    }

    if (bot.owner_telegram_id && bot.bot_token) {
      const premiumText = [
        "<b>PREMIUM ACTIVADO</b>",
        "",
        `<b>Bot:</b> ${escapeHtml(bot.bot_name || bot.bot_username || "Bot clonado")}`,
        `<b>Activado:</b> ${escapeHtml(formatPremiumDate(activatedAt))}`,
        `<b>Vence:</b> ${escapeHtml(formatPremiumDate(premiumUntil))}`
      ].join("\n");

      await sendMessage(bot.owner_telegram_id, premiumText, {}, bot.bot_token).catch(() => null);
    }

    return res.json({ ok: true, bot });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/api/panel/admin/subscriptions/:botId/deactivate", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const botId = String(req.params.botId || "").trim();
    const activatedBy = String((req.body || {}).activated_by || "owner").trim();
    const bot = await updateBotSubscription(botId, {
      subscription_status: "inactive",
      premium_until: null,
      subscription_updated_by: activatedBy,
      premium_reminder_7d_at: null,
      premium_reminder_1d_at: null,
      premium_expired_notified_at: null
    });

    if (!bot) {
      return res.status(404).json({ ok: false, message: "Bot not found." });
    }

    return res.json({ ok: true, bot });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/api/panel/bots/register", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const body = req.body || {};
    const owner = {
      owner_key: String(body.owner_key || "").trim(),
      owner_name: String(body.owner_name || "User").trim(),
      owner_telegram_id: body.owner_telegram_id ? String(body.owner_telegram_id) : null
    };

    const bot = await registerBot(owner, body);
    if (!bot) {
      return res.status(400).json({ ok: false, message: "Missing bot data." });
    }

      const webhookUrl = `${config.appUrl}/telegram/webhook/${bot.webhook_key}`;
      const telegram = await setWebhook(webhookUrl, bot.bot_token);

      if (owner.owner_telegram_id) {
        await sendMessage(
          owner.owner_telegram_id,
          [
            "<b>Bot conectado correctamente</b>",
            "",
            "Bienvenido. Tu bot ya esta listo para configurarse y empezar a funcionar."
          ].join("\n"),
          {},
          bot.bot_token
        ).catch(() => null);
      }

      return res.json({
        ok: true,
        bot: {
        id: bot.id,
        bot_name: bot.bot_name,
        bot_username: bot.bot_username,
        webhook_key: bot.webhook_key,
        webhook_url: webhookUrl
      },
      telegram
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/api/panel/bots/:botId/disconnect", async (req, res) => {
  if (!isPanelTokenValid(req)) {
    return res.status(403).json({ ok: false, message: "Invalid panel token." });
  }

  try {
    const ownerKey = String((req.body || {}).owner_key || "").trim();
    const botId = String(req.params.botId || "").trim();
    const bots = await listBotsByOwner(ownerKey);
    const bot = bots.find((item) => String(item.id) === botId);
    if (!bot) {
      return res.status(404).json({ ok: false, message: "Bot not found." });
    }

    await deleteWebhook(bot.bot_token);
    const ok = await disconnectBot(ownerKey, botId);
    return res.json({ ok });
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

app.get("/telegram/set-webhook/:webhookKey", async (req, res) => {
  try {
    const bot = await getBotByWebhookKey(req.params.webhookKey);
    if (!bot) {
      return res.status(404).json({ ok: false, message: "Bot not found." });
    }

    const webhookUrl = `${config.appUrl}/telegram/webhook/${bot.webhook_key}`;
    const result = await setWebhook(webhookUrl, bot.bot_token);
    return res.json({
      ok: true,
      webhook_url: webhookUrl,
      telegram: result
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/telegram/webhook", async (req, res) => {
  try {
    if (!isSecretValid(req)) {
      return res.status(403).json({ ok: false, message: "Invalid secret token." });
    }

    return processTelegramUpdate(req.body || {}, null, res);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

app.post("/telegram/webhook/:webhookKey", async (req, res) => {
  try {
    if (!isSecretValid(req)) {
      return res.status(403).json({ ok: false, message: "Invalid secret token." });
    }

    const bot = await getBotByWebhookKey(req.params.webhookKey);
    if (!bot) {
      return res.status(404).json({ ok: false, message: "Bot not found." });
    }

    return processTelegramUpdate(req.body || {}, bot, res);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, message: error.message });
  }
});

async function processTelegramUpdate(update, bot, res) {
  return runWithBot(bot, async () => {
    if (update.callback_query) {
      await handleCallbackQuery(update.callback_query);
      return res.json({ ok: true, update: "callback_query", bot_id: bot ? bot.id : "default" });
    }

    if (update.message) {
      await handleMessage(update.message);
      return res.json({ ok: true, update: "message", bot_id: bot ? bot.id : "default" });
    }

    return res.json({ ok: true, skipped: true, bot_id: bot ? bot.id : "default" });
  });
}

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
  const premiumActive = isPremiumActive();

  if (chat.type === "private") {
    await syncPrivateCommandMenu().catch(() => null);
    if (text) {
      await handlePrivateText(message, text);
    } else if (hasTicketRelayContent(message)) {
      if (premiumActive) {
        await handlePrivateNonText(message);
      } else {
        await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      }
    }
    return;
  }

  const groupSettings = await ensureGroupSettings(chat.id, chat.title || "");
  await syncGroupCommandMenus(chat.id, groupSettings).catch(() => null);

  if (await cleanupServiceActionMessage(message)) {
    if (
      !Array.isArray(message.new_chat_members) &&
      !message.left_chat_member
    ) {
      return;
    }
  }

  if (Array.isArray(message.new_chat_members) && message.new_chat_members.length > 0) {
    await handleJoinGate(chat, message.new_chat_members);
    return;
  }

  if (message.left_chat_member) {
    return;
  }

  if (premiumActive && message.reply_to_message && (!text || !text.startsWith("/"))) {
    const replied = await handleSupportReply(chat, from, message);
    if (replied) {
      return;
    }
  }

  if (!text.startsWith("/")) {
    const moderated = await maybeHandleAdvancedModeration(chat, from, message);
    if (moderated) {
      return;
    }
    await maybeHandleAntispam(chat, from, message);
    if (premiumActive && text) {
      const keywordHandled = await handleKeywordAutoReply(chat, from, text);
      if (keywordHandled) {
        return;
      }
    }
    if (premiumActive) {
      await maybeHandleAutoTranslation(chat, from, message, groupSettings);
    }
    return;
  }

  const command = extractCommand(text);

  if (command === "/help") {
    await sendMessage(chat.id, buildGroupHelpText(groupSettings));
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/panelbot") {
    await handlePanelBotCommand(chat, from);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/reglas" || command === "/rules") {
    await sendMessage(chat.id, groupSettings.group_rules_text || tForSettings(groupSettings, "rules_empty"));
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
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }
    await handleTicketCommand(chat, from, message);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/announce") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }

    await handleAnnounceCommand(chat, from, message, text);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/gverify") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }

    await handleGroupVerifyCommand(chat, from);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/tickets") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }

    if (!(await isGroupAdmin(chat.id, from.id))) {
      return;
    }

    await handleTicketsCommand(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/logs") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }

    if (!(await isGroupAdmin(chat.id, from.id))) {
      return;
    }

    await handleLogsCommand(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (!premiumActive) {
    if (findCustomCommand(groupSettings.custom_commands_text, command)) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }
  }

  const customHandled = await handleCustomGroupCommand(chat, from, message, command);
  if (customHandled) {
    await cleanupCommandMessage(message);
    return;
  }

  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  if (command === "/nsorteo") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }
    await handleNewRaffle(chat, from);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/sortear") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }
    await handleDrawWinner(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/reset") {
    if (!premiumActive) {
      await sendMessage(chat.id, premiumBlockText()).catch(() => null);
      await cleanupCommandMessage(message);
      return;
    }
    await handleResetRaffle(chat.id, chat.title || "");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/warn") {
    await handleWarnCommand(chat.id, chat.title || "", message);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/warns") {
    await handleWarnsCommand(chat.id, chat.title || "", message);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/clearwarns") {
    await handleClearWarnsCommand(chat.id, chat.title || "", message);
    await cleanupCommandMessage(message);
    return;
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
  const premiumActive = isPremiumActive();

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
    if (!premiumActive) {
      await sendMessage(chatId, premiumBlockText());
      return;
    }
    await handleOpenPrivateTicketContinuation(message, text);
    return;
  }

  if (state.action_key === "await_ticket_message") {
    if (!premiumActive) {
      await sendMessage(chatId, premiumBlockText());
      return;
    }
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
  if (!premiumActive && isPremiumConfigAction(actionKey)) {
    await clearUserState(from.id);
    await sendMessage(chatId, premiumBlockText());
    return;
  }

  if (actionKey === "language") {
    updated = await updateGroupSettings(targetChatId, {
      group_language: normalizeLocale(text)
    });
    await logGroupActivitySafe(targetChatId, "settings", "Idioma actualizado", `Nuevo idioma: ${updated.group_language || "es"}`);
  } else {
    updated = await updateGroupSettings(targetChatId, {
      [field]: text
    });
    await logGroupActivitySafe(targetChatId, "settings", "Configuracion actualizada", `Campo editado: ${field}`);
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

  if (!isPremiumActive() && isPremiumCallbackData(data)) {
    await answerCallbackQuery(callback.id, "Suscripcion inactiva").catch(() => null);
    const privateChatId = callback.message && callback.message.chat ? callback.message.chat.id : null;
    if (Number.isFinite(Number(privateChatId))) {
      await sendMessage(Number(privateChatId), premiumBlockText()).catch(() => null);
    }
    return;
  }

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

  if (data.indexOf("warningcfg:") === 0) {
    await handleWarningConfigCallback(callback);
    return;
  }

  if (data.indexOf("captchacfg:") === 0) {
    await handleCaptchaConfigCallback(callback);
    return;
  }

  if (data.indexOf("translationcfg:") === 0) {
    await handleTranslationConfigCallback(callback);
    return;
  }

  if (data.indexOf("grouplink:") === 0) {
    await handleGroupLinkActionCallback(callback);
    return;
  }

  if (data.indexOf("logcfg:") === 0) {
    await handleLogChannelActionCallback(callback);
    return;
  }

  if (data.indexOf("logpick:") === 0) {
    await handleLogChannelPickCallback(callback);
    return;
  }

  if (data.indexOf("verifyjoin:") === 0) {
    await handleJoinVerificationCallback(callback);
    return;
  }

  if (data.indexOf("approvejoin:") === 0) {
    await handleJoinApprovalCallback(callback, true);
    return;
  }

  if (data.indexOf("rejectjoin:") === 0) {
    await handleJoinApprovalCallback(callback, false);
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
  const profile = await getUserProfile(userId);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;
  const groups = (await getManageableGroups(userId)).filter((group) => group.chat_id !== supportChatId);

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

  await sendMessage(privateChatId, buildPrivateProspectText(locale), buildPrivateProspectKeyboard(locale));
}

async function showPrivateGroups(privateChatId, userId, locale = "es", panelMessageId = null) {
  const profile = await getUserProfile(userId);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;
  const available = (await getManageableGroups(userId)).filter((group) => group.chat_id !== supportChatId);

  if (!available.length) {
    const edited = await editPanelMessage(
      privateChatId,
      panelMessageId,
      buildPrivateProspectText(locale),
      buildPrivateProspectKeyboard(locale)
    );

    if (!edited) {
      await sendMessage(
        privateChatId,
        buildPrivateProspectText(locale),
        buildPrivateProspectKeyboard(locale)
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

  if (page === "captcha") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildCaptchaConfigText(settings),
      buildCaptchaConfigKeyboard(targetChatId, settings)
    );
    return;
  }

  if (page === "warning") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildWarningConfigText(settings),
      buildWarningConfigKeyboard(targetChatId, settings)
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

  if (page === "logs") {
    const groups = await getManageableGroups(userId);
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildLogChannelConfigText(settings),
      buildLogChannelConfigKeyboard(targetChatId, settings, groups)
    );
    return;
  }

  if (page === "translation") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildTranslationConfigText(settings),
      buildTranslationConfigKeyboard(targetChatId, settings)
    );
    return;
  }

  if (page === "staff") {
    await answerCallbackQuery(callback.id, tForSettings(settings, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildStaffConfigText(settings),
      buildSimpleBackKeyboard(targetChatId)
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
  await logGroupActivitySafe(targetChatId, "settings", "Idioma actualizado", `Nuevo idioma: ${updated.group_language || localeCode}`);

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
    await logGroupActivitySafe(targetChatId, "settings", "Enlace del grupo", `Estado: ${updated.group_link_enabled ? "activo" : "inactivo"}`);

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
    await logGroupActivitySafe(targetChatId, "settings", "Antispam", `Estado: ${updated.antispam_enabled ? "activo" : "inactivo"}`);
  } else if (action === "cycle") {
    const order = ["warn", "mute", "kick"];
    const current = order.includes(settings.antispam_action) ? settings.antispam_action : "warn";
    const next = order[(order.indexOf(current) + 1) % order.length];
    updated = await updateGroupSettings(targetChatId, {
      antispam_action: next
    });
    await logGroupActivitySafe(targetChatId, "settings", "Antispam", `Accion: ${next}`);
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

async function handleWarningConfigCallback(callback) {
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

  if (action === "silent") {
    updated = await updateGroupSettings(targetChatId, {
      silent_actions_enabled: !Boolean(settings.silent_actions_enabled)
    });
    await logGroupActivitySafe(targetChatId, "settings", "Acciones silenciosas", `Estado: ${updated.silent_actions_enabled ? "activadas" : "desactivadas"}`);
  } else if (action === "cycle") {
    const order = ["warn", "mute", "kick"];
    const current = order.includes(settings.warn_action) ? settings.warn_action : "mute";
    const next = order[(order.indexOf(current) + 1) % order.length];
    updated = await updateGroupSettings(targetChatId, {
      warn_action: next
    });
    await logGroupActivitySafe(targetChatId, "settings", "Advertencias", `Accion al limite: ${next}`);
  } else if (action === "limit") {
    await setUserState(userId, targetChatId, "warn_limit", callback.message.message_id);
    await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildEditPrompt("warn_limit", settings),
      {
        reply_markup: {
          inline_keyboard: [[
            { text: "◀️ Volver", callback_data: `cfgmenu:warning:${targetChatId}` },
            { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
          ]]
        }
      }
    );
    return;
  } else if (action === "duration") {
    await setUserState(userId, targetChatId, "warn_duration", callback.message.message_id);
    await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildEditPrompt("warn_duration", settings),
      {
        reply_markup: {
          inline_keyboard: [[
            { text: "◀️ Volver", callback_data: `cfgmenu:warning:${targetChatId}` },
            { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
          ]]
        }
      }
    );
    return;
  }

  await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildWarningConfigText(updated),
    buildWarningConfigKeyboard(targetChatId, updated)
  );
}

async function handleLogChannelActionCallback(callback) {
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

  if (action === "disable") {
    const updated = await updateGroupSettings(targetChatId, {
      log_channel_chat_id: null,
      log_channel_title: ""
    });
    await logGroupActivitySafe(targetChatId, "settings", "Canal de logs", "Canal de logs desactivado");
    const groups = await getManageableGroups(userId);
    await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildLogChannelConfigText(updated),
      buildLogChannelConfigKeyboard(targetChatId, updated, groups)
    );
    return;
  }
}

async function handleLogChannelPickCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const pickedChatId = Number(parts[2]);
  const privateChatId = callback.message.chat.id;
  const userId = callback.from.id;
  const settings = await ensureGroupSettings(targetChatId);

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, tForSettings(settings, "private_not_admin"));
    return;
  }

  const groups = await getManageableGroups(userId);
  const picked = groups.find((group) => Number(group.chat_id) === pickedChatId);
  if (!picked) {
    await answerCallbackQuery(callback.id, "Grupo no disponible");
    return;
  }

  const updated = await updateGroupSettings(targetChatId, {
    log_channel_chat_id: picked.chat_id,
    log_channel_title: picked.chat_title || "Grupo sin nombre"
  });
  await logGroupActivitySafe(targetChatId, "settings", "Canal de logs", `Conectado: ${picked.chat_title || "Grupo sin nombre"}`);

  await answerCallbackQuery(callback.id, "Canal de logs configurado");
  await sendMessage(
    picked.chat_id,
    `Canal de logs conectado para <b>${escapeHtml(updated.chat_title || "Grupo")}</b>.`
  ).catch(() => null);
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildLogChannelConfigText(updated),
    buildLogChannelConfigKeyboard(targetChatId, updated, groups)
  );
}

async function handleCaptchaConfigCallback(callback) {
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

  if (action === "cycle") {
    const order = ["off", "captcha", "approval"];
    const current = order.includes(settings.captcha_mode) ? settings.captcha_mode : "off";
    const next = order[(order.indexOf(current) + 1) % order.length];
    updated = await updateGroupSettings(targetChatId, {
      captcha_mode: next
    });
    await logGroupActivitySafe(targetChatId, "settings", "Modo de ingreso", `Captcha/aprobacion: ${next}`);
  } else if (action === "timeout") {
    await setUserState(userId, targetChatId, "captcha_timeout", callback.message.message_id);
    await answerCallbackQuery(callback.id, tForSettings(settings, "send_new_text"));
    await editMessageText(
      privateChatId,
      callback.message.message_id,
      buildEditPrompt("captcha_timeout", settings),
      {
        reply_markup: {
          inline_keyboard: [[
            { text: "◀️ Volver", callback_data: `cfgmenu:captcha:${targetChatId}` },
            { text: "✅ Cerrar", callback_data: `cfgmenu:close:${targetChatId}` }
          ]]
        }
      }
    );
    return;
  }

  await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildCaptchaConfigText(updated),
    buildCaptchaConfigKeyboard(targetChatId, updated)
  );
}

async function handleTranslationConfigCallback(callback) {
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
      translation_enabled: !Boolean(settings.translation_enabled)
    });
    await logGroupActivitySafe(
      targetChatId,
      "settings",
      "Traduccion automatica",
      `Estado: ${updated.translation_enabled ? "activa" : "inactiva"}`
    );
  } else if (action === "scope") {
    const next = String(settings.translation_scope || "non_admins") === "all" ? "non_admins" : "all";
    updated = await updateGroupSettings(targetChatId, {
      translation_scope: next
    });
    await logGroupActivitySafe(targetChatId, "settings", "Traduccion automatica", `Alcance: ${next}`);
  }

  await answerCallbackQuery(callback.id, tForSettings(updated, "preview_ready"));
  await editMessageText(
    privateChatId,
    callback.message.message_id,
    buildTranslationConfigText(updated),
    buildTranslationConfigKeyboard(targetChatId, updated)
  );
}

async function handleJoinVerificationCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const chatId = Number(parts[1]);
  const userId = Number(parts[2]);
  const key = `${chatId}:${userId}`;
  const challenge = activeJoinChallenges.get(key);

  if (!challenge || callback.from.id !== userId) {
    await answerCallbackQuery(callback.id, "Esta verificacion no te pertenece.");
    return;
  }

  clearTimeout(challenge.timer);
  activeJoinChallenges.delete(key);
  await allowChatMember(chatId, userId).catch(() => null);
  await deleteMessage(chatId, challenge.promptMessageId).catch(() => null);
  await answerCallbackQuery(callback.id, "Verificado");
  await sendLogEvent(challenge.settings, "Captcha completado", [
    `Grupo: <b>${escapeHtml(challenge.chatTitle)}</b>`,
    `Usuario: <b>${escapeHtml(challenge.userLabel)}</b>`
  ]);
  await handleWelcomeMessage({ id: chatId, title: challenge.chatTitle }, [challenge.user]);
}

async function handleJoinApprovalCallback(callback, approved) {
  const parts = String(callback.data || "").split(":");
  const chatId = Number(parts[1]);
  const userId = Number(parts[2]);
  const key = `${chatId}:${userId}`;
  const challenge = activeJoinChallenges.get(key);

  if (!challenge) {
    await answerCallbackQuery(callback.id, "Solicitud no disponible.");
    return;
  }

  if (!(await isGroupAdmin(chatId, callback.from.id))) {
    await answerCallbackQuery(callback.id, "Solo admins pueden responder.");
    return;
  }

  clearTimeout(challenge.timer);
  activeJoinChallenges.delete(key);

  if (approved) {
    await allowChatMember(chatId, userId).catch(() => null);
    await answerCallbackQuery(callback.id, "Usuario aprobado");
    await sendLogEvent(challenge.settings, "Usuario aprobado", [
      `Grupo: <b>${escapeHtml(challenge.chatTitle)}</b>`,
      `Usuario: <b>${escapeHtml(challenge.userLabel)}</b>`,
      `Admin: <b>${escapeHtml(callback.from.username ? `@${callback.from.username}` : (callback.from.first_name || "admin"))}</b>`
    ]);
    await editMessageText(
      chatId,
      challenge.promptMessageId,
      `<b>Aprobado</b>\n${escapeHtml(challenge.userLabel)} ya puede escribir en el grupo.`,
      { reply_markup: { inline_keyboard: [] } }
    ).catch(() => null);
    await handleWelcomeMessage({ id: chatId, title: challenge.chatTitle }, [challenge.user]);
    return;
  }

  await banChatMember(chatId, userId).catch(() => null);
  await answerCallbackQuery(callback.id, "Usuario rechazado");
  await sendLogEvent(challenge.settings, "Usuario rechazado", [
    `Grupo: <b>${escapeHtml(challenge.chatTitle)}</b>`,
    `Usuario: <b>${escapeHtml(challenge.userLabel)}</b>`,
    `Admin: <b>${escapeHtml(callback.from.username ? `@${callback.from.username}` : (callback.from.first_name || "admin"))}</b>`
  ]);
  await editMessageText(
    chatId,
    challenge.promptMessageId,
    `<b>Rechazado</b>\n${escapeHtml(challenge.userLabel)} fue retirado del grupo.`,
    { reply_markup: { inline_keyboard: [] } }
  ).catch(() => null);
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
  await issueGroupWarning(message.chat, settings, target, "Warn manual de administrador");
}

function buildWarnUserLabel(userId, username, firstName) {
  const label = username
    ? `@${String(username).replace(/^@/, "")}`
    : (String(firstName || "").trim() || String(userId || "Usuario"));

  if (Number.isFinite(Number(userId))) {
    return `<a href="tg://user?id=${Number(userId)}">${escapeHtml(label)}</a>`;
  }

  return escapeHtml(label);
}

async function handleWarnsCommand(chatId, chatTitle = "", message) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "database_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chatId, chatTitle || "");

  if (message.reply_to_message && message.reply_to_message.from) {
    const target = message.reply_to_message.from;
    const warningState = await getUserWarnings(chatId, target.id);
    const count = Number(warningState.count || 0);

    await sendMessage(
      chatId,
      [
        "<b>Advertencias del usuario</b>",
        "",
        buildWarnUserLabel(target.id, target.username, target.first_name),
        `Warns acumulados: <b>${count}</b>`,
        warningState.last_reason ? `Ultimo motivo: <b>${escapeHtml(warningState.last_reason)}</b>` : null,
        warningState.last_warned_at ? `Ultima advertencia: <b>${escapeHtml(warningState.last_warned_at)}</b>` : null
      ].filter(Boolean).join("\n")
    );
    return;
  }

  const warnings = await listWarningSnapshots(chatId);
  if (!warnings.length) {
    await sendMessage(chatId, "<b>Warns del grupo</b>\n\nNo hay advertencias registradas.");
    return;
  }

  const lines = ["<b>Warns del grupo</b>", ""];
  warnings.slice(0, 10).forEach((entry, index) => {
    lines.push(`${index + 1}. ${buildWarnUserLabel(entry.user_id, entry.username, entry.first_name)} - <b>${Number(entry.count || 0)}</b>`);
  });

  await sendMessage(chatId, lines.join("\n"));
}

async function handleClearWarnsCommand(chatId, chatTitle = "", message) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "database_unavailable"));
    return;
  }

  const settings = await ensureGroupSettings(chatId, chatTitle || "");

  if (!message.reply_to_message || !message.reply_to_message.from) {
    await sendMessage(chatId, "Responde al usuario al que deseas limpiar las advertencias.");
    return;
  }

  const target = message.reply_to_message.from;
  await resetUserWarnings(chatId, target.id);

  await appendGroupActivityLog(chatId, {
    type: "warning",
    title: "Advertencias limpiadas",
    summary: `${target.username ? `@${target.username}` : (target.first_name || "Usuario")} - limpiadas por admin`
  }).catch(() => null);

  await sendLogEvent(settings, "Advertencias limpiadas", [
    `Grupo: <b>${escapeHtml(chatTitle || tForSettings(settings, "group_title_fallback"))}</b>`,
    `Usuario: <b>${escapeHtml(target.username ? `@${target.username}` : (target.first_name || "Usuario"))}</b>`
  ]);

  await sendMessage(
    chatId,
    [
      "<b>Advertencias limpiadas</b>",
      "",
      buildWarnUserLabel(target.id, target.username, target.first_name)
    ].join("\n")
  );
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

    lines.push(`${staffRankIcon(item.rank)} <b>${escapeHtml(item.rank)}</b>`);
    lines.push(`       ${item.mention}`);
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

async function handleCustomGroupCommand(chat, from, message, command) {
  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const customCommand = findCustomCommand(settings.custom_commands_text, command);

  if (!customCommand) {
    return false;
  }

  if (customCommand.scope === "admin" && !(await isGroupAdmin(chat.id, from.id))) {
    return true;
  }

  const rendered = renderTemplate(customCommand.reply, {
    first_name: from.first_name || tForSettings(settings, "user_fallback"),
    full_name: [from.first_name, from.last_name].filter(Boolean).join(" "),
    username: from.username ? `@${from.username}` : from.first_name || tForSettings(settings, "user_fallback"),
    group: chat.title || tForSettings(settings, "group_title_fallback")
  });

  await sendMessage(chat.id, rendered);
  return true;
}

async function handleKeywordAutoReply(chat, from, text) {
  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const keywordReply = findKeywordReply(settings.custom_commands_text, text, Boolean(await isGroupAdmin(chat.id, from.id)));

  if (!keywordReply) {
    return false;
  }

  const rendered = renderTemplate(keywordReply.reply, {
    first_name: from.first_name || tForSettings(settings, "user_fallback"),
    full_name: [from.first_name, from.last_name].filter(Boolean).join(" "),
    username: from.username ? `@${from.username}` : from.first_name || tForSettings(settings, "user_fallback"),
    group: chat.title || tForSettings(settings, "group_title_fallback")
  });

  await sendMessage(chat.id, rendered).catch(() => null);
  await appendGroupActivityLog(chat.id, {
    type: "automation",
    title: "Auto-respuesta enviada",
    summary: `Keyword: ${keywordReply.keyword}`
  }).catch(() => null);
  return true;
}

async function handleAnnounceCommand(chat, from, message, text) {
  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  const profile = await getUserProfile(from.id);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;

  if (!supportChatId || Number(chat.id) !== supportChatId) {
    await sendMessage(chat.id, "Este comando solo puede usarse dentro del grupo configurado como customer service.").catch(() => null);
    return;
  }

  const targets = (await getManageableGroups(from.id)).filter((group) => Number(group.chat_id) !== Number(chat.id));
  if (!targets.length) {
    await sendMessage(chat.id, "No se encontraron grupos principales para enviar el anuncio.").catch(() => null);
    return;
  }

  const announcementText = String(text || "").replace(/^\/announce(@[^\s]+)?/i, "").trim();
  const deliveredGroups = [];
  const historyPreview = announcementText
    ? announcementText.slice(0, 180)
    : (message.reply_to_message && (message.reply_to_message.text || message.reply_to_message.caption || "Mensaje reenviado"));

  for (const group of targets) {
    if (message.reply_to_message) {
      const copied = await copyMessage(group.chat_id, chat.id, message.reply_to_message.message_id).catch(() => null);
      if (copied && copied.ok) {
        deliveredGroups.push(group.chat_title || String(group.chat_id));
        await appendGroupActivityLog(group.chat_id, {
          type: "announcement",
          title: "Anuncio recibido",
          summary: `Desde customer service: ${historyPreview || "Mensaje reenviado"}`
        }).catch(() => null);
      }
      continue;
    }

    if (!announcementText) {
      continue;
    }

    const sent = await sendMessage(group.chat_id, announcementText).catch(() => null);
    if (sent && sent.ok) {
      deliveredGroups.push(group.chat_title || String(group.chat_id));
      await appendGroupActivityLog(group.chat_id, {
        type: "announcement",
        title: "Anuncio recibido",
        summary: `Desde customer service: ${historyPreview || "Anuncio"}`
      }).catch(() => null);
    }
  }

  if (!message.reply_to_message && !announcementText) {
    await sendMessage(
      chat.id,
      "Usa /announce seguido del texto, o responde a un mensaje con /announce para reenviarlo a todos los grupos principales."
    ).catch(() => null);
    return;
  }

  if (!deliveredGroups.length) {
    await sendMessage(chat.id, "No se pudo enviar el mensaje a los grupos conectados.").catch(() => null);
    return;
  }

  await appendGroupActivityLog(chat.id, {
    type: "announcement",
    title: "Anuncio distribuido",
    summary: `Enviado a ${deliveredGroups.length} grupos`
  }).catch(() => null);

  await sendMessage(
    chat.id,
    [
      "<b>Mensaje enviado</b>",
      "",
      ...deliveredGroups.map((title) => `✅ ${escapeHtml(title)}`)
    ].join("\n")
  ).catch(() => null);
}

async function handleGroupVerifyCommand(chat, from) {
  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  const profile = await getUserProfile(from.id);
  const supportChatId = profile && Number.isFinite(Number(profile.support_group_chat_id))
    ? Number(profile.support_group_chat_id)
    : null;

  if (!supportChatId || Number(chat.id) !== supportChatId) {
    await sendMessage(chat.id, "Este comando solo puede usarse dentro del grupo configurado como customer service.").catch(() => null);
    return;
  }

  const targets = (await getManageableGroups(from.id)).filter((group) => Number(group.chat_id) !== Number(chat.id));
  if (!targets.length) {
    await sendMessage(chat.id, "No hay grupos principales conectados para esta cuenta.").catch(() => null);
    return;
  }

  await sendMessage(
    chat.id,
    [
      "<b>Grupos conectados</b>",
      "",
      ...targets.map((group) => `✅ ${escapeHtml(group.chat_title || String(group.chat_id))}`)
    ].join("\n")
  ).catch(() => null);
}

function ticketStatusIcon(status) {
  return String(status || "").toLowerCase() === "closed" ? "🔴" : "🟢";
}

function ticketReasonLabel(reason) {
  const value = String(reason || "").toLowerCase();
  if (value === "inactive") {
    return "inactividad";
  }
  if (value === "manual") {
    return "cierre manual";
  }
  if (value === "resolved") {
    return "resuelto";
  }
  return value || "sin motivo";
}

async function handleTicketsCommand(chatId, chatTitle = "") {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "database_unavailable"));
    return;
  }

  const tickets = await listSupportTicketsByGroup(chatId);
  if (!tickets.length) {
    await sendMessage(chatId, "<b>Historial de tickets</b>\n\nNo hay tickets registrados todavia.");
    return;
  }

  const lines = [
    "<b>Historial de tickets</b>",
    `<b>Grupo:</b> ${escapeHtml(chatTitle || "Grupo")}`,
    ""
  ];

  tickets.slice(0, 8).forEach((ticket) => {
    const userLabel = ticket.username
      ? `@${String(ticket.username).replace(/^@/, "")}`
      : (ticket.first_name || String(ticket.user_id || "Usuario"));
    const status = String(ticket.status || "open").toLowerCase();
    const lastAdmin = ticket.last_support_admin || "Sin respuesta";
    const replyCount = Number(ticket.support_reply_count || 0);
    const lastAt = ticket.last_activity_at || ticket.updated_at || ticket.created_at || "";

    lines.push(`${ticketStatusIcon(status)} <b>Ticket #${escapeHtml(ticket.ticket_number || "?")}</b>`);
    lines.push(`Usuario: <b>${escapeHtml(userLabel)}</b>`);
    lines.push(`Estado: <b>${escapeHtml(status === "closed" ? "cerrado" : "abierto")}</b>`);
    lines.push(`Respuestas soporte: <b>${replyCount}</b>`);
    lines.push(`Ultimo admin: <b>${escapeHtml(lastAdmin)}</b>`);
    if (ticket.closed_reason) {
      lines.push(`Cierre: <b>${escapeHtml(ticketReasonLabel(ticket.closed_reason))}</b>`);
    }
    if (lastAt) {
      lines.push(`Ultima actividad: <b>${escapeHtml(lastAt)}</b>`);
    }
    lines.push("");
  });

  await sendMessage(chatId, lines.join("\n"));
}

async function handleLogsCommand(chatId, chatTitle = "") {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, tForLocale("es", "database_unavailable"));
    return;
  }

  const logs = await listGroupActivityLogs(chatId, 10);
  if (!logs.length) {
    await sendMessage(chatId, "<b>Actividad reciente</b>\n\nNo hay eventos registrados todavia.");
    return;
  }

  const lines = [
    "<b>Actividad reciente</b>",
    `<b>Grupo:</b> ${escapeHtml(chatTitle || "Grupo")}`,
    ""
  ];

  logs.forEach((entry, index) => {
    lines.push(`${index + 1}. <b>${escapeHtml(entry.title || entry.type || "Evento")}</b>`);
    if (entry.summary) {
      lines.push(escapeHtml(entry.summary));
    }
    if (entry.created_at) {
      lines.push(`<b>Fecha:</b> ${escapeHtml(entry.created_at)}`);
    }
    lines.push("");
  });

  await sendMessage(chatId, lines.join("\n"));
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
  await appendGroupActivityLog(mainChatId, {
    type: "ticket",
    title: `Ticket #${ticket.ticket_number} creado`,
    summary: `Usuario: ${userLabel}`
  }).catch(() => null);
  await sendLogEvent(mainSettings, "Ticket creado", [
    `Grupo: <b>${escapeHtml(mainSettings.chat_title || String(mainChatId))}</b>`,
    `Ticket: <b>#${escapeHtml(ticket.ticket_number)}</b>`,
    `Usuario: <b>${escapeHtml(userLabel)}</b>`,
    `Mensaje: <b>${escapeHtml(messagePreview.slice(0, 180))}</b>`
  ]);
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

  const mainSettings = await ensureGroupSettings(openTicket.main_chat_id);

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

  await appendGroupActivityLog(openTicket.main_chat_id, {
    type: "ticket",
    title: `Ticket #${openTicket.ticket_number} actualizado`,
    summary: `Nuevo mensaje del usuario: ${userLabel}`
  }).catch(() => null);
  await sendLogEvent(mainSettings, "Ticket actualizado", [
    `Grupo: <b>${escapeHtml(mainSettings.chat_title || String(openTicket.main_chat_id))}</b>`,
    `Ticket: <b>#${escapeHtml(openTicket.ticket_number)}</b>`,
    `Usuario: <b>${escapeHtml(userLabel)}</b>`,
    `Mensaje: <b>${escapeHtml(messagePreview.slice(0, 180))}</b>`
  ]);

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

  const mainSettings = await ensureGroupSettings(ticket.main_chat_id);

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

  const replyAt = new Date().toISOString();
  const updatedTicket = await updateSupportTicket(ticket.id, {
    last_activity_at: replyAt,
    last_support_reply_at: replyAt,
    first_response_at: ticket.first_response_at || replyAt,
    last_support_admin: from.username ? `@${String(from.username).replace(/^@/, "")}` : ([from.first_name, from.last_name].filter(Boolean).join(" ") || String(from.id)),
    last_support_admin_id: from.id,
    support_reply_count: Number(ticket.support_reply_count || 0) + 1
  });
  await appendGroupActivityLog(ticket.main_chat_id, {
    type: "ticket",
    title: `Ticket #${ticket.ticket_number} respondido`,
    summary: `Admin: ${from.username ? `@${from.username}` : (from.first_name || String(from.id))}`
  }).catch(() => null);
  await sendLogEvent(mainSettings, "Ticket respondido", [
    `Grupo: <b>${escapeHtml(mainSettings.chat_title || String(ticket.main_chat_id))}</b>`,
    `Ticket: <b>#${escapeHtml(ticket.ticket_number)}</b>`,
    `Admin: <b>${escapeHtml(from.username ? `@${from.username}` : ([from.first_name, from.last_name].filter(Boolean).join(" ") || String(from.id)))}</b>`,
    `Respuestas soporte: <b>${Number((updatedTicket || ticket).support_reply_count || 0)}</b>`
  ]);
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
    const mainSettings = await ensureGroupSettings(fresh.main_chat_id);
    await appendGroupActivityLog(fresh.main_chat_id, {
      type: "ticket",
      title: `Ticket #${fresh.ticket_number} cerrado`,
      summary: "Cerrado automaticamente por inactividad"
    }).catch(() => null);
    await sendLogEvent(mainSettings, "Ticket cerrado", [
      `Grupo: <b>${escapeHtml(mainSettings.chat_title || String(fresh.main_chat_id))}</b>`,
      `Ticket: <b>#${escapeHtml(fresh.ticket_number)}</b>`,
      "Motivo: <b>inactividad</b>"
    ]);
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
    return 4;
  }

  if (rank === "Co-Lider") {
    return 3;
  }

  if (rank === "Lider") {
    return 2;
  }

  return 1;
}

function staffRankIcon(rank) {
  if (rank === "Propietario") {
    return "👑";
  }

  if (rank === "Co-Lider") {
    return "💎";
  }

  if (rank === "Lider") {
    return "🛡️";
  }

  return "🔹";
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
    return "Co-Lider";
  }

  if (permissionCount >= 4) {
    return "Lider";
  }

  return "Administrador";
}

async function handleWelcomeMessage(chat, newMembers) {
  if (!(await isDatabaseAvailable())) {
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const template = settings.welcome_message || getDefaultGroupSettings(getGroupLocale(settings)).welcome_message;
  const previousWelcomeId = activeWelcomeMessages.get(chat.id);
  if (previousWelcomeId) {
    await deleteMessage(chat.id, previousWelcomeId).catch(() => null);
  }

  const previousTimer = activeWelcomeDeleteTimers.get(chat.id);
  if (previousTimer) {
    clearTimeout(previousTimer);
    activeWelcomeDeleteTimers.delete(chat.id);
  }

  const names = newMembers
    .map((user) => user.username ? `@${user.username}` : user.first_name || tForSettings(settings, "user_fallback"))
    .filter(Boolean);

  const primaryUser = newMembers[0] || {};
  const firstName = newMembers.length === 1
    ? (primaryUser.first_name || tForSettings(settings, "user_fallback"))
    : names.join(", ");
  const fullName = newMembers.length === 1
    ? [primaryUser.first_name, primaryUser.last_name].filter(Boolean).join(" ")
    : names.join(", ");
  const username = newMembers.length === 1
    ? (primaryUser.username ? `@${primaryUser.username}` : primaryUser.first_name || tForSettings(settings, "user_fallback"))
    : names.join(", ");

  const text = renderTemplate(template, {
    first_name: firstName,
    full_name: fullName,
    username,
    group: chat.title || tForSettings(settings, "group_title_fallback")
  });

  const sent = await sendMessage(chat.id, text).catch(() => null);
  const newMessageId = sent && sent.ok && sent.result ? sent.result.message_id : null;
  if (newMessageId) {
    activeWelcomeMessages.set(chat.id, newMessageId);
    const deleteAfterSeconds = parseWelcomeDeleteSeconds(settings.welcome_autodelete_text);
    if (deleteAfterSeconds > 0) {
      const timer = setTimeout(async () => {
        await deleteMessage(chat.id, newMessageId).catch(() => null);
        if (activeWelcomeMessages.get(chat.id) === newMessageId) {
          activeWelcomeMessages.delete(chat.id);
        }
        activeWelcomeDeleteTimers.delete(chat.id);
      }, deleteAfterSeconds * 1000);
      activeWelcomeDeleteTimers.set(chat.id, timer);
    }
  } else {
    activeWelcomeMessages.delete(chat.id);
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

function isServiceActionMessage(message) {
  if (!message || !message.chat || message.chat.type === "private") {
    return false;
  }

  return Boolean(
    (Array.isArray(message.new_chat_members) && message.new_chat_members.length > 0) ||
    message.left_chat_member ||
    message.new_chat_title ||
    message.new_chat_photo ||
    message.delete_chat_photo ||
    message.group_chat_created ||
    message.supergroup_chat_created ||
    message.channel_chat_created ||
    message.message_auto_delete_timer_changed ||
    message.pinned_message ||
    message.migrate_to_chat_id ||
    message.migrate_from_chat_id ||
    message.forum_topic_created ||
    message.forum_topic_edited ||
    message.forum_topic_closed ||
    message.forum_topic_reopened ||
    message.general_forum_topic_hidden ||
    message.general_forum_topic_unhidden ||
    message.write_access_allowed ||
    message.users_shared ||
    message.chat_shared
  );
}

async function cleanupServiceActionMessage(message) {
  if (!isServiceActionMessage(message)) {
    return false;
  }

  const chat = message.chat || {};
  const messageId = message.message_id;
  if (!chat.id || !messageId) {
    return false;
  }

  await deleteMessage(chat.id, messageId).catch(() => null);
  return true;
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
  await sendLogEvent(settings, "Antispam detectado", [
    `Grupo: <b>${escapeHtml(chat.title || tForSettings(settings, "group_title_fallback"))}</b>`,
    `Usuario: <b>${escapeHtml(from.username ? `@${from.username}` : (from.first_name || tForSettings(settings, "user_fallback")))}</b>`,
    "Se elimino un mensaje por spam o flood."
  ]);

  if (settings.antispam_action === "warn") {
    await issueGroupWarning(chat, settings, from, "Antispam");
    return;
  }

  if (settings.antispam_action === "mute") {
    const durationSeconds = parseDurationToSeconds(settings.antispam_duration_text || "24 h");
    const untilDate = Math.floor(Date.now() / 1000) + durationSeconds;
    await restrictChatMember(chat.id, from.id, untilDate).catch(() => null);
    if (!settings.silent_actions_enabled) {
      await sendMessage(chat.id, renderTemplate(settings.warning_message || "{first_name}, no se permite enviar spam en este grupo.", {
        first_name: from.first_name || tForSettings(settings, "user_fallback"),
        username: from.username ? `@${from.username}` : from.first_name || tForSettings(settings, "user_fallback"),
        group: chat.title || tForSettings(settings, "group_title_fallback")
      })).catch(() => null);
    }
    return;
  }

  if (settings.antispam_action === "kick") {
    await banChatMember(chat.id, from.id).catch(() => null);
  }
}

async function maybeHandleAdvancedModeration(chat, from, message) {
  if (!from || !from.id || !message) {
    return false;
  }

  if (await isGroupAdmin(chat.id, from.id)) {
    return false;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const visibleText = extractVisibleMessageText(message);

  if (violatesTopicsPolicy(settings, message)) {
    await deleteMessage(chat.id, message.message_id).catch(() => null);
    await issueGroupWarning(chat, settings, from, "Topics");
    return true;
  }

  if (violatesMemberPermissions(settings, message)) {
    await deleteMessage(chat.id, message.message_id).catch(() => null);
    await issueGroupWarning(chat, settings, from, "Gestion de miembros");
    return true;
  }

  if (shouldBlockMaskedUser(settings, message)) {
    await deleteMessage(chat.id, message.message_id).catch(() => null);
    await issueGroupWarning(chat, settings, from, "Usuarios enmascarados");
    return true;
  }

  if (visibleText) {
    const bannedWords = parseBannedWords(settings.banned_words_text);
    if (bannedWords.length && containsBannedWord(visibleText, bannedWords)) {
      await deleteMessage(chat.id, message.message_id).catch(() => null);
      await issueGroupWarning(chat, settings, from, "Palabras prohibidas");
      return true;
    }

    const repeatedThreshold = extractRepeatThreshold(settings.repeated_messages_policy);
    if (repeatedThreshold > 1 && isRepeatedMessageBurst(chat.id, from.id, visibleText, repeatedThreshold)) {
      await deleteMessage(chat.id, message.message_id).catch(() => null);
      await issueGroupWarning(chat, settings, from, "Mensajes recurrentes");
      return true;
    }
  }

  return false;
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
    `<b>${escapeHtml(uiText(settings, "config_title"))}</b>`,
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    escapeHtml(uiText(settings, "config_choose"))
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
          { text: "🚨 Advertencias", callback_data: `cfgmenu:warning:${chatId}` },
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

function buildConfigCategoryKeyboard(chatId, settings, page = "main") {
  if (page === "more") {
    return {
      reply_markup: {
        inline_keyboard: [
          [{ text: uiText(settings, "topics"), callback_data: `cfg:${chatId}:topics` }],
          [{ text: uiText(settings, "banned_words"), callback_data: `cfg:${chatId}:banned_words` }],
          [{ text: uiText(settings, "repeated_messages"), callback_data: `cfg:${chatId}:repeated_messages` }],
          [{ text: uiText(settings, "member_permissions"), callback_data: `cfg:${chatId}:member_permissions` }],
          [{ text: uiText(settings, "masked_users"), callback_data: `cfg:${chatId}:masked_users` }],
          [{ text: uiText(settings, "custom_commands"), callback_data: `cfg:${chatId}:custom_commands` }],
          [{ text: uiText(settings, "translation"), callback_data: `cfgmenu:translation:${chatId}` }],
          [{ text: uiText(settings, "log_channel"), callback_data: `cfgmenu:logs:${chatId}` }],
          [
            { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
            { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` },
            { text: uiText(settings, "lang"), callback_data: `cfg:${chatId}:language` }
          ]
        ]
      }
    };
  }

  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: uiText(settings, "rules"), callback_data: `cfg:${chatId}:rules` },
          { text: uiText(settings, "antispam"), callback_data: `cfgmenu:antispam:${chatId}` }
        ],
        [
          { text: uiText(settings, "welcome"), callback_data: `cfg:${chatId}:welcome` },
          { text: uiText(settings, "antiflood"), callback_data: `cfg:${chatId}:repeated_messages` }
        ],
        [
          { text: uiText(settings, "captcha"), callback_data: `cfgmenu:captcha:${chatId}` },
          { text: uiText(settings, "filters"), callback_data: `cfg:${chatId}:banned_words` }
        ],
        [
          { text: uiText(settings, "warnings"), callback_data: `cfgmenu:warning:${chatId}` },
          { text: uiText(settings, "raffle"), callback_data: `cfgmenu:raffle:${chatId}` }
        ],
        [
          { text: uiText(settings, "staff"), callback_data: `cfgmenu:staff:${chatId}` },
          { text: uiText(settings, "group_link"), callback_data: `cfgmenu:link:${chatId}` }
        ],
        [
          { text: uiText(settings, "lang"), callback_data: `cfg:${chatId}:language` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` },
          { text: uiText(settings, "more"), callback_data: `cfgmenu:more:${chatId}` }
        ]
      ]
    }
  };
}

function buildSimpleBackKeyboard(chatId) {
  const settings = { chat_id: chatId, group_language: "es" };
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildStaffConfigText(settings) {
  return [
    "<b>STAFF DEL GRUPO</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    "El comando /staff mencionara solo a los administradores del grupo.",
    "",
    "Rangos usados por el bot:",
    "- Propietario: creator o admin con casi todos los permisos.",
    "- Co-Lider: admin con varios permisos altos.",
    "- Lider: resto de administradores activos.",
    "",
    "Los usuarios normales no se incluyen en /staff."
  ].join("\n");
}

function buildRaffleConfigKeyboard(chatId, settings) {
  const listUrl = buildRaffleListUrl(chatId);
  const rows = [
    [
      { text: `${uiText(settings, "edit")} ${uiText(settings, "rules").replace(/^[^\s]+\s/, "")}`, callback_data: `cfg:${chatId}:rules` },
      { text: `${uiText(settings, "edit")} ${tForSettings(settings, "preview_raffle_text")}`, callback_data: `cfg:${chatId}:raffle_intro` }
    ],
    [
      { text: uiText(settings, "public_list"), url: listUrl || `${config.panelUrl}/raffle_live.php?chat_id=${chatId}` }
    ],
    [
      { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
      { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
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
          { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildWarningConfigText(settings) {
  return [
    "<b>CONFIGURACION DE ADVERTENCIAS</b>",
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Warns para sancion: <b>${getWarnLimit(settings)}</b>`,
    `Accion al limite: <b>${escapeHtml(formatWarnAction(settings.warn_action))}</b>`,
    `Duracion del mute: <b>${escapeHtml(settings.warn_duration_text || "24 h")}</b>`,
    `Acciones silenciosas: <b>${settings.silent_actions_enabled ? "Activadas" : "Desactivadas"}</b>`,
    `Canal de logs: <b>${escapeHtml(getLogChannelLabel(settings))}</b>`,
    "",
    "Aqui controlas el texto de advertencia, el limite acumulado y la sancion automatica."
  ].join("\n");
}

function buildWarningConfigKeyboard(chatId, settings) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: `${uiText(settings, "edit")} ${tForSettings(settings, "preview_warning")}`, callback_data: `cfgedit:${chatId}:warning` },
          { text: `🔢 Limite: ${getWarnLimit(settings)}`, callback_data: `warningcfg:${chatId}:limit` }
        ],
        [
          { text: `⚖️ ${formatWarnAction(settings.warn_action)}`, callback_data: `warningcfg:${chatId}:cycle` },
          { text: `⏱️ ${settings.warn_duration_text || "24 h"}`, callback_data: `warningcfg:${chatId}:duration` }
        ],
        [
          {
            text: settings.silent_actions_enabled ? "🤫 Desactivar silencioso" : "📢 Activar silencioso",
            callback_data: `warningcfg:${chatId}:silent`
          }
        ],
        [
          { text: uiText(settings, "log_channel"), callback_data: `cfgmenu:logs:${chatId}` }
        ],
        [
          { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildLogChannelConfigText(settings) {
  return [
    `<b>${escapeHtml(uiText(settings, "logs_title"))}</b>`,
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Canal actual: <b>${escapeHtml(getLogChannelLabel(settings))}</b>`,
    "",
    "Elige el grupo donde quieres recibir warns, sanciones, tickets y acciones silenciosas."
  ].join("\n");
}

function buildLogChannelConfigKeyboard(chatId, settings, groups = []) {
  const choices = groups
    .slice(0, 8)
    .map((group) => {
      const isCurrent = Number(settings.log_channel_chat_id || 0) === Number(group.chat_id);
      return [{
        text: `${isCurrent ? "✅" : "📌"} ${group.chat_title || group.chat_id}`,
        callback_data: `logpick:${chatId}:${group.chat_id}`
      }];
    });

  const rows = choices.concat([
    [
      { text: uiText(settings, "disable_logs"), callback_data: `logcfg:${chatId}:disable` }
    ],
    [
      { text: uiText(settings, "back"), callback_data: `cfgmenu:warning:${chatId}` },
      { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
    ]
  ]);

  return {
    reply_markup: {
      inline_keyboard: rows
    }
  };
}

function formatCaptchaMode(mode) {
  if (mode === "captcha") {
    return "Captcha";
  }

  if (mode === "approval") {
    return "Aprobacion manual";
  }

  return "Desactivado";
}

function buildCaptchaConfigText(settings) {
  return [
    `<b>${escapeHtml(uiText(settings, "captcha_title"))}</b>`,
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Modo actual: <b>${escapeHtml(formatCaptchaMode(settings.captcha_mode))}</b>`,
    `Tiempo limite: <b>${escapeHtml(settings.captcha_timeout_text || "5 m")}</b>`,
    "",
    "Captcha restringe al usuario hasta que pulse verificar.",
    "Aprobacion manual espera a que un admin apruebe o rechace."
  ].join("\n");
}

function buildCaptchaConfigKeyboard(chatId, settings) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: `🧠 ${formatCaptchaMode(settings.captcha_mode)}`, callback_data: `captchacfg:${chatId}:cycle` },
          { text: `⏱️ ${settings.captcha_timeout_text || "5 m"}`, callback_data: `captchacfg:${chatId}:timeout` }
        ],
        [
          { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function formatTranslationScope(scope) {
  return String(scope || "non_admins") === "all" ? "Todos" : "Solo usuarios";
}

function buildTranslationConfigText(settings) {
  return [
    `<b>${escapeHtml(uiText(settings, "translation_title"))}</b>`,
    `Grupo: <b>${escapeHtml(settings.chat_title || "Grupo sincronizado")}</b>`,
    "",
    `Estado: <b>${settings.translation_enabled ? "Activada" : "Desactivada"}</b>`,
    `Destino: <b>${escapeHtml(getLocaleLabel(getGroupLocale(settings)))}</b>`,
    `Alcance: <b>${escapeHtml(formatTranslationScope(settings.translation_scope))}</b>`,
    "",
    "Si un mensaje llega en otro idioma, el bot publicara una traduccion al idioma del grupo."
  ].join("\n");
}

function buildTranslationConfigKeyboard(chatId, settings) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          {
            text: settings.translation_enabled ? "🟢 Desactivar" : "🔴 Activar",
            callback_data: `translationcfg:${chatId}:toggle`
          },
          {
            text: `👥 ${formatTranslationScope(settings.translation_scope)}`,
            callback_data: `translationcfg:${chatId}:scope`
          }
        ],
        [
          { text: uiText(settings, "back"), callback_data: `cfgmenu:more:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
        ]
      ]
    }
  };
}

function buildGroupLinkConfigText(settings) {
  return [
    `<b>${escapeHtml(uiText(settings, "group_link_title"))}</b>`,
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
          { text: uiText(settings, "back"), callback_data: `cfgmenu:main:${chatId}` },
          { text: uiText(settings, "close"), callback_data: `cfgmenu:close:${chatId}` }
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

function buildPrivateProspectText(locale = "es") {
  if (locale === "en") {
    return [
      "<b>Get your own bot</b>",
      "",
      "This private panel is only available for owners or admins who already manage groups with the bot.",
      "",
      "Contact: @IdeadigitalPeru",
      "Website: https://ideadigitalbots.xo.je/"
    ].join("\n");
  }

  return [
    "<b>Adquiere tu bot</b>",
    "",
    "Este panel privado solo se habilita para usuarios que ya administran grupos con el bot o tienen un bot activo en la plataforma.",
    "",
    "Comunicate con: @IdeadigitalPeru",
    "O ingresa a nuestra pagina web para conocer el servicio."
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

function buildPrivateProspectKeyboard(locale = "es") {
  const addUrl = config.botUsername ? `https://t.me/${config.botUsername}?startgroup=true` : config.panelUrl;
  const websiteUrl = `${config.panelUrl}/`;
  const contactUrl = "https://t.me/IdeadigitalPeru";

  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: locale === "en" ? "Visit website" : "Visitar", url: websiteUrl }
        ],
        [
          { text: locale === "en" ? "Contact support" : "Contactar", url: contactUrl }
        ],
        [
          { text: locale === "en" ? "Add bot to a group" : "Agregar bot al grupo", url: addUrl }
        ],
        [
          { text: locale === "en" ? "Help" : "Ayuda", callback_data: "home:help" }
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
    welcome_autodelete: "Envia el tiempo para autodestruir la bienvenida. Ejemplo: 30 s, 5 m, 1 h. Escribe off para desactivarlo.",
    warning: tForSettings(settings, "prompt_warning"),
    rules: "Envia el nuevo reglamento del grupo.",
    raffle_intro: tForSettings(settings, "prompt_raffle_intro"),
    language: tForSettings(settings, "prompt_language"),
    antispam_duration: "Envia la duracion del mute. Ejemplo: 1 d 24 h 17 m",
    group_link_value: "Envia el enlace del grupo. Ejemplo: https://t.me/tu_grupo"
  };

  const currentValue = {
    welcome: settings.welcome_message,
    welcome_autodelete: settings.welcome_autodelete_text || "Desactivado",
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
    welcome_autodelete: "Autoeliminar bienvenida",
    warning: tForSettings(settings, "preview_warning"),
    rules: tForSettings(settings, "preview_rules"),
    raffle_intro: tForSettings(settings, "preview_raffle_text"),
    language: tForSettings(settings, "preview_language"),
    antispam_duration: "Duracion del antispam",
    group_link_value: "Enlace del grupo"
  };

  const values = {
    welcome: settings.welcome_message || "",
    welcome_autodelete: settings.welcome_autodelete_text || "Desactivado",
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
  const rows = [];

  if (action === "welcome") {
    rows.push([
      { text: "✏️ Editar", callback_data: `cfgedit:${chatId}:${action}` },
      { text: "⏱️ Autoeliminar", callback_data: `cfgedit:${chatId}:welcome_autodelete` }
    ]);
  } else if (action === "warning") {
    rows.push([
      { text: "✏️ Editar", callback_data: `cfgedit:${chatId}:${action}` },
      { text: "◀️ Volver", callback_data: `cfgmenu:warning:${chatId}` }
    ]);
  } else {
    rows.push([
      { text: "✏️ Editar", callback_data: `cfgedit:${chatId}:${action}` },
      { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` }
    ]);
  }

  rows.push([
    { text: "◀️ Volver", callback_data: `cfgmenu:main:${chatId}` },
    { text: "✅ Cerrar", callback_data: `cfgmenu:close:${chatId}` }
  ]);

  return {
    reply_markup: {
      inline_keyboard: rows
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

function buildEditPrompt(action, settings) {
  const labels = {
    welcome: tForSettings(settings, "prompt_welcome"),
    warning: tForSettings(settings, "prompt_warning"),
    warn_limit: "Envia el numero de advertencias para sancionar automaticamente.",
    warn_duration: "Envia la duracion del mute automatico. Ejemplo: 1 d 24 h 17 m",
    rules: "Envia el nuevo reglamento del grupo.",
    raffle_intro: tForSettings(settings, "prompt_raffle_intro"),
    language: tForSettings(settings, "prompt_language"),
    antispam_duration: "Envia la duracion del mute. Ejemplo: 1 d 24 h 17 m",
    captcha_timeout: "Envia el tiempo limite de validacion. Ejemplo: 5 m",
    group_link_value: "Envia el enlace del grupo. Ejemplo: https://t.me/tu_grupo",
    topics: tForSettings(settings, "prompt_topics"),
    banned_words: tForSettings(settings, "prompt_banned_words"),
    repeated_messages: tForSettings(settings, "prompt_repeated_messages"),
    member_permissions: tForSettings(settings, "prompt_member_permissions"),
    masked_users: tForSettings(settings, "prompt_masked_users"),
    custom_commands: tForSettings(settings, "prompt_custom_commands")
  };

  const currentValue = {
    welcome: settings.welcome_message,
    warning: settings.warning_message,
    warn_limit: String(getWarnLimit(settings)),
    warn_duration: settings.warn_duration_text || "24 h",
    rules: settings.group_rules_text,
    raffle_intro: settings.raffle_intro_text,
    language: `${getGroupLocale(settings).toUpperCase()} - ${getLocaleLabel(getGroupLocale(settings))}`,
    antispam_duration: settings.antispam_duration_text || "24 h",
    captcha_timeout: settings.captcha_timeout_text || "5 m",
    group_link_value: settings.group_link_value || "Sin enlace configurado.",
    topics: settings.topics_policy || "",
    banned_words: settings.banned_words_text || "",
    repeated_messages: settings.repeated_messages_policy || "",
    member_permissions: settings.member_permissions_text || "",
    masked_users: settings.masked_users_policy || "",
    custom_commands: settings.custom_commands_text || ""
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
    warn_limit: "Limite de advertencias",
    warn_duration: "Duracion del mute automatico",
    rules: tForSettings(settings, "preview_rules"),
    raffle_intro: tForSettings(settings, "preview_raffle_text"),
    language: tForSettings(settings, "preview_language"),
    antispam_duration: "Duracion del antispam",
    captcha_timeout: "Tiempo limite de validacion",
    group_link_value: "Enlace del grupo",
    topics: tForSettings(settings, "preview_topics"),
    banned_words: tForSettings(settings, "preview_banned_words"),
    repeated_messages: tForSettings(settings, "preview_repeated_messages"),
    member_permissions: tForSettings(settings, "preview_member_permissions"),
    masked_users: tForSettings(settings, "preview_masked_users"),
    custom_commands: tForSettings(settings, "preview_custom_commands")
  };

  const values = {
    welcome: settings.welcome_message || "",
    warning: settings.warning_message || "",
    warn_limit: String(getWarnLimit(settings)),
    warn_duration: settings.warn_duration_text || "24 h",
    rules: settings.group_rules_text || "",
    raffle_intro: settings.raffle_intro_text || "",
    language: `${getGroupLocale(settings).toUpperCase()} - ${getLocaleLabel(getGroupLocale(settings))}`,
    antispam_duration: settings.antispam_duration_text || "24 h",
    captcha_timeout: settings.captcha_timeout_text || "5 m",
    group_link_value: settings.group_link_value || "Sin enlace configurado.",
    topics: settings.topics_policy || "",
    banned_words: settings.banned_words_text || "",
    repeated_messages: settings.repeated_messages_policy || "",
    member_permissions: settings.member_permissions_text || "",
    masked_users: settings.masked_users_policy || "",
    custom_commands: settings.custom_commands_text || ""
  };

  return [
    `<b>${escapeHtml(labels[action] || action)}</b>`,
    "",
    escapeHtml(values[action] || ""),
    "",
    "Pulsa Editar si deseas cambiar este valor o Volver para regresar al menu."
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

function uiText(settings, key) {
  const locale = getGroupLocale(settings);
  const catalog = {
    es: {
      config_title: "CONFIGURACION",
      config_choose: "Elige cual de los ajustes quieres editar.",
      rules: "📜 Reglamento",
      antispam: "🛡️ Antispam",
      welcome: "💬 Bienvenida",
      antiflood: "🌊 Anti-flood",
      captcha: "🧠 Captcha",
      filters: "🧪 Filtros",
      warnings: "🚨 Advertencias",
      raffle: "🎁 Sorteo",
      staff: "👥 Staff",
      group_link: "🔗 Enlace del grupo",
      lang: "🌐 Lang",
      close: "✅ Cerrar",
      more: "▶️ Mas",
      back: "◀️ Volver",
      topics: "📁 Temas / Topics",
      banned_words: "🔤 Palabras prohibidas",
      repeated_messages: "🕘 Mensajes recurrentes",
      member_permissions: "👥 Gestion de miembros",
      masked_users: "🫥 Usuarios enmascarados",
      custom_commands: "📱 Comandos personales",
      translation: "🌐 Traduccion",
      log_channel: "🧾 Canal de logs",
      raffle_config_title: "CONFIGURACION DEL SORTEO",
      staff_title: "STAFF DEL GRUPO",
      antispam_title: "CONFIGURACION ANTISPAM",
      warnings_title: "CONFIGURACION DE ADVERTENCIAS",
      logs_title: "CANAL DE LOGS",
      captcha_title: "CAPTCHA Y APROBACION",
      translation_title: "TRADUCCION AUTOMATICA",
      group_link_title: "ENLACE DEL GRUPO",
      customer_service_title: "CUSTOMER SERVICE GROUP",
      edit: "✏️ Editar",
      auto_delete: "⏱️ Autoeliminar",
      public_list: "🌐 Ver lista publica",
      disable_logs: "🚫 Desconectar logs"
    },
    en: {
      config_title: "SETTINGS",
      config_choose: "Choose which setting you want to edit.",
      rules: "📜 Rules",
      antispam: "🛡️ Antispam",
      welcome: "💬 Welcome",
      antiflood: "🌊 Anti-flood",
      captcha: "🧠 Captcha",
      filters: "🧪 Filters",
      warnings: "🚨 Warnings",
      raffle: "🎁 Raffle",
      staff: "👥 Staff",
      group_link: "🔗 Group link",
      lang: "🌐 Lang",
      close: "✅ Close",
      more: "▶️ More",
      back: "◀️ Back",
      topics: "📁 Topics",
      banned_words: "🔤 Banned words",
      repeated_messages: "🕘 Repeated messages",
      member_permissions: "👥 Member permissions",
      masked_users: "🫥 Masked users",
      custom_commands: "📱 Custom commands",
      translation: "🌐 Translation",
      log_channel: "🧾 Log channel",
      raffle_config_title: "RAFFLE SETTINGS",
      staff_title: "GROUP STAFF",
      antispam_title: "ANTISPAM SETTINGS",
      warnings_title: "WARNING SETTINGS",
      logs_title: "LOG CHANNEL",
      captcha_title: "CAPTCHA AND APPROVAL",
      translation_title: "AUTO TRANSLATION",
      group_link_title: "GROUP LINK",
      customer_service_title: "CUSTOMER SERVICE GROUP",
      edit: "✏️ Edit",
      auto_delete: "⏱️ Auto-delete",
      public_list: "🌐 View public list",
      disable_logs: "🚫 Disconnect logs"
    },
    fr: {
      config_title: "CONFIGURATION",
      config_choose: "Choisissez le parametre que vous voulez modifier.",
      rules: "📜 Regles",
      antispam: "🛡️ Anti-spam",
      welcome: "💬 Bienvenue",
      antiflood: "🌊 Anti-flood",
      captcha: "🧠 Captcha",
      filters: "🧪 Filtres",
      warnings: "🚨 Avertissements",
      raffle: "🎁 Tirage",
      staff: "👥 Staff",
      group_link: "🔗 Lien du groupe",
      lang: "🌐 Lang",
      close: "✅ Fermer",
      more: "▶️ Plus",
      back: "◀️ Retour",
      topics: "📁 Topics",
      banned_words: "🔤 Mots interdits",
      repeated_messages: "🕘 Messages repetes",
      member_permissions: "👥 Permissions des membres",
      masked_users: "🫥 Utilisateurs masques",
      custom_commands: "📱 Commandes personnelles",
      translation: "🌐 Traduction",
      log_channel: "🧾 Canal de logs",
      raffle_config_title: "CONFIGURATION DU TIRAGE",
      staff_title: "STAFF DU GROUPE",
      antispam_title: "CONFIGURATION ANTI-SPAM",
      warnings_title: "CONFIGURATION DES AVERTISSEMENTS",
      logs_title: "CANAL DE LOGS",
      captcha_title: "CAPTCHA ET APPROBATION",
      translation_title: "TRADUCTION AUTOMATIQUE",
      group_link_title: "LIEN DU GROUPE",
      customer_service_title: "GROUPE CUSTOMER SERVICE",
      edit: "✏️ Modifier",
      auto_delete: "⏱️ Auto-suppression",
      public_list: "🌐 Voir la liste publique",
      disable_logs: "🚫 Deconnecter les logs"
    },
    pt: {
      config_title: "CONFIGURACAO",
      config_choose: "Escolha qual ajuste deseja editar.",
      rules: "📜 Regras",
      antispam: "🛡️ Antispam",
      welcome: "💬 Boas-vindas",
      antiflood: "🌊 Anti-flood",
      captcha: "🧠 Captcha",
      filters: "🧪 Filtros",
      warnings: "🚨 Avisos",
      raffle: "🎁 Sorteio",
      staff: "👥 Staff",
      group_link: "🔗 Link do grupo",
      lang: "🌐 Lang",
      close: "✅ Fechar",
      more: "▶️ Mais",
      back: "◀️ Voltar",
      topics: "📁 Topics",
      banned_words: "🔤 Palavras proibidas",
      repeated_messages: "🕘 Mensagens repetidas",
      member_permissions: "👥 Permissoes dos membros",
      masked_users: "🫥 Usuarios mascarados",
      custom_commands: "📱 Comandos personalizados",
      translation: "🌐 Traducao",
      log_channel: "🧾 Canal de logs",
      raffle_config_title: "CONFIGURACAO DO SORTEIO",
      staff_title: "STAFF DO GRUPO",
      antispam_title: "CONFIGURACAO ANTISPAM",
      warnings_title: "CONFIGURACAO DE AVISOS",
      logs_title: "CANAL DE LOGS",
      captcha_title: "CAPTCHA E APROVACAO",
      translation_title: "TRADUCAO AUTOMATICA",
      group_link_title: "LINK DO GRUPO",
      customer_service_title: "GRUPO CUSTOMER SERVICE",
      edit: "✏️ Editar",
      auto_delete: "⏱️ Auto-apagar",
      public_list: "🌐 Ver lista publica",
      disable_logs: "🚫 Desconectar logs"
    },
    it: {
      config_title: "CONFIGURAZIONE",
      config_choose: "Scegli quale impostazione vuoi modificare.",
      rules: "📜 Regole",
      antispam: "🛡️ Antispam",
      welcome: "💬 Benvenuto",
      antiflood: "🌊 Anti-flood",
      captcha: "🧠 Captcha",
      filters: "🧪 Filtri",
      warnings: "🚨 Avvisi",
      raffle: "🎁 Sorteggio",
      staff: "👥 Staff",
      group_link: "🔗 Link del gruppo",
      lang: "🌐 Lang",
      close: "✅ Chiudi",
      more: "▶️ Altro",
      back: "◀️ Indietro",
      topics: "📁 Topics",
      banned_words: "🔤 Parole vietate",
      repeated_messages: "🕘 Messaggi ripetuti",
      member_permissions: "👥 Permessi membri",
      masked_users: "🫥 Utenti mascherati",
      custom_commands: "📱 Comandi personalizzati",
      translation: "🌐 Traduzione",
      log_channel: "🧾 Canale log",
      raffle_config_title: "CONFIGURAZIONE DEL SORTEGGIO",
      staff_title: "STAFF DEL GRUPPO",
      antispam_title: "CONFIGURAZIONE ANTISPAM",
      warnings_title: "CONFIGURAZIONE AVVISI",
      logs_title: "CANALE DEI LOG",
      captcha_title: "CAPTCHA E APPROVAZIONE",
      translation_title: "TRADUZIONE AUTOMATICA",
      group_link_title: "LINK DEL GRUPPO",
      customer_service_title: "GRUPPO CUSTOMER SERVICE",
      edit: "✏️ Modifica",
      auto_delete: "⏱️ Auto-elimina",
      public_list: "🌐 Vedi lista pubblica",
      disable_logs: "🚫 Disconnetti log"
    },
    ar: {
      config_title: "الاعدادات",
      config_choose: "اختر الاعداد الذي تريد تعديله.",
      rules: "📜 القوانين",
      antispam: "🛡️ مكافحة السبام",
      welcome: "💬 الترحيب",
      antiflood: "🌊 مكافحة الاغراق",
      captcha: "🧠 التحقق",
      filters: "🧪 الفلاتر",
      warnings: "🚨 التحذيرات",
      raffle: "🎁 السحب",
      staff: "👥 الطاقم",
      group_link: "🔗 رابط المجموعة",
      lang: "🌐 اللغة",
      close: "✅ اغلاق",
      more: "▶️ المزيد",
      back: "◀️ رجوع",
      topics: "📁 المواضيع",
      banned_words: "🔤 الكلمات المحظورة",
      repeated_messages: "🕘 الرسائل المتكررة",
      member_permissions: "👥 صلاحيات الاعضاء",
      masked_users: "🫥 المستخدمون المقنعون",
      custom_commands: "📱 الاوامر المخصصة",
      translation: "🌐 الترجمة",
      log_channel: "🧾 قناة السجلات",
      raffle_config_title: "اعدادات السحب",
      staff_title: "طاقم المجموعة",
      antispam_title: "اعدادات مكافحة السبام",
      warnings_title: "اعدادات التحذيرات",
      logs_title: "قناة السجلات",
      captcha_title: "التحقق والموافقة",
      translation_title: "الترجمة التلقائية",
      group_link_title: "رابط المجموعة",
      customer_service_title: "مجموعة خدمة العملاء",
      edit: "✏️ تعديل",
      auto_delete: "⏱️ حذف تلقائي",
      public_list: "🌐 عرض القائمة العامة",
      disable_logs: "🚫 فصل السجلات"
    }
  };

  const chosen = catalog[locale] || catalog.es;
  return chosen[key] || catalog.es[key] || key;
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

function parseBooleanRule(source, aliases = []) {
  const value = String(source || "").toLowerCase();
  for (const alias of aliases) {
    const normalized = String(alias || "").toLowerCase();
    if (!normalized) {
      continue;
    }

    if (new RegExp(`${normalized}\\s*[:=]?\\s*(si|sí|yes|on|true|permitido|permitida|allow)`, "i").test(value)) {
      return true;
    }

    if (new RegExp(`${normalized}\\s*[:=]?\\s*(no|off|false|bloqueado|bloqueada|denegado|denegada|forbid)`, "i").test(value)) {
      return false;
    }
  }

  return null;
}

function getMessageContentType(message) {
  if (message.photo) {
    return "images";
  }

  if (message.video || message.video_note) {
    return "videos";
  }

  if (message.audio || message.voice) {
    return "audio";
  }

  if (message.document) {
    return "documents";
  }

  if (message.animation) {
    return "gifs";
  }

  if (message.sticker) {
    return "stickers";
  }

  if (message.text) {
    return "text";
  }

  return "";
}

function violatesTopicsPolicy(settings, message) {
  const policy = String(settings.topics_policy || "").toLowerCase();
  if (!policy) {
    return false;
  }

  const onlyTopics = policy.includes("solo en temas") || policy.includes("only in topics") || policy.includes("requerir topics");
  const blockTopics = policy.includes("sin temas") || policy.includes("bloquear temas") || policy.includes("block topics");

  if (onlyTopics && !message.is_topic_message) {
    return true;
  }

  if (blockTopics && message.is_topic_message) {
    return true;
  }

  return false;
}

function violatesMemberPermissions(settings, message) {
  const policy = String(settings.member_permissions_text || "").toLowerCase();
  if (!policy) {
    return false;
  }

  const type = getMessageContentType(message);
  if (!type) {
    return false;
  }

  const aliases = {
    text: ["texto", "text", "mensajes"],
    images: ["imagenes", "imágenes", "fotos", "images", "photos"],
    videos: ["videos", "video", "video circular"],
    audio: ["audios", "audio", "voz", "voice", "nota de voz"],
    documents: ["documentos", "document", "files", "archivos"],
    gifs: ["gifs", "gif", "animaciones", "animations"],
    stickers: ["stickers", "sticker"]
  };

  const rule = parseBooleanRule(policy, aliases[type] || []);
  return rule === false;
}

function parseBannedWords(value) {
  return String(value || "")
    .split(/[\n,]+/)
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function containsBannedWord(text, bannedWords) {
  const normalized = String(text || "").toLowerCase();
  return bannedWords.some((word) => normalized.includes(word));
}

function extractRepeatThreshold(policyText) {
  const match = String(policyText || "").match(/(\d+)/);
  return match ? Math.max(0, Number(match[1] || 0)) : 0;
}

function isRepeatedMessageBurst(chatId, userId, text, threshold) {
  const key = `${chatId}:${userId}`;
  const now = Date.now();
  const normalizedText = normalizeSpamText(text);
  const previous = repeatedMessageTracker.get(key) || {
    timestamps: [],
    lastText: "",
    sameTextCount: 0
  };

  const timestamps = [...previous.timestamps.filter((value) => now - value < 2 * 60 * 1000), now];
  const sameTextCount = previous.lastText === normalizedText ? previous.sameTextCount + 1 : 1;

  repeatedMessageTracker.set(key, {
    timestamps,
    lastText: normalizedText,
    sameTextCount
  });

  return sameTextCount >= threshold;
}

function shouldBlockMaskedUser(settings, message) {
  const policy = String(settings.masked_users_policy || "").toLowerCase();
  if (!(policy.includes("bloque") || policy.includes("block"))) {
    return false;
  }

  return Boolean(message.sender_chat && !message.is_automatic_forward);
}

function shouldTranslateMessage(settings, isAdmin) {
  if (!settings || !settings.translation_enabled) {
    return false;
  }

  const scope = String(settings.translation_scope || "non_admins");
  if (scope === "all") {
    return true;
  }

  return !isAdmin;
}

function looksLikeOnlyTargetLanguage(text, targetLocale) {
  const sample = String(text || "").trim();
  if (!sample) {
    return true;
  }

  if (targetLocale === "ar") {
    return /[\u0600-\u06FF]/.test(sample);
  }

  if (targetLocale === "en") {
    return /^[\x00-\x7F\s.,!?@#%&*()_\-+=:;"'`~\/\\[\]{}<>|0-9]+$/.test(sample);
  }

  return false;
}

async function translateTextToLocale(text, targetLocale) {
  const value = String(text || "").trim();
  if (!value) {
    return "";
  }

  const target = normalizeLocale(targetLocale || "es");
  const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=${encodeURIComponent(target)}&dt=t&q=${encodeURIComponent(value)}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Accept": "application/json"
      }
    });

    if (!response.ok) {
      return "";
    }

    const payload = await response.json();
    if (!Array.isArray(payload) || !Array.isArray(payload[0])) {
      return "";
    }

    return payload[0]
      .map((entry) => (Array.isArray(entry) ? String(entry[0] || "") : ""))
      .join("")
      .trim();
  } catch (_error) {
    return "";
  }
}

async function maybeHandleAutoTranslation(chat, from, message, settings) {
  const visibleText = extractVisibleMessageText(message);
  if (!visibleText || visibleText.length < 2) {
    return;
  }

  const isAdmin = await isGroupAdmin(chat.id, from.id);
  if (!shouldTranslateMessage(settings, isAdmin)) {
    return;
  }

  const targetLocale = getGroupLocale(settings);
  if (looksLikeOnlyTargetLanguage(visibleText, targetLocale)) {
    return;
  }

  const translated = await translateTextToLocale(visibleText, targetLocale);
  if (!translated || translated.toLowerCase() === visibleText.toLowerCase()) {
    return;
  }

  const localeTitle = String(getLocaleLabel(targetLocale) || targetLocale || "Translation")
    .trim()
    .toUpperCase();

  await sendMessage(
    chat.id,
    [
      `<b>${escapeHtml(localeTitle)}:</b>`,
      escapeHtml(translated)
    ].join("\n"),
    {
      reply_to_message_id: Number(message.message_id)
    }
  ).catch(() => null);
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

function getWarnLimit(settings) {
  const value = Number(String(settings.warn_limit_text || "3").match(/\d+/)?.[0] || 3);
  return Math.max(1, value);
}

function formatWarnAction(action) {
  if (action === "kick") {
    return "Expulsar";
  }

  if (action === "mute") {
    return "Silenciar";
  }

  return "Solo advertir";
}

function getLogChannelLabel(settings) {
  return settings.log_channel_title || "No configurado";
}

async function sendLogEvent(settings, title, lines = []) {
  const targetChatId = Number(settings.log_channel_chat_id || 0);
  if (!Number.isFinite(targetChatId) || !targetChatId) {
    return;
  }

  const payload = [
    `<b>${escapeHtml(title)}</b>`,
    ...lines.filter(Boolean).map((line) => String(line))
  ].join("\n");

  await sendMessage(targetChatId, payload).catch(() => null);
}

async function sendWarningNotice(chat, settings, renderedText) {
  if (settings.silent_actions_enabled) {
    return;
  }

  await sendMessage(chat.id, renderedText).catch(() => null);
}

async function applyWarnLimitPenalty(chat, settings, user, warningState, sourceLabel) {
  const limit = getWarnLimit(settings);
  const count = Number(warningState.count || 0);

  if (count < limit) {
    return false;
  }

  const action = String(settings.warn_action || "mute").toLowerCase();
  const userLabel = user.username ? `@${user.username}` : (user.first_name || tForSettings(settings, "user_fallback"));
  let penaltyLine = "Sin sancion automatica.";

  if (action === "mute") {
    const durationSeconds = parseDurationToSeconds(settings.warn_duration_text || "24 h");
    const untilDate = Math.floor(Date.now() / 1000) + durationSeconds;
    await restrictChatMember(chat.id, user.id, untilDate).catch(() => null);
    penaltyLine = `Silenciado por ${escapeHtml(settings.warn_duration_text || "24 h")}.`;
  } else if (action === "kick") {
    await banChatMember(chat.id, user.id).catch(() => null);
    penaltyLine = "Expulsado del grupo.";
  }

  await sendLogEvent(settings, "Sancion automatica", [
    `Grupo: <b>${escapeHtml(chat.title || tForSettings(settings, "group_title_fallback"))}</b>`,
    `Usuario: <b>${escapeHtml(userLabel)}</b>`,
    `Motivo: <b>${escapeHtml(sourceLabel)}</b>`,
    `Warns acumulados: <b>${count}/${limit}</b>`,
    `Accion: <b>${escapeHtml(formatWarnAction(action))}</b>`,
    penaltyLine
  ]);

  await resetUserWarnings(chat.id, user.id);
  return true;
}

async function issueGroupWarning(chat, settings, user, sourceLabel) {
  const warningState = await incrementUserWarnings(chat.id, user, sourceLabel);
  const count = Number(warningState.count || 0);
  const limit = getWarnLimit(settings);
  const rendered = renderTemplate(settings.warning_message, {
    first_name: user.first_name || tForSettings(settings, "user_fallback"),
    username: user.username ? `@${user.username}` : user.first_name || tForSettings(settings, "user_fallback"),
    group: chat.title || tForSettings(settings, "group_title_fallback")
  });

  await sendWarningNotice(chat, settings, `${rendered}\n\nWarns: <b>${count}/${limit}</b>`);

  await sendLogEvent(settings, "Advertencia registrada", [
    `Grupo: <b>${escapeHtml(chat.title || tForSettings(settings, "group_title_fallback"))}</b>`,
    `Usuario: <b>${escapeHtml(user.username ? `@${user.username}` : (user.first_name || tForSettings(settings, "user_fallback")))}</b>`,
    `Motivo: <b>${escapeHtml(sourceLabel)}</b>`,
    `Warns: <b>${count}/${limit}</b>`
  ]);

  await appendGroupActivityLog(chat.id, {
    type: "warning",
    title: "Advertencia registrada",
    summary: `${user.username ? `@${user.username}` : (user.first_name || "Usuario")} - ${sourceLabel} - ${count}/${limit}`
  }).catch(() => null);

  await applyWarnLimitPenalty(chat, settings, user, warningState, sourceLabel);
  return warningState;
}

function parseCustomCommands(source) {
  return String(source || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = line.match(/^\/([a-z0-9_]+)(?:\s*\|\s*(admin|all))?\s*[:=\-]\s*(.+)$/i);
      if (!match) {
        return null;
      }

      return {
        command: `/${String(match[1]).toLowerCase()}`,
        scope: String(match[2] || "all").toLowerCase(),
        reply: String(match[3] || "").trim()
      };
    })
    .filter(Boolean);
}

function parseKeywordReplies(source) {
  return String(source || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = line.match(/^(.+?)(?:\s*\|\s*(admin|all))?\s*=>\s*(.+)$/i);
      if (!match) {
        return null;
      }

      const keyword = String(match[1] || "").trim();
      if (!keyword || keyword.startsWith("/")) {
        return null;
      }

      return {
        keyword: keyword.toLowerCase(),
        scope: String(match[2] || "all").toLowerCase(),
        reply: String(match[3] || "").trim()
      };
    })
    .filter(Boolean);
}

function findCustomCommand(source, command) {
  const target = String(command || "").toLowerCase();
  return parseCustomCommands(source).find((item) => item.command === target) || null;
}

function findKeywordReply(source, text, isAdmin = false) {
  const target = String(text || "").trim().toLowerCase();
  if (!target) {
    return null;
  }

  return parseKeywordReplies(source).find((item) => {
    if (item.scope === "admin" && !isAdmin) {
      return false;
    }

    return target.includes(item.keyword);
  }) || null;
}

function parseWelcomeDeleteSeconds(value) {
  const input = String(value || "").trim().toLowerCase();
  if (!input || input === "off" || input === "no" || input === "0") {
    return 0;
  }

  return parseDurationToSeconds(input);
}

function getCaptchaTimeoutSeconds(settings) {
  return parseDurationToSeconds(settings.captcha_timeout_text || "5 m");
}

function getEffectiveJoinGateMode(chatId, settings, incomingCount = 1) {
  const now = Date.now();
  const explicitMode = String(settings.captcha_mode || "off").toLowerCase();
  const existingRaid = activeRaidProtection.get(chatId);

  if (existingRaid && existingRaid.expiresAt > now) {
    return existingRaid.mode;
  }

  if (existingRaid && existingRaid.expiresAt <= now) {
    activeRaidProtection.delete(chatId);
  }

  const recent = (recentJoinTracker.get(chatId) || []).filter((ts) => now - ts <= ANTI_RAID_WINDOW_MS);
  for (let index = 0; index < Math.max(1, incomingCount); index += 1) {
    recent.push(now);
  }
  recentJoinTracker.set(chatId, recent);

  if (explicitMode === "captcha" || explicitMode === "approval") {
    return explicitMode;
  }

  if (recent.length >= ANTI_RAID_JOIN_THRESHOLD) {
    activeRaidProtection.set(chatId, {
      mode: "captcha",
      expiresAt: now + ANTI_RAID_HOLD_MS
    });
    return "captcha";
  }

  return "off";
}

async function handleJoinGate(chat, newMembers) {
  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const configuredMode = String(settings.captcha_mode || "off").toLowerCase();
  const mode = getEffectiveJoinGateMode(chat.id, settings, Array.isArray(newMembers) ? newMembers.length : 1);

  if (mode !== "captcha" && mode !== "approval") {
    await handleWelcomeMessage(chat, newMembers);
    return;
  }

  if (configuredMode === "off" && mode === "captcha") {
    const recentCount = (recentJoinTracker.get(chat.id) || []).length;
    await appendGroupActivityLog(chat.id, {
      type: "security",
      title: "Anti-raid activado",
      summary: `${recentCount} ingresos detectados en menos de 60 segundos`
    }).catch(() => null);

    await sendLogEvent(settings, "Anti-raid activado", [
      `Grupo: <b>${escapeHtml(chat.title || tForSettings(settings, "group_title_fallback"))}</b>`,
      `Ingresos detectados: <b>${recentCount}</b>`,
      `Proteccion temporal: <b>Captcha automatico</b>`
    ]);
  }

  const timeoutSeconds = Math.max(60, getCaptchaTimeoutSeconds(settings));

  for (const user of newMembers) {
    if (!user || !user.id || user.is_bot) {
      continue;
    }

    const userLabel = user.username ? `@${user.username}` : (user.first_name || tForSettings(settings, "user_fallback"));
    await restrictChatMember(chat.id, user.id, Math.floor(Date.now() / 1000) + timeoutSeconds).catch(() => null);

    let promptText = "";
    let keyboard = [];

    if (mode === "captcha") {
      promptText = `<b>Verificacion requerida</b>\n${escapeHtml(userLabel)}, pulsa el boton para activar tu acceso al grupo.`;
      keyboard = [[{ text: "✅ Verificar acceso", callback_data: `verifyjoin:${chat.id}:${user.id}` }]];
    } else {
      promptText = `<b>Aprobacion pendiente</b>\n${escapeHtml(userLabel)} esta esperando la aprobacion de un administrador.`;
      keyboard = [[
        { text: "✅ Aprobar", callback_data: `approvejoin:${chat.id}:${user.id}` },
        { text: "🚫 Rechazar", callback_data: `rejectjoin:${chat.id}:${user.id}` }
      ]];
    }

    const sent = await sendMessage(chat.id, promptText, {
      reply_markup: { inline_keyboard: keyboard }
    }).catch(() => null);

    const promptMessageId = sent && sent.ok && sent.result ? sent.result.message_id : null;
    const key = `${chat.id}:${user.id}`;
    const timer = setTimeout(async () => {
      const current = activeJoinChallenges.get(key);
      if (!current) {
        return;
      }

      activeJoinChallenges.delete(key);
      await banChatMember(chat.id, user.id).catch(() => null);
      if (current.promptMessageId) {
        await editMessageText(
          chat.id,
          current.promptMessageId,
          `<b>Acceso vencido</b>\n${escapeHtml(current.userLabel)} no completo la validacion a tiempo.`,
          { reply_markup: { inline_keyboard: [] } }
        ).catch(() => null);
      }
      await sendLogEvent(settings, "Ingreso vencido", [
        `Grupo: <b>${escapeHtml(chat.title || tForSettings(settings, "group_title_fallback"))}</b>`,
        `Usuario: <b>${escapeHtml(current.userLabel)}</b>`,
        `Modo: <b>${escapeHtml(formatCaptchaMode(mode))}</b>`
      ]);
    }, timeoutSeconds * 1000);

    activeJoinChallenges.set(key, {
      user,
      userLabel,
      chatTitle: chat.title || tForSettings(settings, "group_title_fallback"),
      settings,
      promptMessageId,
      timer
    });
  }
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

async function maybeSendPremiumReminders() {
  const bots = await listAllBots().catch(() => []);
  if (!Array.isArray(bots) || !bots.length) {
    return;
  }

  for (const bot of bots) {
    if (!bot || String(bot.status || "active") !== "active") {
      continue;
    }

    if (String(bot.subscription_status || "inactive") !== "active") {
      continue;
    }

    if (!bot.owner_telegram_id || !bot.premium_until) {
      continue;
    }

    const remainingHours = hoursUntil(bot.premium_until);
    if (remainingHours === null) {
      continue;
    }

    const patch = {};
    let message = "";

    if (remainingHours <= 0 && !bot.premium_expired_notified_at) {
      message = [
        "<b>PREMIUM VENCIDO</b>",
        "",
        `Bot: ${escapeHtml(bot.bot_name || "Bot")}`,
        `Vencio: ${escapeHtml(formatPremiumDate(bot.premium_until))}`,
        "Renueva tu suscripcion desde la web para seguir usando funciones premium."
      ].join("\n");
      patch.premium_expired_notified_at = new Date().toISOString();
      patch.subscription_status = "inactive";
    } else if (remainingHours <= 24 && !bot.premium_reminder_1d_at) {
      message = [
        "<b>RECORDATORIO DE VENCIMIENTO</b>",
        "",
        `Bot: ${escapeHtml(bot.bot_name || "Bot")}`,
        `Tu premium vence en menos de 1 dia.`,
        `Vence: ${escapeHtml(formatPremiumDate(bot.premium_until))}`
      ].join("\n");
      patch.premium_reminder_1d_at = new Date().toISOString();
    } else if (remainingHours <= 24 * 7 && !bot.premium_reminder_7d_at) {
      message = [
        "<b>RECORDATORIO DE VENCIMIENTO</b>",
        "",
        `Bot: ${escapeHtml(bot.bot_name || "Bot")}`,
        `Tu premium vence en menos de 7 dias.`,
        `Vence: ${escapeHtml(formatPremiumDate(bot.premium_until))}`
      ].join("\n");
      patch.premium_reminder_7d_at = new Date().toISOString();
    }

    if (!message) {
      continue;
    }

    await runWithBot(bot, async () => {
      await sendMessage(bot.owner_telegram_id, message).catch(() => null);
    });

    await updateBotSubscription(bot.id, patch).catch(() => null);
  }
}

function startBackgroundJobs() {
  if (backgroundJobsStarted) {
    return;
  }

  backgroundJobsStarted = true;
  setInterval(() => {
    maybeSendPremiumReminders().catch(() => null);
  }, PREMIUM_REMINDER_INTERVAL_MS);

  maybeSendPremiumReminders().catch(() => null);
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

  startBackgroundJobs();
}

startServer().catch((error) => {
  console.error(error);
  process.exit(1);
});
