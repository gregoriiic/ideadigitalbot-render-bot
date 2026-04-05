const express = require("express");
const crypto = require("crypto");
const config = require("./config");
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
  raffle_intro: "raffle_intro_text"
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
    await sendMessage(chat.id, buildGroupHelpText());
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
    await sendMessage(chat.id, settings.raffle_rules_text || "No rules configured yet.");
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/staff") {
    await handleStaffCommand(chat.id);
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
    await handleDrawWinner(chat.id);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/reset") {
    await handleResetRaffle(chat.id);
    await cleanupCommandMessage(message);
    return;
  }

  if (command === "/warn") {
    await handleWarnCommand(chat.id, message);
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

    await sendMessage(chatId, buildPrivateWelcomeText());
    return;
  }

  if (command === "/help") {
    await sendMessage(chatId, buildPrivateHelpText());
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
    await sendMessage(chatId, "You are no longer an administrator in that group.");
    return;
  }

  const updated = await updateGroupSettings(targetChatId, {
    [field]: text
  });

  await clearUserState(from.id);
  await sendMessage(
    chatId,
    `Saved successfully for group <b>${escapeHtml(updated.chat_title || String(targetChatId))}</b>.\n\n${buildSettingPreview(actionKey, updated[field])}`,
    buildManageKeyboard(targetChatId)
  );
}

async function handlePrivateManageStart(privateChatId, userId, targetChatId) {
  const member = await getChatMember(targetChatId, userId);

  if (!member.ok || !isMemberAdminStatus(member.result.status)) {
    await sendMessage(privateChatId, "You must be an admin of that group to edit its settings.");
    return;
  }

  const title = member.result.chat ? member.result.chat.title : "";
  await ensureGroupSettings(targetChatId, title);
  await sendMessage(
    privateChatId,
    `You are now managing <b>${escapeHtml(title || String(targetChatId))}</b>.\nChoose what you want to edit:`,
    buildManageKeyboard(targetChatId)
  );
}

async function handleCallbackQuery(callback) {
  const data = callback.data || "";

  if (data.indexOf("raffle_join:") === 0) {
    await handleRaffleJoin(callback);
    return;
  }

  if (data.indexOf("cfg:") === 0) {
    await handleConfigCallback(callback);
    return;
  }
}

async function handleConfigCallback(callback) {
  const parts = String(callback.data || "").split(":");
  const targetChatId = Number(parts[1]);
  const action = parts[2];
  const userId = callback.from.id;
  const privateChatId = callback.message.chat.id;

  if (!(await isGroupAdmin(targetChatId, userId))) {
    await answerCallbackQuery(callback.id, "You are not an admin in that group.");
    return;
  }

  const settings = await ensureGroupSettings(targetChatId);

  if (action === "preview") {
    await answerCallbackQuery(callback.id, "Preview ready");
    await sendMessage(
      privateChatId,
      buildConfigPreview(settings),
      buildManageKeyboard(targetChatId)
    );
    return;
  }

  if (action === "cancel") {
    await clearUserState(userId);
    await answerCallbackQuery(callback.id, "Cancelled");
    await sendMessage(privateChatId, "Edition cancelled.", buildManageKeyboard(targetChatId));
    return;
  }

  await setUserState(userId, targetChatId, action);
  await answerCallbackQuery(callback.id, "Send the new text now.");
  await sendMessage(privateChatId, buildEditPrompt(action, settings));
}

async function handleRaffleJoin(callback) {
  if (!(await isDatabaseAvailable())) {
    await answerCallbackQuery(callback.id, "Database is unavailable right now.");
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
    await answerCallbackQuery(callback.id, "This raffle is not active anymore.");
    return;
  }

  const roundId = round.id;
  const entryResult = await addRaffleEntry(roundId, callback.from);
  const entries = await getRaffleEntries(roundId);
  const settings = await ensureGroupSettings(round.chat_id);
  const messageText = buildRaffleMessage(round.chat_id, settings, entries);
  const replyMarkup = buildRaffleKeyboard(roundId, entries.length);

  if (round.message_id) {
    await editMessageText(round.chat_id, round.message_id, messageText, replyMarkup);
  }

  if (entryResult.duplicate) {
    await answerCallbackQuery(callback.id, "You are already on the raffle list.");
    return;
  }

  await answerCallbackQuery(callback.id, `You are in. Total: ${entries.length}`);
}

async function handlePanelBotCommand(chat, from) {
  if (!(await isGroupAdmin(chat.id, from.id))) {
    return;
  }

  if (!config.botUsername) {
    await sendMessage(
      chat.id,
      "BOT_USERNAME is not configured in Render yet, so the private setup link cannot be generated."
    );
    return;
  }

  const manageUrl = `https://t.me/${config.botUsername}?start=manage_${chat.id}`;
  await sendMessage(
    chat.id,
    "Open the private admin panel of the bot from this button:",
    {
      reply_markup: {
        inline_keyboard: [
          [{ text: "Open bot admin", url: manageUrl }]
        ]
      }
    }
  );
}

async function handleNewRaffle(chat, from) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chat.id, "The raffle database is unavailable right now.");
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const round = await createRaffleRound(chat.id, from.id);
  const entries = await getRaffleEntries(round.id);
  const response = await sendMessage(
    chat.id,
    buildRaffleMessage(chat.id, settings, entries),
    buildRaffleKeyboard(round.id, entries.length)
  );

  if (response.ok && response.result && response.result.message_id) {
    await setRaffleRoundMessage(round.id, response.result.message_id);
  }
}

async function handleDrawWinner(chatId) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, "The raffle database is unavailable right now.");
    return;
  }

  const round = await getActiveRaffleRound(chatId);

  if (!round) {
    await sendMessage(chatId, "There is no active raffle. Use /NSorteo first.");
    return;
  }

  const entries = await getRaffleEntries(round.id);
  if (!entries.length) {
    await sendMessage(chatId, "There are no registered users yet.");
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
    `Se han sorteado (${participantCount}) Usuarios.\n\nEl ganador es:\n<b>${mention}</b>`
  );
}

async function handleResetRaffle(chatId) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, "The raffle database is unavailable right now.");
    return;
  }

  const round = await getActiveRaffleRound(chatId);

  if (!round) {
    await sendMessage(chatId, "There is no active raffle to reset.");
    return;
  }

  await clearRaffleEntries(round.id);
  const settings = await ensureGroupSettings(chatId);
  const entries = await getRaffleEntries(round.id);

  if (round.message_id) {
    await editMessageText(
      chatId,
      round.message_id,
      buildRaffleMessage(chatId, settings, entries),
      buildRaffleKeyboard(round.id, 0)
    );
  }

  await sendMessage(chatId, "The raffle list was reset. Users can register again.");
}

async function handleWarnCommand(chatId, message) {
  if (!(await isDatabaseAvailable())) {
    await sendMessage(chatId, "The configuration database is unavailable right now.");
    return;
  }

  if (!message.reply_to_message || !message.reply_to_message.from) {
    await sendMessage(chatId, "Reply to a user message to send a warning.");
    return;
  }

  const settings = await ensureGroupSettings(chatId);
  const target = message.reply_to_message.from;
  const rendered = renderTemplate(settings.warning_message, {
    first_name: target.first_name || "user",
    username: target.username ? `@${target.username}` : target.first_name || "user",
    group: message.chat.title || "group"
  });

  await sendMessage(chatId, rendered);
}

async function handleStaffCommand(chatId) {
  const admins = await getChatAdministrators(chatId);
  if (!admins.ok || !Array.isArray(admins.result)) {
    await sendMessage(chatId, "I could not read the staff list right now.");
    return;
  }

  const lines = ["<b>Group staff</b>"];

  admins.result.forEach((item) => {
    const user = item.user || {};
    const title = classifyAdmin(item);
    const name = user.username ? `@${user.username}` : [user.first_name, user.last_name].filter(Boolean).join(" ");
    lines.push(`• <b>${escapeHtml(title)}</b>: ${escapeHtml(name || String(user.id || ""))}`);
  });

  await sendMessage(chatId, lines.join("\n"));
}

async function handleWelcomeMessage(chat, newMembers) {
  if (!(await isDatabaseAvailable())) {
    return;
  }

  const settings = await ensureGroupSettings(chat.id, chat.title || "");
  const template = settings.welcome_message || "Bienvenido {first_name} a {group}.";

  for (const user of newMembers) {
    const text = renderTemplate(template, {
      first_name: user.first_name || "user",
      full_name: [user.first_name, user.last_name].filter(Boolean).join(" "),
      username: user.username ? `@${user.username}` : user.first_name || "user",
      group: chat.title || "group"
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

function buildManageKeyboard(chatId) {
  return {
    reply_markup: {
      inline_keyboard: [
        [
          { text: "Edit welcome", callback_data: `cfg:${chatId}:welcome` },
          { text: "Edit warning", callback_data: `cfg:${chatId}:warning` }
        ],
        [
          { text: "Edit rules", callback_data: `cfg:${chatId}:rules` },
          { text: "Edit raffle text", callback_data: `cfg:${chatId}:raffle_intro` }
        ],
        [
          { text: "Preview", callback_data: `cfg:${chatId}:preview` },
          { text: "Cancel", callback_data: `cfg:${chatId}:cancel` }
        ]
      ]
    }
  };
}

function buildPrivateWelcomeText() {
  return [
    "<b>Ideadigital Bot</b>",
    "",
    "This private chat is your bot control center.",
    "Use /panel inside a group or /panelbot in a group where you are admin to receive the private management button."
  ].join("\n");
}

function buildPrivateHelpText() {
  return [
    "<b>Private help</b>",
    "",
    "Use the private management menu to edit:",
    "• Welcome message",
    "• Warning message",
    "• Raffle rules",
    "• Main raffle message"
  ].join("\n");
}

function buildGroupHelpText() {
  return [
    "<b>Main commands</b>",
    "/NSorteo - Start a raffle with a live register button",
    "/Sortear - Pick a random winner",
    "/Reset - Clear all current raffle entries",
    "/Reglas - Show the raffle rules",
    "/Staff - Show founders, admins, and moderators",
    "/Warn - Reply to a user and send a warning",
    "/PanelBot - Open the private configuration flow"
  ].join("\n");
}

function buildRaffleMessage(chatId, settings, entries) {
  const intro = settings.raffle_intro_text || "Participa en nuestro sorteo presionando el botón.";
  const listUrl = buildRaffleListUrl(chatId);
  const listLine = listUrl
    ? `<a href="${escapeHtml(listUrl)}">Ver lista...</a>`
    : "Ver lista...";

  return [
    "<b>Sorteo activo</b>",
    escapeHtml(intro),
    "",
    `<b>Anotados (${entries.length})</b>`,
    listLine,
    "",
    "Usa /Reglas para ver las condiciones."
  ].join("\n");
}

function buildRaffleKeyboard(roundId, count) {
  return {
    reply_markup: {
      inline_keyboard: [
        [{ text: `Anotar ✅ ${String(count).padStart(2, "0")}`, callback_data: `raffle_join:${roundId}` }]
      ]
    }
  };
}

function buildEditPrompt(action, settings) {
  const labels = {
    welcome: "Send the new welcome message.",
    warning: "Send the new warning message.",
    rules: "Send the new raffle rules.",
    raffle_intro: "Send the new text that appears above the raffle button."
  };

  const currentValue = {
    welcome: settings.welcome_message,
    warning: settings.warning_message,
    rules: settings.raffle_rules_text,
    raffle_intro: settings.raffle_intro_text
  };

  return [
    labels[action] || "Send the new content.",
    "",
    "<b>Current value:</b>",
    escapeHtml(currentValue[action] || "(empty)"),
    "",
    "Available placeholders: {first_name}, {full_name}, {username}, {group}"
  ].join("\n");
}

function buildConfigPreview(settings) {
  return [
    `<b>Group:</b> ${escapeHtml(settings.chat_title || String(settings.chat_id))}`,
    "",
    `<b>Welcome:</b>\n${escapeHtml(settings.welcome_message || "")}`,
    "",
    `<b>Warning:</b>\n${escapeHtml(settings.warning_message || "")}`,
    "",
    `<b>Rules:</b>\n${escapeHtml(settings.raffle_rules_text || "")}`,
    "",
    `<b>Raffle text:</b>\n${escapeHtml(settings.raffle_intro_text || "")}`
  ].join("\n");
}

function buildSettingPreview(action, value) {
  return `<b>${escapeHtml(action)}</b>\n${escapeHtml(value || "")}`;
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

function classifyAdmin(item) {
  const status = item.status;
  const customTitle = item.custom_title;

  if (status === "creator") {
    return "Founder";
  }

  if (customTitle) {
    return customTitle;
  }

  return "Administrator";
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
