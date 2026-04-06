const mysql = require("mysql2/promise");
const config = require("./config");
const firebaseStore = require("./firebaseStore");

let pool;

const FALLBACK_GROUP_SETTINGS = {
  chat_id: 0,
  chat_title: "",
  welcome_message: "Bienvenido {first_name} a {group}.",
  warning_message: "{first_name}, esta es una advertencia oficial del grupo.",
  raffle_rules_text:
    "Reglas del sorteo:\n1. Una participación por usuario.\n2. Respeta las decisiones de la administración.",
  raffle_intro_text: "Participa en nuestro sorteo presionando el botón de abajo.",
  updated_at: null
};

function hasDbConfig() {
  return Boolean(config.db.host && config.db.name && config.db.user);
}

function useFirebase() {
  return firebaseStore.hasFirebaseConfig();
}

function getPool() {
  if (!hasDbConfig()) {
    return null;
  }

  if (!pool) {
    pool = mysql.createPool({
      host: config.db.host,
      port: config.db.port,
      database: config.db.name,
      user: config.db.user,
      password: config.db.pass,
      waitForConnections: true,
      connectionLimit: 5,
      charset: "utf8mb4"
    });
  }

  return pool;
}

async function testDbConnection() {
  if (useFirebase()) {
    return firebaseStore.testConnection();
  }

  const currentPool = getPool();
  if (!currentPool) {
    return { ok: false, message: "Database env vars are missing." };
  }

  try {
    await currentPool.query("SELECT 1");
    return { ok: true, message: "Database connection OK." };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

async function ensureSchema() {
  if (useFirebase()) {
    return firebaseStore.ensureSchema();
  }

  const currentPool = getPool();
  if (!currentPool) {
    return { ok: false, message: "Database env vars are missing." };
  }

  try {
    await currentPool.query(`
      CREATE TABLE IF NOT EXISTS bot_group_settings (
        chat_id BIGINT PRIMARY KEY,
        chat_title VARCHAR(255) NULL,
        welcome_message TEXT NULL,
        warning_message TEXT NULL,
        raffle_rules_text TEXT NULL,
        raffle_intro_text TEXT NULL,
        updated_at DATETIME NOT NULL
      )
    `);

    await currentPool.query(`
      CREATE TABLE IF NOT EXISTS bot_user_states (
        user_id BIGINT PRIMARY KEY,
        group_chat_id BIGINT NOT NULL,
        action_key VARCHAR(100) NOT NULL,
        updated_at DATETIME NOT NULL
      )
    `);

    await currentPool.query(`
      CREATE TABLE IF NOT EXISTS bot_raffle_rounds (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        chat_id BIGINT NOT NULL,
        message_id BIGINT NULL,
        created_by BIGINT NOT NULL,
        status VARCHAR(40) NOT NULL DEFAULT 'active',
        draws_count INT NOT NULL DEFAULT 0,
        last_winner_user_id BIGINT NULL,
        last_winner_username VARCHAR(255) NULL,
        last_winner_name VARCHAR(255) NULL,
        last_winner_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
      )
    `);

    await currentPool.query(`
      CREATE TABLE IF NOT EXISTS bot_raffle_entries (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        round_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        username VARCHAR(255) NULL,
        first_name VARCHAR(255) NULL,
        joined_at DATETIME NOT NULL,
        UNIQUE KEY uniq_round_user (round_id, user_id)
      )
    `);

    return { ok: true, message: "Schema ready." };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

async function listGroups() {
  if (useFirebase()) {
    return firebaseStore.listGroups();
  }

  const currentPool = getPool();
  if (!currentPool) {
    return [];
  }

  try {
    const [rows] = await currentPool.query(
      "SELECT chat_id, chat_title, updated_at FROM bot_group_settings ORDER BY chat_title ASC"
    );

    return rows.map((row) => ({
      chat_id: Number(row.chat_id),
      chat_title: row.chat_title || "",
      updated_at: row.updated_at || null
    }));
  } catch (_error) {
    return [];
  }
}

function nowSql() {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

async function getGroupSettings(chatId) {
  if (useFirebase()) {
    return firebaseStore.getGroupSettings(chatId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return {
      ...FALLBACK_GROUP_SETTINGS,
      chat_id: chatId
    };
  }

  try {
    const [rows] = await currentPool.query(
      "SELECT * FROM bot_group_settings WHERE chat_id = ? LIMIT 1",
      [chatId]
    );

    if (!rows[0]) {
      return {
        ...FALLBACK_GROUP_SETTINGS,
        chat_id: chatId
      };
    }

    return rows[0];
  } catch (_error) {
    return {
      ...FALLBACK_GROUP_SETTINGS,
      chat_id: chatId
    };
  }
}

async function ensureGroupSettings(chatId, chatTitle = "") {
  if (useFirebase()) {
    return firebaseStore.ensureGroupSettings(chatId, chatTitle);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return {
      ...FALLBACK_GROUP_SETTINGS,
      chat_id: chatId,
      chat_title: chatTitle || ""
    };
  }

  try {
    await currentPool.query(
      `INSERT INTO bot_group_settings (chat_id, chat_title, welcome_message, warning_message, raffle_rules_text, raffle_intro_text, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         chat_title = VALUES(chat_title),
         updated_at = VALUES(updated_at)`,
      [
        chatId,
        chatTitle || null,
        FALLBACK_GROUP_SETTINGS.welcome_message,
        FALLBACK_GROUP_SETTINGS.warning_message,
        FALLBACK_GROUP_SETTINGS.raffle_rules_text,
        FALLBACK_GROUP_SETTINGS.raffle_intro_text,
        nowSql()
      ]
    );

    return getGroupSettings(chatId);
  } catch (_error) {
    return {
      ...FALLBACK_GROUP_SETTINGS,
      chat_id: chatId,
      chat_title: chatTitle || ""
    };
  }
}

async function updateGroupSettings(chatId, patch) {
  if (useFirebase()) {
    return firebaseStore.updateGroupSettings(chatId, patch);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return {
      ...(await getGroupSettings(chatId)),
      ...patch
    };
  }

  try {
    const settings = await ensureGroupSettings(chatId);
    const merged = {
      ...settings,
      ...patch,
      updated_at: nowSql()
    };

    await currentPool.query(
      `UPDATE bot_group_settings
       SET chat_title = ?,
           welcome_message = ?,
           warning_message = ?,
           raffle_rules_text = ?,
           raffle_intro_text = ?,
           updated_at = ?
       WHERE chat_id = ?`,
      [
        merged.chat_title || null,
        merged.welcome_message || null,
        merged.warning_message || null,
        merged.raffle_rules_text || null,
        merged.raffle_intro_text || null,
        merged.updated_at,
        chatId
      ]
    );

    return getGroupSettings(chatId);
  } catch (_error) {
    return {
      ...(await getGroupSettings(chatId)),
      ...patch
    };
  }
}

async function getUserState(userId) {
  if (useFirebase()) {
    return firebaseStore.getUserState(userId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  try {
    const [rows] = await currentPool.query(
      "SELECT * FROM bot_user_states WHERE user_id = ? LIMIT 1",
      [userId]
    );

    return rows[0] || null;
  } catch (_error) {
    return null;
  }
}

async function setUserState(userId, groupChatId, actionKey, panelMessageId = null) {
  if (useFirebase()) {
    return firebaseStore.setUserState(userId, groupChatId, actionKey, panelMessageId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  try {
    await currentPool.query(
      `INSERT INTO bot_user_states (user_id, group_chat_id, action_key, updated_at)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         group_chat_id = VALUES(group_chat_id),
         action_key = VALUES(action_key),
         updated_at = VALUES(updated_at)`,
      [userId, groupChatId, actionKey, nowSql()]
    );

    return getUserState(userId);
  } catch (_error) {
    return null;
  }
}

async function clearUserState(userId) {
  if (useFirebase()) {
    return firebaseStore.clearUserState(userId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return;
  }

  try {
    await currentPool.query("DELETE FROM bot_user_states WHERE user_id = ?", [userId]);
  } catch (_error) {
    return;
  }
}

async function getUserProfile(userId) {
  if (useFirebase()) {
    return firebaseStore.getUserProfile(userId);
  }

  return null;
}

async function updateUserProfile(userId, patch) {
  if (useFirebase()) {
    return firebaseStore.updateUserProfile(userId, patch);
  }

  return null;
}

async function createRaffleRound(chatId, createdBy) {
  if (useFirebase()) {
    return firebaseStore.createRaffleRound(chatId, createdBy);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  await currentPool.query(
    "UPDATE bot_raffle_rounds SET status = 'archived', updated_at = ? WHERE chat_id = ? AND status = 'active'",
    [nowSql(), chatId]
  );

  const [result] = await currentPool.query(
    `INSERT INTO bot_raffle_rounds (chat_id, created_by, status, created_at, updated_at)
     VALUES (?, ?, 'active', ?, ?)`,
    [chatId, createdBy, nowSql(), nowSql()]
  );

  return getRaffleRoundById(result.insertId);
}

async function getActiveRaffleRound(chatId) {
  if (useFirebase()) {
    return firebaseStore.getActiveRaffleRound(chatId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  const [rows] = await currentPool.query(
    "SELECT * FROM bot_raffle_rounds WHERE chat_id = ? AND status = 'active' ORDER BY id DESC LIMIT 1",
    [chatId]
  );

  return rows[0] || null;
}

async function getRaffleRoundById(roundId) {
  if (useFirebase()) {
    return firebaseStore.getRaffleRoundById(roundId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  const [rows] = await currentPool.query(
    "SELECT * FROM bot_raffle_rounds WHERE id = ? LIMIT 1",
    [roundId]
  );

  return rows[0] || null;
}

async function setRaffleRoundMessage(roundId, messageId) {
  if (useFirebase()) {
    return firebaseStore.setRaffleRoundMessage(roundId, messageId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  await currentPool.query(
    "UPDATE bot_raffle_rounds SET message_id = ?, updated_at = ? WHERE id = ?",
    [messageId, nowSql(), roundId]
  );

  return getRaffleRoundById(roundId);
}

async function getRaffleEntries(roundId) {
  if (useFirebase()) {
    return firebaseStore.getRaffleEntries(roundId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return [];
  }

  const [rows] = await currentPool.query(
    "SELECT * FROM bot_raffle_entries WHERE round_id = ? ORDER BY id ASC",
    [roundId]
  );

  return rows;
}

async function addRaffleEntry(roundId, user) {
  if (useFirebase()) {
    return firebaseStore.addRaffleEntry(roundId, user);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return { inserted: false, duplicate: false };
  }

  try {
    await currentPool.query(
      `INSERT INTO bot_raffle_entries (round_id, user_id, username, first_name, joined_at)
       VALUES (?, ?, ?, ?, ?)`,
      [
        roundId,
        user.id,
        user.username || null,
        user.first_name || null,
        nowSql()
      ]
    );

    return { inserted: true, duplicate: false };
  } catch (error) {
    if (error && error.code === "ER_DUP_ENTRY") {
      return { inserted: false, duplicate: true };
    }
    throw error;
  }
}

async function clearRaffleEntries(roundId) {
  if (useFirebase()) {
    return firebaseStore.clearRaffleEntries(roundId);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return;
  }

  await currentPool.query("DELETE FROM bot_raffle_entries WHERE round_id = ?", [roundId]);
  await currentPool.query(
    `UPDATE bot_raffle_rounds
     SET draws_count = 0,
         last_winner_user_id = NULL,
         last_winner_username = NULL,
         last_winner_name = NULL,
         last_winner_at = NULL,
         updated_at = ?
     WHERE id = ?`,
    [nowSql(), roundId]
  );
}

async function saveRaffleWinner(roundId, winner) {
  if (useFirebase()) {
    return firebaseStore.saveRaffleWinner(roundId, winner);
  }

  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  await currentPool.query(
    `UPDATE bot_raffle_rounds
     SET draws_count = draws_count + 1,
         last_winner_user_id = ?,
         last_winner_username = ?,
         last_winner_name = ?,
         last_winner_at = ?,
         updated_at = ?
     WHERE id = ?`,
    [
      winner.user_id,
      winner.username || null,
      winner.first_name || null,
      nowSql(),
      nowSql(),
      roundId
    ]
  );

  return getRaffleRoundById(roundId);
}

async function createSupportTicket(mainChatId, supportChatId, user, messageText) {
  if (useFirebase()) {
    return firebaseStore.createSupportTicket(mainChatId, supportChatId, user, messageText);
  }

  return null;
}

async function attachSupportTicketMessage(ticketId, supportMessageId) {
  if (useFirebase()) {
    return firebaseStore.attachSupportTicketMessage(ticketId, supportMessageId);
  }

  return null;
}

async function getSupportTicketByReply(supportChatId, supportMessageId) {
  if (useFirebase()) {
    return firebaseStore.getSupportTicketByReply(supportChatId, supportMessageId);
  }

  return null;
}

module.exports = {
  getPool,
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
  getSupportTicketByReply
};
