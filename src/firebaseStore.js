const admin = require("firebase-admin");
const config = require("./config");

let firestore = null;

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

function nowIso() {
  return new Date().toISOString();
}

function hasFirebaseConfig() {
  return Boolean(
    config.firebase.projectId &&
      config.firebase.clientEmail &&
      config.firebase.privateKey
  );
}

function getFirestore() {
  if (!hasFirebaseConfig()) {
    return null;
  }

  if (firestore) {
    return firestore;
  }

  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert({
        projectId: config.firebase.projectId,
        clientEmail: config.firebase.clientEmail,
        privateKey: String(config.firebase.privateKey).replace(/\\n/g, "\n")
      })
    });
  }

  firestore = admin.firestore();
  return firestore;
}

async function testConnection() {
  const db = getFirestore();
  if (!db) {
    return { ok: false, message: "Firebase env vars are missing." };
  }

  try {
    await db.collection("_health").doc("ping").set({ updatedAt: nowIso() }, { merge: true });
    return { ok: true, message: "Firestore connection OK." };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

async function ensureSchema() {
  return testConnection();
}

function groupDoc(chatId) {
  return getFirestore().collection("groups").doc(String(chatId));
}

function stateDoc(userId) {
  return getFirestore().collection("userStates").doc(String(userId));
}

function raffleDoc(chatId) {
  return groupDoc(chatId).collection("raffles").doc("active");
}

function raffleEntriesCollection(chatId) {
  return raffleDoc(chatId).collection("entries");
}

async function getGroupSettings(chatId) {
  const db = getFirestore();
  if (!db) {
    return { ...FALLBACK_GROUP_SETTINGS, chat_id: chatId };
  }

  try {
    const snap = await groupDoc(chatId).get();
    if (!snap.exists) {
      return { ...FALLBACK_GROUP_SETTINGS, chat_id: chatId };
    }

    return { ...FALLBACK_GROUP_SETTINGS, ...snap.data(), chat_id: chatId };
  } catch (_error) {
    return { ...FALLBACK_GROUP_SETTINGS, chat_id: chatId };
  }
}

async function ensureGroupSettings(chatId, chatTitle = "") {
  const db = getFirestore();
  if (!db) {
    return { ...FALLBACK_GROUP_SETTINGS, chat_id: chatId, chat_title: chatTitle };
  }

  await groupDoc(chatId).set(
    {
      ...FALLBACK_GROUP_SETTINGS,
      chat_id: chatId,
      chat_title: chatTitle || "",
      updated_at: nowIso()
    },
    { merge: true }
  );

  return getGroupSettings(chatId);
}

async function updateGroupSettings(chatId, patch) {
  const db = getFirestore();
  if (!db) {
    return { ...(await getGroupSettings(chatId)), ...patch };
  }

  await groupDoc(chatId).set(
    {
      ...patch,
      updated_at: nowIso()
    },
    { merge: true }
  );

  return getGroupSettings(chatId);
}

async function getUserState(userId) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  try {
    const snap = await stateDoc(userId).get();
    return snap.exists ? snap.data() : null;
  } catch (_error) {
    return null;
  }
}

async function setUserState(userId, groupChatId, actionKey) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const payload = {
    user_id: userId,
    group_chat_id: groupChatId,
    action_key: actionKey,
    updated_at: nowIso()
  };

  await stateDoc(userId).set(payload, { merge: true });
  return payload;
}

async function clearUserState(userId) {
  const db = getFirestore();
  if (!db) {
    return;
  }

  await stateDoc(userId).delete().catch(() => null);
}

async function createRaffleRound(chatId, createdBy) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  await raffleDoc(chatId).set(
    {
      id: "active",
      chat_id: chatId,
      message_id: null,
      created_by: createdBy,
      status: "active",
      draws_count: 0,
      last_winner_user_id: null,
      last_winner_username: null,
      last_winner_name: null,
      last_winner_at: null,
      created_at: nowIso(),
      updated_at: nowIso()
    },
    { merge: true }
  );

  const docs = await raffleEntriesCollection(chatId).listDocuments();
  await Promise.all(docs.map((doc) => doc.delete()));

  return getActiveRaffleRound(chatId);
}

async function getActiveRaffleRound(chatId) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const snap = await raffleDoc(chatId).get();
  return snap.exists ? snap.data() : null;
}

async function getRaffleRoundById(roundId) {
  if (String(roundId) !== "active") {
    return null;
  }

  return null;
}

async function setRaffleRoundMessage(roundId, messageId) {
  if (String(roundId) !== "active") {
    return null;
  }

  return null;
}

async function getRaffleEntries(roundId) {
  if (String(roundId) !== "active") {
    return [];
  }

  return [];
}

async function addRaffleEntry(roundId, user) {
  if (String(roundId) !== "active") {
    return { inserted: false, duplicate: false };
  }

  return { inserted: false, duplicate: false };
}

async function clearRaffleEntries(roundId) {
  if (String(roundId) !== "active") {
    return;
  }
}

async function saveRaffleWinner(roundId, winner) {
  if (String(roundId) !== "active") {
    return null;
  }

  return null;
}

module.exports = {
  hasFirebaseConfig,
  testConnection,
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
};
