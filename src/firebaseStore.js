const admin = require("firebase-admin");
const config = require("./config");
const { getDefaultGroupSettings, normalizeLocale } = require("./i18n");

let firestore = null;

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

function buildRoundId(chatId) {
  return `${chatId}:active`;
}

function parseRoundId(roundId) {
  const value = String(roundId || "");
  const [chatIdPart, raffleKey] = value.split(":");

  if (!chatIdPart || raffleKey !== "active") {
    return null;
  }

  const chatId = Number(chatIdPart);
  if (!Number.isFinite(chatId)) {
    return null;
  }

  return { chatId, raffleKey };
}

async function getGroupSettings(chatId) {
  const db = getFirestore();
  if (!db) {
    return { ...getDefaultGroupSettings("es"), chat_id: chatId };
  }

  try {
    const snap = await groupDoc(chatId).get();
    if (!snap.exists) {
      return { ...getDefaultGroupSettings("es"), chat_id: chatId };
    }

    const stored = snap.data() || {};
    const locale = normalizeLocale(stored.group_language || "es");
    return {
      ...getDefaultGroupSettings(locale),
      ...stored,
      group_language: locale,
      chat_id: chatId
    };
  } catch (_error) {
    return { ...getDefaultGroupSettings("es"), chat_id: chatId };
  }
}

async function ensureGroupSettings(chatId, chatTitle = "") {
  const db = getFirestore();
  if (!db) {
    return { ...getDefaultGroupSettings("es"), chat_id: chatId, chat_title: chatTitle };
  }

  const existing = await getGroupSettings(chatId);
  const locale = normalizeLocale(existing.group_language || "es");
  const defaults = getDefaultGroupSettings(locale);
  const resolvedTitle = chatTitle || existing.chat_title || "";

  await groupDoc(chatId).set(
    {
      ...defaults,
      ...existing,
      chat_id: chatId,
      chat_title: resolvedTitle,
      group_language: locale,
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

  const existing = await getGroupSettings(chatId);
  const nextLocale = patch.group_language
    ? normalizeLocale(patch.group_language)
    : normalizeLocale(existing.group_language || "es");

  await groupDoc(chatId).set(
    {
      ...patch,
      group_language: nextLocale,
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

  const roundId = buildRoundId(chatId);

  await raffleDoc(chatId).set(
    {
      id: roundId,
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
  return snap.exists ? { ...snap.data(), id: buildRoundId(chatId), chat_id: chatId } : null;
}

async function getRaffleRoundById(roundId) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return null;
  }

  const snap = await raffleDoc(parsed.chatId).get();
  if (!snap.exists) {
    return null;
  }

  return {
    ...snap.data(),
    id: buildRoundId(parsed.chatId),
    chat_id: parsed.chatId
  };
}

async function setRaffleRoundMessage(roundId, messageId) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return null;
  }

  await raffleDoc(parsed.chatId).set(
    {
      message_id: messageId,
      updated_at: nowIso()
    },
    { merge: true }
  );

  return getRaffleRoundById(roundId);
}

async function getRaffleEntries(roundId) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return [];
  }

  const snap = await raffleEntriesCollection(parsed.chatId)
    .orderBy("joined_at", "asc")
    .get();

  return snap.docs.map((doc) => ({
    id: doc.id,
    ...doc.data()
  }));
}

async function addRaffleEntry(roundId, user) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return { inserted: false, duplicate: false };
  }

  const entryRef = raffleEntriesCollection(parsed.chatId).doc(String(user.id));
  const entrySnap = await entryRef.get();

  if (entrySnap.exists) {
    return { inserted: false, duplicate: true };
  }

  await entryRef.set({
    round_id: buildRoundId(parsed.chatId),
    user_id: user.id,
    username: user.username || null,
    first_name: user.first_name || null,
    joined_at: nowIso()
  });

  return { inserted: true, duplicate: false };
}

async function clearRaffleEntries(roundId) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return;
  }

  const snapshot = await raffleEntriesCollection(parsed.chatId).get();
  const batch = getFirestore().batch();

  snapshot.docs.forEach((doc) => {
    batch.delete(doc.ref);
  });

  batch.set(
    raffleDoc(parsed.chatId),
    {
      draws_count: 0,
      last_winner_user_id: null,
      last_winner_username: null,
      last_winner_name: null,
      last_winner_at: null,
      updated_at: nowIso()
    },
    { merge: true }
  );

  await batch.commit();
}

async function saveRaffleWinner(roundId, winner) {
  const parsed = parseRoundId(roundId);
  if (!parsed) {
    return null;
  }

  const round = await getRaffleRoundById(roundId);
  const drawsCount = Number(round && round.draws_count ? round.draws_count : 0) + 1;

  await raffleDoc(parsed.chatId).set(
    {
      draws_count: drawsCount,
      last_winner_user_id: winner.user_id,
      last_winner_username: winner.username || null,
      last_winner_name: winner.first_name || null,
      last_winner_at: nowIso(),
      updated_at: nowIso()
    },
    { merge: true }
  );

  return getRaffleRoundById(roundId);
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
