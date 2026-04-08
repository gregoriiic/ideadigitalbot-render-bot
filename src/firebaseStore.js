const admin = require("firebase-admin");
const config = require("./config");
const { getDefaultGroupSettings, normalizeLocale } = require("./i18n");
const { currentBotId } = require("./botContext");

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

function scopedGroupId(chatId) {
  return `${currentBotId()}:${chatId}`;
}

function scopedGroupPrefix() {
  return `${currentBotId()}:`;
}

function groupDoc(chatId) {
  return getFirestore().collection("groups").doc(scopedGroupId(chatId));
}

function stateDoc(userId) {
  return getFirestore().collection("userStates").doc(String(userId));
}

function profileDoc(userId) {
  return getFirestore().collection("userProfiles").doc(String(userId));
}

function raffleDoc(chatId) {
  return groupDoc(chatId).collection("raffles").doc("active");
}

function raffleEntriesCollection(chatId) {
  return raffleDoc(chatId).collection("entries");
}

function ticketsCollection() {
  return getFirestore().collection("tickets");
}

function warningsCollection(chatId) {
  return groupDoc(chatId).collection("warnings");
}

function warningDoc(chatId, userId) {
  return warningsCollection(chatId).doc(String(userId));
}

function botsCollection() {
  return getFirestore().collection("bots");
}

function buildWebhookKey() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

async function listBotsByOwner(ownerKey) {
  const db = getFirestore();
  if (!db || !ownerKey) {
    return [];
  }

  const snap = await botsCollection()
    .where("owner_key", "==", ownerKey)
    .where("status", "==", "active")
    .get();

  return snap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));
}

async function listAllBots() {
  const db = getFirestore();
  if (!db) {
    return [];
  }

  const snap = await botsCollection().get();
  return snap.docs
    .map((doc) => ({ id: doc.id, ...doc.data() }))
    .sort((left, right) => {
      const a = new Date(right.updated_at || right.created_at || 0).getTime();
      const b = new Date(left.updated_at || left.created_at || 0).getTime();
      return a - b;
    });
}

async function registerBot(owner, input) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const username = String(input.bot_username || "").replace(/^@/, "").trim();
  const token = String(input.bot_token || "").trim();
  const name = String(input.bot_name || "").trim();
  const ownerKey = String(owner.owner_key || "").trim();

  if (!ownerKey || !username || !token || !name) {
    return null;
  }

  const existing = await botsCollection()
    .where("owner_key", "==", ownerKey)
    .where("bot_username", "==", username)
    .limit(1)
    .get();

  const ref = existing.empty ? botsCollection().doc() : existing.docs[0].ref;
  const webhookKey = existing.empty
    ? buildWebhookKey()
    : String((existing.docs[0].data() || {}).webhook_key || buildWebhookKey());

  const payload = {
    owner_key: ownerKey,
    owner_name: owner.owner_name || "User",
    owner_telegram_id: owner.owner_telegram_id || null,
    bot_name: name,
    bot_username: username,
    bot_token: token,
    webhook_key: webhookKey,
    status: "active",
    subscription_status: existing.empty
      ? "inactive"
      : String((existing.docs[0].data() || {}).subscription_status || "inactive"),
    premium_activated_at: existing.empty
      ? null
      : ((existing.docs[0].data() || {}).premium_activated_at || null),
    premium_until: existing.empty
      ? null
      : ((existing.docs[0].data() || {}).premium_until || null),
    updated_at: nowIso(),
    created_at: existing.empty ? nowIso() : ((existing.docs[0].data() || {}).created_at || nowIso())
  };

  await ref.set(payload, { merge: true });
  const snap = await ref.get();
  return { id: ref.id, ...snap.data() };
}

async function disconnectBot(ownerKey, botId) {
  const db = getFirestore();
  if (!db || !ownerKey || !botId) {
    return false;
  }

  const ref = botsCollection().doc(String(botId));
  const snap = await ref.get();
  if (!snap.exists) {
    return false;
  }

  const data = snap.data() || {};
  if (String(data.owner_key || "") !== String(ownerKey)) {
    return false;
  }

  await ref.set(
    {
      status: "disconnected",
      disconnected_at: nowIso(),
      updated_at: nowIso()
    },
    { merge: true }
  );

  return true;
}

async function updateBotSubscription(botId, patch) {
  const db = getFirestore();
  if (!db || !botId) {
    return null;
  }

  const ref = botsCollection().doc(String(botId));
  const snap = await ref.get();
  if (!snap.exists) {
    return null;
  }

  await ref.set(
    {
      ...patch,
      updated_at: nowIso()
    },
    { merge: true }
  );

  const updated = await ref.get();
  return updated.exists ? { id: updated.id, ...updated.data() } : null;
}

async function getBotByWebhookKey(webhookKey) {
  const db = getFirestore();
  if (!db || !webhookKey) {
    return null;
  }

  const snap = await botsCollection()
    .where("webhook_key", "==", String(webhookKey))
    .where("status", "==", "active")
    .limit(1)
    .get();

  if (snap.empty) {
    return null;
  }

  return { id: snap.docs[0].id, ...snap.docs[0].data() };
}

async function listGroups() {
  const db = getFirestore();
  if (!db) {
    return [];
  }

  try {
    const prefix = scopedGroupPrefix();
    const groupsRef = db.collection("groups");
    const queries = [
      groupsRef
        .orderBy(admin.firestore.FieldPath.documentId())
        .startAt(prefix)
        .endAt(`${prefix}\uf8ff`)
        .get()
    ];

    if (currentBotId() === "default") {
      queries.push(groupsRef.get());
    }

    const snapshots = await Promise.all(queries);
    const deduped = new Map();

    snapshots.forEach((snapshot) => {
      snapshot.docs.forEach((doc) => {
        const data = doc.data() || {};
        const docId = String(doc.id || "");
        const isScopedDoc = docId.startsWith(prefix);
        const isLegacyDefaultDoc =
          currentBotId() === "default" &&
          docId.indexOf(":") === -1;

        if (!isScopedDoc && !isLegacyDefaultDoc) {
          return;
        }

        const chatId = Number(data.chat_id || (isLegacyDefaultDoc ? docId : ""));
        if (!Number.isFinite(chatId)) {
          return;
        }

        const candidate = {
          chat_id: chatId,
          chat_title: String(data.chat_title || "").trim(),
          group_language: normalizeLocale(data.group_language || "es"),
          updated_at: data.updated_at || null
        };

        const existing = deduped.get(chatId);
        if (!existing) {
          deduped.set(chatId, candidate);
          return;
        }

        const existingTime = new Date(existing.updated_at || 0).getTime();
        const candidateTime = new Date(candidate.updated_at || 0).getTime();

        if (candidateTime >= existingTime) {
          deduped.set(chatId, {
            ...existing,
            ...candidate,
            chat_title: candidate.chat_title || existing.chat_title
          });
        }
      });
    });

    return Array.from(deduped.values()).sort((a, b) => {
      const left = (a.chat_title || "").toLowerCase();
      const right = (b.chat_title || "").toLowerCase();
      return left.localeCompare(right);
    });
  } catch (_error) {
    return [];
  }
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

async function setUserState(userId, groupChatId, actionKey, panelMessageId = null) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const payload = {
    user_id: userId,
    group_chat_id: groupChatId,
    action_key: actionKey,
    panel_message_id: panelMessageId,
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

async function getUserProfile(userId) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  try {
    const snap = await profileDoc(userId).get();
    return snap.exists ? snap.data() : null;
  } catch (_error) {
    return null;
  }
}

async function updateUserProfile(userId, patch) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  await profileDoc(userId).set(
    {
      user_id: userId,
      ...patch,
      updated_at: nowIso()
    },
    { merge: true }
  );

  return getUserProfile(userId);
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

async function createSupportTicket(mainChatId, supportChatId, user, messageText) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const counterRef = db.collection("_meta").doc("ticketCounter");
  const ticketNumber = await db.runTransaction(async (transaction) => {
    const snap = await transaction.get(counterRef);
    const current = snap.exists ? Number((snap.data() || {}).value || 1000) : 1000;
    const next = current + 1;
    transaction.set(counterRef, { value: next, updated_at: nowIso() }, { merge: true });
    return next;
  });

  const ticketRef = ticketsCollection().doc();
  const payload = {
    id: ticketRef.id,
    ticket_number: ticketNumber,
    main_chat_id: mainChatId,
    support_chat_id: supportChatId,
    user_id: user.id,
    username: user.username || null,
    first_name: user.first_name || null,
    message_text: messageText,
    support_message_id: null,
    support_message_ids: [],
    status: "open",
    last_activity_at: nowIso(),
    created_at: nowIso(),
    updated_at: nowIso()
  };

  await ticketRef.set(payload);
  return payload;
}

async function attachSupportTicketMessage(ticketId, supportMessageId, extraMessageIds = []) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const ref = ticketsCollection().doc(String(ticketId));
  const ids = [supportMessageId].concat(extraMessageIds).filter(Boolean);
  await ref.set(
    {
      support_message_id: supportMessageId,
      support_message_ids: admin.firestore.FieldValue.arrayUnion(...ids),
      updated_at: nowIso()
    },
    { merge: true }
  );

  const snap = await ref.get();
  return snap.exists ? snap.data() : null;
}

async function getSupportTicketByReply(supportChatId, supportMessageId) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const snap = await ticketsCollection()
    .where("support_chat_id", "==", supportChatId)
    .where("support_message_ids", "array-contains", supportMessageId)
    .limit(1)
    .get();

  if (snap.empty) {
    return null;
  }

  return snap.docs[0].data();
}

async function getOpenSupportTicketByUser(userId) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const snap = await ticketsCollection()
    .where("user_id", "==", userId)
    .where("status", "==", "open")
    .get();

  if (snap.empty) {
    return null;
  }

  return snap.docs
    .map((doc) => doc.data())
    .sort((left, right) => {
      const a = new Date(left.updated_at || left.created_at || 0).getTime();
      const b = new Date(right.updated_at || right.created_at || 0).getTime();
      return b - a;
    })[0] || null;
}

async function updateSupportTicket(ticketId, patch) {
  const db = getFirestore();
  if (!db) {
    return null;
  }

  const ref = ticketsCollection().doc(String(ticketId));
  await ref.set(
    {
      ...patch,
      updated_at: nowIso()
    },
    { merge: true }
  );

  const snap = await ref.get();
  return snap.exists ? snap.data() : null;
}

async function closeSupportTicket(ticketId, reason = "inactive") {
  return updateSupportTicket(ticketId, {
    status: "closed",
    closed_reason: reason,
    closed_at: nowIso()
  });
}

async function listSupportTicketsByGroup(chatId) {
  const db = getFirestore();
  if (!db || !Number.isFinite(Number(chatId))) {
    return [];
  }

  const numericChatId = Number(chatId);
  const [mainSnap, supportSnap] = await Promise.all([
    ticketsCollection().where("main_chat_id", "==", numericChatId).get(),
    ticketsCollection().where("support_chat_id", "==", numericChatId).get()
  ]);

  const deduped = new Map();
  [mainSnap, supportSnap].forEach((snap) => {
    snap.docs.forEach((doc) => {
      deduped.set(doc.id, { id: doc.id, ...doc.data() });
    });
  });

  return Array.from(deduped.values()).sort((left, right) => {
    const a = new Date(right.updated_at || right.created_at || 0).getTime();
    const b = new Date(left.updated_at || left.created_at || 0).getTime();
    return a - b;
  });
}

async function getUserWarnings(chatId, userId) {
  const db = getFirestore();
  if (!db) {
    return { count: 0 };
  }

  const snap = await warningDoc(chatId, userId).get();
  if (!snap.exists) {
    return { count: 0 };
  }

  return snap.data() || { count: 0 };
}

async function incrementUserWarnings(chatId, user, reason = "manual") {
  const db = getFirestore();
  if (!db || !user || !user.id) {
    return { count: 0 };
  }

  const ref = warningDoc(chatId, user.id);
  const count = await db.runTransaction(async (transaction) => {
    const snap = await transaction.get(ref);
    const current = snap.exists ? Number((snap.data() || {}).count || 0) : 0;
    const next = current + 1;

    transaction.set(ref, {
      user_id: user.id,
      username: user.username || null,
      first_name: user.first_name || null,
      count: next,
      last_reason: reason,
      last_warned_at: nowIso(),
      updated_at: nowIso()
    }, { merge: true });

    return next;
  });

  const snap = await ref.get();
  return snap.exists ? snap.data() : { count };
}

async function resetUserWarnings(chatId, userId) {
  const db = getFirestore();
  if (!db || !userId) {
    return;
  }

  await warningDoc(chatId, userId).delete().catch(() => null);
}

async function listWarningSnapshots(chatId) {
  const db = getFirestore();
  if (!db || !Number.isFinite(Number(chatId))) {
    return [];
  }

  const snap = await warningsCollection(Number(chatId)).get();
  return snap.docs
    .map((doc) => ({ id: doc.id, ...doc.data() }))
    .sort((left, right) => Number(right.count || 0) - Number(left.count || 0));
}

module.exports = {
  hasFirebaseConfig,
  testConnection,
  ensureSchema,
  listBotsByOwner,
  listAllBots,
  registerBot,
  disconnectBot,
  updateBotSubscription,
  getBotByWebhookKey,
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
  listWarningSnapshots
};
