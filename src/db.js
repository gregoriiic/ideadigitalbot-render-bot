const mysql = require("mysql2/promise");
const config = require("./config");

let pool;

function hasDbConfig() {
  return Boolean(
    config.db.host &&
      config.db.name &&
      config.db.user
  );
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

async function findLatestRaffleWinner() {
  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }

  try {
    const [rows] = await currentPool.query(
      `SELECT title, winner_display_name, winner_telegram_username, winner_picked_at
       FROM raffles
       WHERE winner_telegram_username IS NOT NULL
       ORDER BY winner_picked_at DESC
       LIMIT 1`
    );

    return rows[0] || null;
  } catch (error) {
    return null;
  }
}

module.exports = {
  getPool,
  testDbConnection,
  findLatestRaffleWinner
};
