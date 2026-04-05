const path = require("path");
require("dotenv").config({ path: path.resolve(process.cwd(), ".env") });

module.exports = {
  port: Number(process.env.PORT || 10000),
  appUrl: (process.env.APP_URL || "").replace(/\/$/, ""),
  panelUrl: (process.env.PANEL_URL || "https://ideadigitalbots.xo.je").replace(/\/$/, ""),
  botUsername: (process.env.BOT_USERNAME || "").replace(/^@/, ""),
  botToken: process.env.BOT_TOKEN || "",
  webhookSecret: process.env.WEBHOOK_SECRET || "",
  db: {
    host: process.env.DB_HOST || "",
    port: Number(process.env.DB_PORT || 3306),
    name: process.env.DB_NAME || "",
    user: process.env.DB_USER || "",
    pass: process.env.DB_PASS || ""
  }
};
