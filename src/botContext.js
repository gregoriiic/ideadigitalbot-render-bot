const { AsyncLocalStorage } = require("async_hooks");

const botStorage = new AsyncLocalStorage();

function runWithBot(bot, callback) {
  return botStorage.run(bot || null, callback);
}

function currentBot() {
  return botStorage.getStore() || null;
}

function currentBotId() {
  return (currentBot() || {}).id || "default";
}

module.exports = {
  runWithBot,
  currentBot,
  currentBotId
};
