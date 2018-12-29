const crypto = require('crypto');
const util = require('util');

const UNMISTAKABLE_CHARS = '23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz';

async function randomId() {
  const random = await util.promisify(crypto.randomBytes)(17);

  const result = [];
  for (const r of random) {
    result.push(UNMISTAKABLE_CHARS[r % UNMISTAKABLE_CHARS.length]);
  }

  return result.join('');
}

module.exports = {
  UNMISTAKABLE_CHARS,
  randomId,
};
