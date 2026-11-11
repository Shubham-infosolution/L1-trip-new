'use strict';
function integer(name, fallback, min = 1, max = 100000) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}
module.exports = { integer };
