const crypto = require('node:crypto');

function generateUUID(context, ee, next) {
    context.vars.idKey = crypto.randomUUID();
    return next();
}

module.exports = { generateUUID };