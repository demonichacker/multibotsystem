const fs = require('fs');
let code = fs.readFileSync('app.js', 'utf8');

// Helper to safely replace without triggering $ regex substitutions
function safeReplace(str, search, replacement) {
    if(typeof search === 'string') {
        const parts = str.split(search);
        return parts.join(replacement);
    } else {
        return str.replace(search, () => replacement);
    }
}

// 1. Add mongoose and BotConfig at the top
code = safeReplace(code,
    "const path = require('path');",
    "const path = require('path');\nconst mongoose = require('mongoose');\n" +
    "const BotConfigSchema = new mongoose.Schema({ name: String, token: { type: String, unique: true }, roomId: String, addedBy: String });\n" +
    "mongoose.models = {}; const BotConfig = mongoose.model('BotConfig', BotConfigSchema);\n" +
    "const GlobalStateSchema = new mongoose.Schema({ data: mongoose.Schema.Types.Mixed });\n" +
    "const GlobalState = mongoose.model('GlobalState', GlobalStateSchema);\n" +
    "const GLOBAL_BOTS = [];"
);

// 2. Modify saveState to dump to MongoDB
code = safeReplace(code,
    "function saveState() {\n\ttry {\n\t\tfs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), 'utf8');\n\t} catch { }\n}",
    "let saveTimeout = null;\nfunction saveState() {\n\ttry {\n\t\tfs.writeFileSync(DATA_FILE, JSON.stringify(state, null, 2), 'utf8');\n\t\tclearTimeout(saveTimeout);\n\t\tsaveTimeout = setTimeout(async () => { try { await GlobalState.updateOne({}, { data: state }, { upsert: true }); } catch(e){} }, 1500);\n\t} catch { }\n}"
);

// 3. Move Express server BEFORE bot instantiation
const expressRegex = /\/\/ --- WEB SERVER & DASHBOARD ---[\s\S]*?\/\/ Minimal event hooks for visibility/m;
const expressMatch = code.match(expressRegex);
if (expressMatch) {
    let expressCode = expressMatch[0];
    expressCode = safeReplace(expressCode,
        "const statusColor = bot?.connected ? '#00ffa3' : '#ff4b4b';\n\tconst statusText = bot?.connected ? 'CONNECTED' : 'DISCONNECTED';",
        "const botRows = GLOBAL_BOTS.map(b => `<div class=\"status-badge\"><div class=\"status-dot\" style=\"background:${b?.connected ? '#00ffa3' : '#ff4b4b'}\"></div>${b.botName || 'Bot'} - ${b.roomId || 'Booting...'}</div>`).join('');"
    );
    expressCode = safeReplace(expressCode,
        "<div class=\"status-badge\">\n\t\t\t\t\t<div class=\"status-dot\"></div>\n\t\t\t\t\t${statusText}\n\t\t\t\t</div>",
        "${botRows || '<div class=\"status-badge\">Starting...</div>'}"
    );

    code = safeReplace(code, expressMatch[0], ""); 
    code = safeReplace(code,
        "// Create bot instance with common intents", 
        expressCode + "\n\nasync function spawnBot({ name: botName, token, roomId }) {\n// Create bot instance with common intents"
    );
}

// 4. Override bot config and push to GLOBAL_BOTS
code = safeReplace(code,
    "settings.reconnect\n);\n",
    "settings.reconnect\n);\nbot.botName = botName;\nGLOBAL_BOTS.push(bot);\n"
);

// 5. Change bot.login
code = safeReplace(code,
    "bot.login(settings.token, settings.room);",
    "bot.login(token, roomId);"
);

// 6. Fix Boot block and close spawnBot block
const bootRegex = /\/\/ Start the bot with Cloud Settle Delay\n\(\s*async \(\) => {[\s\S]*?}\)\(\);/m;
code = safeReplace(code, bootRegex, `// Start the bot with Cloud Settle Delay
	console.log(\`[BOOT] Settle period active. Waiting 5 seconds...\`);
	await new Promise(resolve => setTimeout(resolve, 5000));
	console.log(\`[BOOT] Settle period complete. Logging in...\`);
	bot.login(token, roomId);`);

// 7. Add COMMAND_DISPATCHER new commands
code = safeReplace(code,
    "		send(isDm ? sender.id : null, \"Okay. Go to the new room, tap the Room Name -> 'Share' -> 'Copy Link', and paste the link to me here!\", isDm);\n\t}\n};",
    "		send(isDm ? sender.id : null, \"Okay. Go to the new room, tap the Room Name -> 'Share' -> 'Copy Link', and paste the link to me here!\", isDm);\n\t},\n" +
    "\t'!botadd': async (sender, args, isDm) => {\n\t\tif (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, '❌ Master Only.', isDm);\n\t\tawait new BotConfig({ name: args[1], token: args[2], roomId: args[3], addedBy: sender.actorId || sender.id }).save();\n\t\tspawnBot({ name: args[1], token: args[2], roomId: args[3] });\n\t\tsend(isDm ? sender.id : null, `✅ Booting Bot: ${args[1]}`, isDm);\n\t},\n" +
    "\t'!botlist': async (sender, args, isDm) => {\n\t\tif (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, '❌ Master Only.', isDm);\n\t\tconst all = await BotConfig.find();\n\t\tsend(isDm ? sender.id : null, `Bots:\\n${all.map(b=>b.name).join(', ')}`, isDm);\n\t},\n" +
    "\t'!botdel': async (sender, args, isDm) => {\n\t\tif (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, '❌ Master Only.', isDm);\n\t\tawait BotConfig.deleteOne({ name: new RegExp(`^${args[1]}$`, 'i') });\n\t\tsend(isDm ? sender.id : null, `🗑️ Deleted Bot ${args[1]} from DB!`, isDm);\n\t}\n};\n\n} // End of spawnBot()\n\n" +
    "async function bootstrapMultiBot() {\n" +
    "    try {\n" +
    "        console.log(\"🚀 Starting Monolithic MongoDB Multi-Bot System...\");\n" +
    "        const uri = process.env.MONGODB_URI || \"mongodb+srv://heiszilla_db_user:wXYE76B8jjaVbWOe@cluster0.1oxqb8a.mongodb.net/?appName=Cluster0\";\n" +
    "        await mongoose.connect(uri);\n" +
    "        console.log(\"📦 Connected to MongoDB (Cluster0)\");\n" +
    "        const dbState = await GlobalState.findOne({});\n" +
    "        if (dbState && dbState.data) { Object.assign(state, dbState.data); console.log(\"📥 Restored master state from MongoDB!\"); }\n" +
    "        let bots = await BotConfig.find();\n" +
    "        if (bots.length === 0 && process.env.BOT_TOKEN) {\n" +
    "            console.log(\"🤖 First-time boot: Seeding default bot from .env!\");\n" +
    "            const newBot = new BotConfig({ name: \"Zilla Master\", token: process.env.BOT_TOKEN, roomId: process.env.ROOM_ID });\n" +
    "            await newBot.save(); bots.push(newBot);\n" +
    "        }\n" +
    "        console.log(`🤖 Booting ${bots.length} persistent bots...`);\n" +
    "        for (const b of bots) {\n" +
    "            spawnBot({ name: b.name, token: b.token, roomId: b.roomId });\n" +
    "            await new Promise(r => setTimeout(r, 2000));\n" +
    "        }\n" +
    "    } catch (e) { console.error(\"❌ MongoDB Boot Failed:\", e.message); }\n" +
    "}\n\nbootstrapMultiBot();\n"
);

fs.writeFileSync('app.js', code);
console.log("Refactor Complete!");
