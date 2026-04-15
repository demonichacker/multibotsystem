require('dotenv').config();
const { Highrise, GatewayIntentBits, WebApi } = require('highrise.sdk');
const express = require('express');
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const BotConfigSchema = new mongoose.Schema({ 
    name: String, 
    token: { type: String, unique: true }, 
    roomId: String, // The room the bot is ACTUALLY in currently
    targetRoomId: String, // The room we WANT the bot to go to
    assignedRunnerId: { type: String, default: 'default_runner' }, // Which server owns this bot
    isOnline: { type: Boolean, default: false },
    addedBy: String, 
    ownerConversationId: String, 
    expiresAt: { type: Date, default: () => new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) }, // Default 30 days
    isPermanent: { type: Boolean, default: false },
    state: { type: mongoose.Schema.Types.Mixed, default: {} } 
});
mongoose.models = {}; const BotConfig = mongoose.model('BotConfig', BotConfigSchema);
const GLOBAL_BOTS = [];

// --- ORCHESTRATION CONSTANTS ---
const ROLE = process.env.ROLE || 'MASTER'; // MASTER (Dashboard) or RUNNER (Bots)
const RUNNER_ID = process.env.RUNNER_ID || 'default_runner';
console.log(`[SYSTEM] Starting in ROLE: ${ROLE} (ID: ${RUNNER_ID})`);

// Bot Configuration
const settings = {
	token: process.env.BOT_TOKEN,
	room: process.env.ROOM_ID,
	reconnect: 5
};

// Optional: restrict owner-only commands. Set env var like `BOT_OWNER_IDS="id1,id2"`.
// Or hardcode here for automatic setup
const HARDCODED_OWNER_IDS = [
	'6446fc09d86431b11043cb18'  // ZillaGram (room owner)
];

const PRISON_ROOMID = "69cd50fd435f294f643f66d8";

const OWNER_USER_IDS = (process.env.BOT_OWNER_IDS || HARDCODED_OWNER_IDS.join(','))
	.split(',')
	.map((s) => s.trim())
	.filter(Boolean);

function getDefaultState(initialRoomId = null) {
	return {
		protectedUserIds: [],
		vipUserIds: [],
		fullControllerUserIds: [],
		subscribers: [],
		globalPrison: {},
		waitingForInviteUser: null,
		autoTeleEnabled: false,
		lastGoodRoomId: initialRoomId,
		lastSourceConversationId: null,
		vipPrice: 500,
		vipDurationDays: 30,
		walletTotal: 0,
		vipSubscriptions: {},
		usernameCache: {},
		logs: { bans: [], kicks: [], mutes: [], punches: [], cuts: [], voids: [], freezes: [], unbans: [], slaps: [] },
		rooms: {}
	};
}

/**
 * Helper to get room-specific state safely (must be run within spawnBot)
 */
function getRoomData(roomId, state) {
	const id = roomId || process.env.ROOM_ID || 'default';
	if (!state.rooms[id]) {
		state.rooms[id] = {
			teleportPoints: {},
			spawnPos: null,
			vipSpawnPos: null,
			prison: {}
		};
	}
	return state.rooms[id];
}

const activePrisonTimers = new Map(); // userId -> NodeJS.Timeout

// Dance party runtime state (not persisted) - moved inside spawnBot per-bot isolation

// Fun interaction messages
const RIZZ_MESSAGES = [
	"Are you a rainbow? Because you appear after my storms. 🌈",
	"Do you have a charger? Because my phone is about to die, but you already took my breath away. 🔋",
	"Are you a bank loan? Because you have my interest. 🏦",
	"Is your name Wi-Fi? Because I'm feeling a really strong connection. 📶",
	"If beauty were time, you’d be an eternity. ⏳",
	"Do you have a sunburn or are you always this hot? ☀️",
	"Are you a camera? Because every time I look at you, I smile. 📸",
	"I’m not a genie, but I can make your dreams come true. 🧞‍♂️",
	"Did it hurt? When you fell from heaven? ✨",
	"If I were a cat, I’d spend all 9 lives with you. 🐱",
	"Are you a magician? Because whenever I look at you, everyone else disappears. ✨",
	"Is your name Google? Because you have everything I’m searching for. 🔍",
	"Are you an interior decorator? Because when I saw you, the whole room became beautiful. 🏡",
	"I’m not a photographer, but I can definitely picture us together. 📸",
	"Are you a parking ticket? Because you've got FINE written all over you. 🎫"
];

const ROAST_MESSAGES = [
	"I've seen more talent in a broken vending machine. 🥤",
	"I'd agree with you but then we'd both be wrong. 🤡",
	"You’re the reason shampoo has instructions. 🧴",
	"If I had a face like yours, I’d sue my parents. 👨‍👩‍👧",
	"You bring everyone so much joy... when you leave the room. 🚪",
	"I’m not a proctologist, but I know an asshole when I see one. 🩺",
	"You're like a cloud. When you disappear, it's a beautiful day. ☁️",
	"I'd give you a nasty look but you've already got one. 👺",
	"Your brain is like the 4th of July—all sparks and no light. 🎆",
	"I would roast you, but my mom told me not to burn trash. 🗑️",
	"Mirrors can't talk, and lucky for you they can't laugh either. 🪞",
	"I’m not insulting you, I’m describing you. 💁‍♂️",
	"You're living proof that even the best mistakes can be repeated. 📉",
	"You have a face for radio and a voice for silent movies. 📻",
	"If I wanted to kill myself, I'd climb your ego and jump to your IQ. 🏔️"
];

const AUTOTELE_THRESHOLD = 8.0;

const WELCOME_TEMPLATE =
	"🌟 **Welcome to Exclusive Announcement!** 🌟\n\n" +
	"🎉 **Thank you for subscribing!** 🎉\n\n" +
	"✨ Stay tuned for automatic updates on our exciting events, giveaways, and exclusive invitations. Be the first to know whenever we're hosting something amazing!\n\n" +
	"💬 *Disclaimer: Users who block the bot will be automatically unsubscribed from our notification system.*\n\n" +
	"💖 **Happy connecting and exploring with us!**";

const INVITE_MESSAGE = "✨ Our community offers a friendly and positive atmosphere! Join us!";

// --- WEB SERVER & DASHBOARD ---
const app = express();
const PORT = process.env.PORT || 3000;
const startTime = Date.now();
// Unique ID for this specific running instance (Deployment)
const INSTANCE_ID = `zilla_node_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
console.log(`[SYSTEM] New Instance ID: ${INSTANCE_ID}`);

// --- DATABASE MODELS ---
const SystemLockSchema = new mongoose.Schema({
    cluster: { type: String, default: 'main', unique: true },
    activeInstanceId: String,
    lastHeartbeat: { type: Date, default: Date.now }
});
const SystemLock = mongoose.model('SystemLock', SystemLockSchema);

app.use(express.json());

app.get('/', (req, res) => {
	const uptime = Math.floor((Date.now() - startTime) / 1000);
	const hours = Math.floor(uptime / 3600);
	const minutes = Math.floor((uptime % 3600) / 60);
	const seconds = uptime % 60;

	const botRows = GLOBAL_BOTS.map(b => `<div class="status-badge"><div class="status-dot" style="background:${b?.connected ? '#00ffa3' : '#ff4b4b'}"></div>${b.botName || 'Bot'} - ${b.roomId || 'Booting...'}</div>`).join('');

	res.send(`
		<!DOCTYPE html>
		<html lang="en">
		<head>
			<meta charset="UTF-8">
			<meta name="viewport" content="width=device-width, initial-scale=1.0">
			<title>ZillaBot Dashboard</title>
			<style>
				:root {
					--bg: #0a0a0c;
					--card: #16161a;
					--primary: #7c4dff;
					--text: #ffffff;
					--accent: #00ffa3;
				}
				body {
					margin: 0;
					font-family: 'Inter', system-ui, -apple-system, sans-serif;
					background: var(--bg);
					color: var(--text);
					display: flex;
					align-items: center;
					justify-content: center;
					height: 100vh;
					overflow: hidden;
				}
				.dashboard {
					background: var(--card);
					padding: 3rem;
					border-radius: 24px;
					box-shadow: 0 20px 50px rgba(0,0,0,0.5);
					text-align: center;
					border: 1px solid rgba(255,255,255,0.05);
					backdrop-filter: blur(10px);
					max-width: 400px;
					width: 90%;
				}
				h1 {
					margin: 0 0 0.5rem;
					font-size: 2.5rem;
					background: linear-gradient(45deg, #7c4dff, #00ffa3);
					-webkit-background-clip: text;
					-webkit-text-fill-color: transparent;
					letter-spacing: -1px;
				}
				.status-badge {
					display: inline-flex;
					align-items: center;
					gap: 8px;
					background: rgba(255,255,255,0.03);
					padding: 8px 16px;
					border-radius: 100px;
					font-weight: 600;
					font-size: 0.8rem;
					margin-bottom: 2rem;
					border: 1px solid rgba(255,255,255,0.1);
				}
				.status-dot {
					width: 8px;
					height: 8px;
					background: var(--accent);
					border-radius: 50%;
					box-shadow: 0 0 10px var(--accent);
				}
				.uptime {
					font-size: 1.2rem;
					opacity: 0.8;
					font-variant-numeric: tabular-nums;
				}
				.label {
					font-size: 0.7rem;
					text-transform: uppercase;
					letter-spacing: 2px;
					opacity: 0.4;
					margin-bottom: 0.5rem;
				}
				.grid {
					display: grid;
					grid-template-columns: 1fr 1fr;
					gap: 1rem;
					margin-top: 2rem;
				}
				.stat {
					background: rgba(255,255,255,0.02);
					padding: 1rem;
					border-radius: 16px;
					border: 1px solid rgba(255,255,255,0.05);
				}
			</style>
		</head>
		<body>
			<div class="dashboard">
				<h1>ZillaBot</h1>
				${botRows || '<div class="status-badge">Starting...</div>'}
				<div class="stat">
					<div class="label">Uptime</div>
					<div class="uptime">${hours}h ${minutes}m ${seconds}s</div>
				</div>
				<div class="grid">
					<div class="stat">
						<div class="label">Platform</div>
						<div class="uptime">Render</div>
					</div>
					<div class="stat">
						<div class="label">Port</div>
						<div class="uptime">${PORT}</div>
					</div>
				</div>
			</div>
			<script>
				setTimeout(() => location.reload(), 10000);
			</script>
		</body>
		</html>
	`);
});

app.post('/webhook', (req, res) => {
	console.log('[WEBHOOK] Signal Received:', JSON.stringify(req.body, null, 2));
	res.status(200).send({ status: 'success', message: 'Webhook signal received' });
});

if (ROLE === 'MASTER' || ROLE === 'BOTH') {
    app.listen(PORT, () => console.log(`[SERVER] Dashboard live on port ${PORT}`));
}

// Minimal event hooks for visibility

async function spawnBot(botConfig) {
	const { name: botName, token, roomId } = botConfig;

	// Setup Isolated Per-Bot State Memory
	const state = { ...getDefaultState(roomId), ...(botConfig.state || {}) };

	let saveTimeout = null;
	function saveState() {
		clearTimeout(saveTimeout);
		saveTimeout = setTimeout(async () => {
			try { await BotConfig.updateOne({ token: botConfig.token }, { state: state }); } catch (e) { }
		}, 1500);
	}

	// Create bot instance with common intents
	const bot = new Highrise(
		{
			intents: [
				GatewayIntentBits.Ready,
				GatewayIntentBits.Messages,
				GatewayIntentBits.DirectMessages,
				GatewayIntentBits.Joins,
				GatewayIntentBits.Leaves,
				GatewayIntentBits.Reactions,
				GatewayIntentBits.Emotes,
				GatewayIntentBits.Error,
				GatewayIntentBits.Moderate,
				GatewayIntentBits.Movements,
				GatewayIntentBits.Tips
			],
			cache: true
		},
		settings.reconnect
	);
	bot.botName = botName;
	bot.botConfig = botConfig;
    // Removed redundant GLOBAL_BOTS.push(bot) - Handled by the Runner now


	let bootTime = Date.now();
	bot.on('ready', async (session) => {
		console.log(`[READY] ${botName} connected. Session: ${session?.sessionId || 'unknown'}`);
        bootTime = Date.now(); // Reset boot time on successful connection
		try {
			botUserId = session.user_id;
            const currentRoom = session.room_info?.room_id || bot.roomId;
			roomOwnerId = session.room_info?.owner_id;
			console.log(`[DIAGNOSTIC] ${botName} User ID: ${botUserId}`);
			console.log(`[DIAGNOSTIC] Joined Room ID: ${currentRoom}`);
			console.log(`[DIAGNOSTIC] Room Owner ID: ${roomOwnerId}`);
		} catch (e) {
			console.warn(`[DIAGNOSTIC-ERR] ${botName} failed to parse session:`, e.message);
		}

		// Safety Checkpoint: This room is verified as working!
		state.lastGoodRoomId = bot.roomId;
		saveState();

        // --- ORCHESTRATOR STATUS UPDATE ---
        await BotConfig.updateOne({ token: bot.token }, { isOnline: true });

		// --- SILENT MEMBERSHIP CHECK LOOP ---
        // Prevents "Not in room" errors by waiting until the bot is physically present
		let confirmed = false;
        let attempts = 0;
        while (!confirmed && attempts < 20) {
            try {
                const players = await bot.room.players.fetch().catch(() => null);
                if (players && players.some(([u]) => u.id === botUserId)) {
                    confirmed = true;
                } else {
                    // Diagnostic info
                    const playerCount = players ? players.length : 0;
                    console.log(`[WAIT] ${botName} still not in room (Seen ${playerCount} players). Attempt ${attempts+1}/20...`);
                    await new Promise(r => setTimeout(r, 2000));
                }
            } catch (e) {
                await new Promise(r => setTimeout(r, 2000));
            }
            attempts++;
        }

		const roomState = getRoomData(bot.roomId, state);
		if (roomState.spawnPos && roomState.spawnPos.x !== undefined) {
			setTimeout(async () => {
				try {
					await bot.move.walk(roomState.spawnPos.x, roomState.spawnPos.y, roomState.spawnPos.z, roomState.spawnPos.facing || 'FrontRight');
					console.log(`[INFO] ${botName} successfully walked to default spawnPos`);
				} catch (e) {
					// Only log real failures after membership is confirmed
					if (confirmed) console.error(`[ERROR] ${botName} failed to walk to spawnPos:`, e.message);
				}
			}, 1000);
		}
	});

	const activeBotSetups = {};

	bot.on('chatCreate', async (user, message) => {
		if (user?.id) state.usernameCache[user.id] = user.username;
		console.log(`[CHAT] ${user?.username || user?.id}: ${message}`);
		handleChatCommand(user, message, false);
	});

	bot.on('whisperCreate', async (user, message) => {
		if (user?.id) state.usernameCache[user.id] = user.username;
		console.log(`[DM] ${user?.username || user?.id}: ${message}`);
		if (activeBotSetups[user.id]) {
			return handleBotSetupStep(user.id, user, true, message);
		}
		handleChatCommand(user, message, true);
	});

	bot.on('messageCreate', async (userId, conversation) => {
		try {
			const messages = await bot.inbox.messages.get(conversation.id);
			if (messages && messages.length > 0) {
				const latestMsg = messages[0];
				if (latestMsg.category !== 'text') return; // We only care about text DMs now

				// 1. Invites Handling Pipeline (Via Shared Room Link)
				if (state.waitingForInviteUser === userId) {
					// Improved regex to handle various Highrise room link formats and raw IDs
					const roomLinkMatch = latestMsg.content.match(/\/room[s]?\/([a-fA-F0-9]{24})/i) ||
						latestMsg.content.match(/room_id=([a-fA-F0-9]{24})/i) ||
						latestMsg.content.match(/\b([a-fA-F0-9]{24})\b/);

					if (roomLinkMatch) {
						const newRoomId = roomLinkMatch[1];
						console.log(`[ROOM-TRANSFER] Acquired Target RoomID: ${newRoomId}`);
						state.waitingForInviteUser = null; // Un-arm
						saveState();

						// Save the new room target to the database for this specific bot
						await BotConfig.updateOne({ token: botConfig.token }, { targetRoomId: newRoomId }).catch(console.error);
                        
						try { bot.direct.send(conversation.id, "✅ Destination Recorded! The system will now perform a Safe Transfer (15s cooldown) to ensure no session errors. 🚀"); } catch (e) { }

						return;
					}
				}

				// 1.5 Interactive Bot Setup Wizard Pipeline
				if (activeBotSetups[userId]) {
					const sender = { id: conversation.id, actorId: userId, username: userId };
					await handleBotSetupStep(userId, sender, 'inbox', latestMsg.content);
					return;
				}

				// 2. Command Dispatching over Inbox DMs
				if (latestMsg.content.startsWith('!')) {
					const parts = latestMsg.content.trim().split(/\s+/);
					const cmdName = parts[0].toLowerCase();

					const handler = COMMAND_DISPATCHER[cmdName];
					if (handler) {
						// Use conversation.id as the 'id' for routing the reply back to Inbox
						// But pass the raw 'userId' as 'actorId' for permission checks
						const sender = { id: conversation.id, actorId: userId, username: userId };
						try {
							await handler(sender, parts, 'inbox');
						} catch (e) {
							console.error(`[INBOX CMD ERROR] ${e.message}`);
						}
						return;
					}
				}
			}
		} catch (error) {
			console.error('[INBOX ERROR] payload parsing failed:', error);
		}
	});

	async function handleBotSetupStep(userId, senderObj, isDm, text) {
		if (text.toLowerCase() === '!cancel') {
			delete activeBotSetups[userId];
			return send(isDm ? senderObj.id : null, "❌ Bot setup cancelled!", isDm);
		}
		const session = activeBotSetups[userId];
		const cleanText = text.trim();

		if (session.step === 1) {
			session.name = cleanText;
			session.step = 2;
			send(isDm ? senderObj.id : null, `✅ Name set to ${cleanText}.\nNow, please send the **Bot Token** for this new bot:`, isDm);
		} else if (session.step === 2) {
			session.token = cleanText;
			session.step = 3;
			send(isDm ? senderObj.id : null, `✅ Token received.\nNow, give me the **Room ID** (or full room link) where the bot should spawn:`, isDm);
		} else if (session.step === 3) {
			let roomId = cleanText;
			const match = cleanText.match(/[a-fA-F0-9]{24}/);
			if (match) roomId = match[0];

			session.roomId = roomId;
			session.step = 4;
			send(isDm ? senderObj.id : null, `✅ Room set to ${roomId}.\nFinally, what is the **Highrise Username** (@Name) of the person who bought this bot?`, isDm);
		} else if (session.step === 4) {
			session.customer = cleanText.replace('@', '');
			send(isDm ? senderObj.id : null, `⏳ Looking up exact User ID for @${session.customer}...`, isDm);

			try {
				const profile = await WebApi.player.profile.get(session.customer);
				if (!profile || !profile.user || !profile.user.user_id) {
					return send(isDm ? senderObj.id : null, `❌ Could not find user @${session.customer}. Check spelling and try again (type the name exactly):`, isDm);
				}
				const customerId = profile.user.user_id;

				await new BotConfig({ 
                    name: session.name, 
                    token: session.token, 
                    roomId: session.roomId, 
                    targetRoomId: session.roomId,
                    addedBy: customerId,
                    assignedRunnerId: 'default_runner',
                    expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) // 30 day trial
                }).save();
				
                // Only spawn if we are a RUNNER and this is our job
                if (ROLE === 'RUNNER' && RUNNER_ID === 'default_runner') {
                    spawnBot({ name: session.name, token: session.token, roomId: session.roomId });
                }

				send(isDm ? senderObj.id : null, `🎉 **SUCCESS!** 🎉\nBot **${session.name}** database entry created for room ${session.roomId}!\n**@${session.customer}** gets Permanent Ownership.`, isDm);
				delete activeBotSetups[userId];
			} catch (e) {
				delete activeBotSetups[userId];
				send(isDm ? senderObj.id : null, `❌ Database error during allocation: ${e.message}`, isDm);
			}
		}
	}

	bot.on('playerJoin', async (user) => {
		const username = user?.username || user?.id || 'Guest';
		console.log(`[JOIN] ${username} joined the room (ID: ${user?.id})`);
		// console.log(`[DEBUG] User object:`, JSON.stringify(user));

		// Try to send greeting with small delay
		try {
			// --- PRISON ENFORCEMENT ---
			const prisonRecord = state.globalPrison?.[user.id];
			if (prisonRecord && Date.now() < prisonRecord.until) {
				const remainingMins = Math.ceil((prisonRecord.until - Date.now()) / 60000);
				await send(null, `🛑 @${user.username} is still serving a prison sentence! Sending back to Prison...`);
				await prisonUser(user.id, remainingMins);
				return;
			}

			await new Promise(r => setTimeout(r, 500)); // Brief delay to ensure connection ready
			await bot.message.send(`Welcome ${username}! 👋`);
			console.log(`[GREETING SENT] to ${username}`);
		} catch (err) {
			console.error(`[GREETING ERROR] ${err?.message || String(err)}`);
			console.error(`[GREETING DEBUG] Full error:`, err);
		}
	});

	let followUserId = null;
	let lastRizzIdx = -1;
	let lastRoastIdx = -1;
	// Track last known positions for autotele distance calculation
	const playerLastPositions = new Map(); // per-bot, declared inside spawnBot closure

	bot.on('playerMove', async (user, position) => {
		// 1. Follow logic
		if (followUserId && user.id === followUserId && position.x !== undefined) {
			bot.move.walk(position.x + 0.5, position.y, position.z + 0.5, position.facing);
		}

		// 2. Auto-Teleport logic (Available to EVERYONE, skip for bot itself)
		if (state.autoTeleEnabled && user.id !== bot.userId) {
			const prevPos = playerLastPositions.get(user.id);

			// If we have a previous position and we are not currently teleporting them
			if (prevPos && position.x !== undefined) {
				const dx = position.x - prevPos.x;
				const dy = position.y - prevPos.y;
				const dz = position.z - prevPos.z;
				const distance = Math.sqrt(dx * dx + dy * dy + dz * dz);

				if (distance >= AUTOTELE_THRESHOLD) {
					// Update position record BEFORE teleporting to prevent loop
					playerLastPositions.set(user.id, { x: position.x, y: position.y, z: position.z });

					setTimeout(() => {
						bot.player.teleport(user.id, position.x, position.y, position.z, position.facing);
					}, 200);
					return;
				}
			}
		}

		// Always update the last known position
		playerLastPositions.set(user.id, { x: position.x, y: position.y, z: position.z });
	});

	bot.on('playerTip', async (sender, receiver, item) => {
		// 1. Check if the tip was sent to the bot (using the correct global botUserId)
		if (receiver.id !== botUserId) return;

		if (sender?.id) state.usernameCache[sender.id] = sender.username;

		console.log(`[TIP] ${sender.username} sent ${item.amount} gold to bot.`);

		// 2. Track total gold in bot wallet
		state.walletTotal = (state.walletTotal || 0) + item.amount;
		saveState();

		// 3. Process VIP Subscription
		if (item.amount >= state.vipPrice) {
			const durationMs = state.vipDurationDays * 24 * 60 * 60 * 1000;

			// If they are already a sub, add time! If not, start from now.
			const baseTime = (state.vipSubscriptions[sender.id] && state.vipSubscriptions[sender.id] > Date.now())
				? state.vipSubscriptions[sender.id]
				: Date.now();

			state.vipSubscriptions[sender.id] = baseTime + durationMs;

			// Ensure they are also in the set list (for floor permissions)
			if (!state.vipUserIds.includes(sender.id)) {
				state.vipUserIds.push(sender.id);
			}

			saveState();

			const expiryDate = new Date(state.vipSubscriptions[sender.id]).toLocaleString();
			const msg = `💎 **VIP PURCHASE SUCCESS!** 💎\n\n` +
				`🎉 Thank you for your support, @${sender.username}!\n` +
				`✨ You have been granted VIP status for **${state.vipDurationDays} days**.\n` +
				`📅 Your subscription expires on: **${expiryDate}**.\n\n` +
				`🚀 Enjoy your exclusive floors and features!`;

			await send(sender.id, msg, true); // Send as DM
		} else {
			// Just a friendly receipt for small tips
			const msg = `✨ **TIP RECEIVED!** ✨\n\n` +
				`💰 Thank you for the ${item.amount} gold bars, @${sender.username}!\n` +
				`📉 Note: VIP status requires a tip of **${state.vipPrice}g**. You are ${state.vipPrice - item.amount}g short for the upgrade!`;
			await send(sender.id, msg, true);
		}
	});

	bot.on('playerLeave', (user) => {
		console.log(`[LEAVE] ${user?.username || user?.id}`);
		if (user?.id === followUserId) followUserId = null;
		if (user?.id) stopEmoteForUser(user.id);
	});

	bot.on('error', async (message) => {
		const msg = String(message || '').toLowerCase();
		
		// --- SILENCE TRANSIENT STARTUP ERRORS ---
		// If the error happens in the first 30s of booting, it's likely noise from the SDK handshake
		const isStartup = (Date.now() - bootTime < 30000);
		if (isStartup && (msg.includes('not in room') || msg.includes('not authorized'))) return;

		// --- FIX: Suppress "Target user not in room" logs ---
		if (msg.includes('target user not in room')) return;

		if (msg.includes('multilogin')) {
			console.warn(`[SESSION] ${botName}: Multilogin detected. Clearing session...`);
			try { bot.logout(); } catch(e){} // Force SDK to drop state
		}

		console.error(`[ERROR] ${botName}: ${message}`);
		lastApiError = { message: msg, at: Date.now() };

        // Auto-offline status
        if (msg.includes('not in room') || msg.includes('not authorized')) {
             await BotConfig.updateOne({ token: bot.token }, { isOnline: false });
        }

        // --- FATAL ERROR WATCHDOG ---
        // "Error while reading" often means the stream is dead. 
        // We force a reconnect after 10 seconds to ensure the bot returns.
        if (msg.includes('error while reading') || msg.includes('stream closed')) {
            if (bot.isTerminated) return; // Don't restart if we intentionally shut it down!
            console.warn(`[WATCHDOG] Fatal stream error for ${botName}. Forced reconnect in 10s...`);
            setTimeout(() => {
                if (bot.isTerminated) return; 
                try {
                    console.log(`[WATCHDOG] Attempting recovery login for ${botName}...`);
                    bot.login(token, bot.roomId || roomId);
                } catch(e) { console.error(`[WATCHDOG-ERR]`, e); }
            }, 10000);
        }

		// --- SAFETY REVERT TRIGGER ---
		// If we hit "Room not found" or "Not authorized" and we have a backup room, jump back!
		if (state.lastGoodRoomId && (msg.includes('room not found') || msg.includes('not authorized'))) {
			console.warn(`[SAFETY] Move failed (${msg}). Reverting to: ${state.lastGoodRoomId}`);

			const failedTargetId = bot.roomId;
			const backupId = state.lastGoodRoomId;
			const convoId = state.lastSourceConversationId;

			// 1. Wipe the trap
			state.lastGoodRoomId = null;
			saveState();

			// 2. Alert the owner via the original Inbox thread
			if (convoId) {
				try { bot.direct.send(convoId, `⚠️ Move Failed: ${message}. I couldn't join room ${failedTargetId}. Returning to ${backupId} now!`); } catch (e) { }
			}

			// 3. Patch BotConfig back
			BotConfig.updateOne({ token: botConfig.token }, { roomId: backupId }).catch(console.error);

			// 4. Hot-swap back
			setTimeout(() => {
				botConfig.roomId = backupId;
				bot.roomId = backupId;
				bot.connected = false;
				if (bot.ws) bot.ws.close();
			}, 1000);
		}
	});

	// Capture last API error so command handlers can react to failures.
	let lastApiError = { message: '', at: 0 };
	let botUserId = null; // Cache the bot's user ID
	let roomOwnerId = null; // Cache the room owner's ID

	bot.on('roomModerate', (moderatorId, targetUserId, moderationType, duration) => {
		const action = (moderationType || '').toLowerCase();
		const common = {
			actor: moderatorId || 'unknown',
			target: targetUserId || 'unknown',
			action,
			duration: duration || 0,
			at: Date.now()
		};
		if (action === 'ban') pushLog('bans', common);
		if (action === 'unban') pushLog('unbans', common);
		if (action === 'kick') pushLog('kicks', common);
		if (action === 'mute') pushLog('mutes', common);
		if (action === 'freeze' || action === 'unfreeze') pushLog('freezes', common);
		if (action === 'void') pushLog('voids', common);
		if (action === 'punch') pushLog('punches', common);
		if (action === 'cut') pushLog('cuts', common);
		if (action === 'slap') pushLog('slaps', common);
	});

	// --- GRACEFUL SHUTDOWN ---
	async function shutdown(signal) {
		console.log(`[SHUTDOWN] Received ${signal}. Closing connections...`);
		try {
			saveState();
			bot.logout();
			await new Promise(resolve => setTimeout(resolve, 500));
			console.log('[SHUTDOWN] Bot logged out and state saved.');
			process.exit(0);
		} catch (err) {
			console.error('[SHUTDOWN ERROR]', err.message);
			process.exit(1);
		}
	}

	// --- CONNECTION LIFECYCLE ---
	async function transferToRoom(newRoomId) {
		console.log(`[TRANSFER] ${botName} moving to: ${newRoomId}`);
		try {
			bot.logout();
			botConfig.roomId = newRoomId;
			bot.roomId = newRoomId;
			bot.connected = false;
			
			// Significant wait to allow Highrise to clear the previous session
			console.log(`[TRANSFER] Waiting 10s for session clear...`);
			await new Promise(resolve => setTimeout(resolve, 10000));
			
			bot.login(token, newRoomId);
		} catch (e) {
			console.error(`[TRANSFER ERROR] ${botName}:`, e.message);
		}
	}

	bot.transfer = transferToRoom;
    bot.terminate = async (reason) => {
        console.log(`[TERMINATE] ${botName} reason: ${reason}`);
        saveState();
        try { bot.logout(); } catch(e){}
    };


	// -----------------------
	// Command Router (minimal)
	// -----------------------
	function parseArgs(text) {
		return text.trim().split(/\s+/g);
	}
	function parseTargetRoomId(input) {
		if (!input || typeof input !== 'string') return null;
		const raw = input.trim();
		if (raw.startsWith('http://') || raw.startsWith('https://')) {
			try {
				const url = new URL(raw);
				return url.searchParams.get('id') || null;
			} catch {
				// Some URLs might fail URL parsing; fall back to regex.
			}
		}
		// Try common Highrise share formats.
		const m = raw.match(/[?&]id=([a-fA-F0-9]{24})/);
		if (m?.[1]) return m[1];
		// If user pasted raw room id.
		if (/^[a-fA-F0-9]{24}$/.test(raw)) return raw;
		return null;
	}
	function normalizeUsername(u) {
		if (!u) return '';
		return u.replace(/^@/, '');
	}

	async function resolveUserIdByMention(mention) {
		const username = normalizeUsername(mention);
		try {
			// 1. Prefer room cache with case-insensitive check
			if (bot.room.players.userMap) {
				const cachedUser = [...bot.room.players.userMap.values()]
					.find(u => u.username.toLowerCase() === username.toLowerCase());
				if (cachedUser) return cachedUser.id;
			}

			// 2. Check globalPrison list (Allows releasing users even if they are out of the room!)
			if (state.globalPrison) {
				const prisonerId = Object.keys(state.globalPrison).find(id => {
					const rec = state.globalPrison[id];
					return rec.username && rec.username.toLowerCase() === username.toLowerCase();
				});
				if (prisonerId) return prisonerId;
			}

			// 3. Fallback to fetch by username from SDK (May throw if not in room)
			return await bot.room.players.getId(username);
		} catch {
			// 4. Final attempt: Global SDK lookup (if available in this version)
			try {
				if (bot.user?.id?.get) return await bot.user.id.get(username);
			} catch (e) { }
			return null;
		}
	}

	function isProtected(userId) {
		return state.protectedUserIds.includes(userId);
	}
	function pushLog(key, entry) {
		if (!state.logs) state.logs = {};
		if (!Array.isArray(state.logs[key])) state.logs[key] = [];
		state.logs[key].unshift(entry);
		if (state.logs[key].length > 100) state.logs[key] = state.logs[key].slice(0, 100);
		saveState();
	}
	function formatLogItems(items) {
		if (!items?.length) return 'No logs yet.';
		return items
			.slice(0, 10)
			.map((x, i) => {
				const at = new Date(x.at).toLocaleString();
				return `${i + 1}. ${x.actor} -> ${x.target} (${x.action}) at ${at}`;
			})
			.join('\n');
	}
	const isVip = (userId) => {
		const uid = userId?.actorId || userId?.id || userId; // Handle sender object or raw ID
		// 1. Check manual VIP list
		if (state.vipUserIds?.includes(uid)) return true;

		// 2. Check active subscriptions in state.vipSubscriptions
		const expiry = state.vipSubscriptions[uid];
		if (expiry && Date.now() < expiry) return true;

		// 3. Fallback to full controllers
		return state.fullControllerUserIds?.includes(uid);
	};
	function addProtection(userId) {
		if (!state.protectedUserIds.includes(userId)) {
			state.protectedUserIds.push(userId);
			saveState();
		}
	}
	function removeProtection(userId) {
		const idx = state.protectedUserIds.indexOf(userId);
		if (idx >= 0) {
			state.protectedUserIds.splice(idx, 1);
			saveState();
		}
	}

	function delay(ms) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	async function getRoomPermissions(sender) {
		try {
			const userId = (typeof sender === 'object') ? (sender.actorId || sender.id) : sender;
			if (!userId) return null;

			// 1. Manually grant all permissions to the Room Owner
			if (userId === roomOwnerId) {
				return { moderator: true, designer: true, owner: true };
			}
			// 2. Fetch for others from SDK
			const perms = await bot.player.permissions.get(userId);
			// Room owners should have mod rights
			if (perms && perms.owner) {
				perms.moderator = true;
			}
			return perms;
		} catch (e) {
			console.warn(`Could not get permissions for ${userId}:`, e.message);
			return null;
		}
	}

	async function hasModeratorRights(sender) {
		try {
			const userId = (typeof sender === 'object') ? (sender.actorId || sender.id) : sender;
			if (!userId) return false;

			const perms = await getRoomPermissions(userId);
			// Check for moderator, designer, or owner status
			if (perms?.moderator || perms?.designer || perms?.owner) return true;
			// Check if in full controller list
			if ((state.fullControllerUserIds || []).includes(userId)) return true;
			// Check if owner via env var
			if (OWNER_USER_IDS.includes(userId)) return true;
			return false;
		} catch (e) {
			console.warn(`Error checking moderator rights for ${userId}:`, e.message);
			// Fallback to checking full controller or owner IDs
			if ((state.fullControllerUserIds || []).includes(userId)) return true;
			if (OWNER_USER_IDS.includes(userId)) return true;
			return false;
		}
	}

	async function isOwnerOnly(sender) {
		// Handle both raw ID strings and sender objects
		const userId = (typeof sender === 'object') ? (sender.actorId || sender.id) : sender;

		// Dynamically trust the current room owner
		if (roomOwnerId && userId === roomOwnerId) return true;
		// Also trust IDs in the static owner list
		if (OWNER_USER_IDS.length && OWNER_USER_IDS.includes(userId)) return true;
		// Finally, trust whoever bought/spawned this specific bot instance!
		if (botConfig.addedBy && userId === botConfig.addedBy) return true;

		return false;
	}

	function getCachedUsername(userId) {
		try {
			const name = bot.room.players.cache.username(userId);
			return name || userId;
		} catch {
			return userId;
		}
	}

	async function getUserPosition(userId) {
		try {
			const pos = bot.room.players.cache.position(userId);
			if (pos) return pos;
		} catch { }
		// Fallback (fetches from API)
		const pos = await bot.room.players.getPosition(userId);
		return pos;
	}

	async function canUseTeleportPoint(actorId, point) {
		const scope = point?.scope || ['e'];
		const isV = isVip(actorId);
		const isMod = await hasModeratorRights(actorId);
		if (scope.includes('e')) return true;
		if (scope.includes('m') && isMod) return true;
		if (scope.includes('v') && (isMod || isV)) return true;
		return false;
	}

	async function teleportToSavedPoint(actorId, pointName, isDm, replyToUserId) {
		const roomState = getRoomData(bot.roomId, state);
		const key = (pointName || '').trim();
		const point = roomState.teleportPoints?.[key];
		if (!point) {
			return send(replyToUserId, `Point not found: ${key}`, isDm);
		}
		const ok = await canUseTeleportPoint(actorId, point);
		if (!ok) {
			return send(replyToUserId, `No permission for point: ${key}`, isDm);
		}
		const pos = point.pos;
		if (!pos) return send(replyToUserId, `Point has no position: ${key}`, isDm);

		try {
			// Verify user is in the room using their userId
			if (bot.room.players.cache.username(actorId)) {
				await bot.player.teleport(actorId, pos.x, pos.y, pos.z, pos.facing || 'FrontRight');
				// In room chat, we don't always need a noisy reply, but let's keep it for confirmation for now
				return send(replyToUserId, `Teleported to ${key}.`, isDm);
			}
		} catch (e) {
			if (!e.message.includes('not in room')) console.error(`[TELEPORT ERROR] ${e.message}`);
		}
	}

	function scopeToText(scope) {
		if (!Array.isArray(scope) || scope.length === 0) return 'unknown';
		if (scope.includes('e')) return 'e (everyone)';
		if (scope.includes('m')) return 'm (mods)';
		if (scope.includes('v')) return 'v (vip+mods)';
		return scope.join(',');
	}

	function formatTeleportPointsList() {
		const roomState = getRoomData(bot.roomId, state);
		const points = roomState.teleportPoints || {};
		const entries = Object.entries(points);
		if (!entries.length) return 'No saved points.';
		return entries
			.slice(0, 50)
			.map(([name, point]) => `${name}: ${scopeToText(point.scope)}`)
			.join('\n');
	}

	async function send(target, text, type = false) {
		try {
			// Explicit check for Inbox DMs
			const isPrivate = (type === 'inbox' || type === true);
			const processedText = isPrivate ? `\n\n<color=#FFFF00>${text}</color>\n\n` : text;

			if (type === 'inbox') {
				bot.direct.send(target, processedText);
				return;
			}

			if (type === true && target) {
				bot.whisper.send(target, processedText);
			} else {
				bot.message.send(processedText);
			}
		} catch (e) {
			console.error(`[SEND ERROR] ${e.message}`);
		}
	}

	async function prisonUser(targetUserId, minutes = 10) {
		if (!targetUserId) return;
		if (isProtected(targetUserId)) return;

		// Transfer to the dedicated Prison Room
		try {
			bot.player.transport(targetUserId, PRISON_ROOMID);
		} catch (e) {
			console.error(`[PRISON JUMP FAILED] ${e.message}`);
		}

		const until = Date.now() + minutes * 60 * 1000;
		if (!state.globalPrison) state.globalPrison = {};

		// Store username so we can resolve it even after the user leaves the room
		const username = getCachedUsername(targetUserId) || 'Unknown User';
		state.globalPrison[targetUserId] = { until, username };
		saveState();

		// Clear existing timer if any
		if (activePrisonTimers.has(targetUserId)) {
			clearTimeout(activePrisonTimers.get(targetUserId));
		}
		const timer = setTimeout(() => {
			releaseUser(targetUserId);
		}, minutes * 60 * 1000);
		activePrisonTimers.set(targetUserId, timer);
	}

	async function releaseUser(targetUserId) {
		if (!targetUserId) return;
		if (state.globalPrison) {
			delete state.globalPrison[targetUserId];
		}
		saveState();

		// Since the user is in another room, we simply clear their local record.
		// We don't attempt to teleport them back automatically.
		try {
			// --- FIX: Check if the user is actually in the room before teleporting ---
			const players = await bot.room.players.fetch();
			const player = players.find(([u]) => u.id === targetUserId);
			const botPlayer = players.find(([u]) => u.id === botUserId);
			const botPos = botPlayer?.[1];

			if (player && botPos) {
				await bot.player.teleport(targetUserId, botPos.x, botPos.y, botPos.z, botPos.facing || 'FrontRight');
			}
		} catch (e) {
			// Suppress "Target user not in room" but log other errors
			if (!e.message?.includes('Target user not in room')) {
				console.error(`[RELEASE ERROR] ${e.message}`);
			}
		}
		if (activePrisonTimers.has(targetUserId)) {
			clearTimeout(activePrisonTimers.get(targetUserId));
			activePrisonTimers.delete(targetUserId);
		}
	}

	async function releaseAll() {
		if (!state.globalPrison) return;
		const ids = Object.keys(state.globalPrison);
		for (const uid of ids) {
			await releaseUser(uid);
		}
	}
	async function moveUserToRoom(targetUserId, roomInput) {
		const targetRoomId = parseTargetRoomId(roomInput);
		if (!targetRoomId) return { ok: false, error: 'Invalid room link/id.' };
		if (isProtected(targetUserId)) return { ok: false, error: 'That user is protected.' };
		try {
			const before = lastApiError.at;
			bot.player.transport(targetUserId, targetRoomId);
			await new Promise((r) => setTimeout(r, 1200));
			if (lastApiError.at > before && lastApiError.message.toLowerCase().includes('not authorized')) {
				return { ok: false, error: lastApiError.message };
			}
			return { ok: true, roomId: targetRoomId };
		} catch {
			return { ok: false, error: 'Move failed.' };
		}
	}
	async function moveAllToRoom(roomInput) {
		const targetRoomId = parseTargetRoomId(roomInput);
		if (!targetRoomId) return { moved: 0, skipped: 0, error: 'Invalid room link/id.' };
		let moved = 0;
		let skipped = 0;
		try {
			const users = await bot.room.players.fetch();
			const botId = botUserId;
			for (const row of users) {
				const uid = row?.[0]?.id;
				if (!uid || uid === botId || isProtected(uid)) {
					skipped += 1;
					continue;
				}
				const before = lastApiError.at;
				bot.player.transport(uid, targetRoomId);
				await new Promise((r) => setTimeout(r, 600));
				if (lastApiError.at > before && lastApiError.message.toLowerCase().includes('not authorized')) {
					// Bot has no permissions in the destination room; stop early.
					return { moved, skipped, error: lastApiError.message, roomId: targetRoomId };
				}
				moved += 1;
			}
			return { moved, skipped, roomId: targetRoomId };
		} catch {
			return { moved, skipped, error: 'Moveall failed.' };
		}
	}

	// Re-arm timers on startup (across all rooms)
	for (const roomId in state.rooms) {
		const roomState = state.rooms[roomId];
		if (!roomState.prison) continue;

		for (const [uid, rec] of Object.entries(roomState.prison)) {
			const remaining = Math.max(0, rec.until - Date.now());
			if (remaining > 0) {
				const t = setTimeout(() => releaseUser(uid), remaining);
				activePrisonTimers.set(uid, t);
			} else {
				delete roomState.prison[uid];
				saveState();
			}
		}
	}

	// -----------------------
	// Emote System
	// -----------------------
	const EMOTE_MAP = {
		"rest": "sit-idle-cute",
		"zombie": "idle_zombie",
		"relaxed": "idle_layingdown2",
		"attentive": "idle_layingdown",
		"sleepy": "idle-sleep",
		"sleepyloop": "idle-loop-tired",
		"poutyface": "idle-sad",
		"posh": "idle-posh",
		"taploop": "idle-loop-tapdance",
		"sit": "idle-loop-sitfloor",
		"shy": "emote-shy",
		"bummed": "idle-loop-sad",
		"chillin": "idle-loop-happy",
		"annoyed": "idle-loop-annoyed",
		"aerobics": "idle-loop-aerobics",
		"ponder": "idle-lookup",
		"heropose": "idle-hero",
		"relaxing": "idle-floorsleeping2",
		"cozynap": "idle-floorsleeping",
		"enthused": "idle-enthusiastic",
		"boogieswing": "idle-dance-swinging",
		"feelthebeat": "idle-dance-headbobbing",
		"irritated": "idle-angry",

		"yes": "emote-yes",
		"wave": "emote-wave",
		"tired": "emote-tired",
		"think": "emote-think",
		"theatrical": "emote-theatrical",
		"tapdance": "emote-tapdance",
		"superrun": "emote-superrun",
		"superpunch": "emote-superpunch",
		"sumofight": "emote-sumo",
		"thumbsuck": "emote-suckthumb",
		"splitsdrop": "emote-splitsdrop",
		"snowball": "emote-snowball",
		"snowangel": "emote-snowangel",
		"secrethandshake": "emote-secrethandshake",
		"sad": "emote-sad",
		"ropepull": "emote-ropepull",
		"roll": "emote-roll",
		"rofl": "emote-rofl",
		"robot": "emote-robot",
		"rainbow": "emote-rainbow",
		"proposing": "emote-proposing",
		"peekaboo": "emote-peekaboo",
		"peace": "emote-peace",
		"panic": "emote-panic",
		"no": "emote-no",
		"ninjarun": "emote-ninjarun",
		"nightfever": "emote-nightfever",
		"monsterfail": "emote-monster_fail",
		"model": "emote-model",
		"flirtywave": "emote-lust",
		"levelup": "emote-levelup",
		"amused": "emote-laughing2",
		"laugh": "emote-laughing",
		"kiss": "emote-kiss",
		"superkick": "emote-kicking",
		"jump": "emote-jumpb",
		"judochop": "emote-judochop",
		"jetpack": "emote-jetpack",
		"hugyourself": "emote-hugyourself",
		"hot": "emote-hot",
		"heroentrance": "emote-hero",
		"hello": "emote-hello",
		"headball": "emote-headball",
		"harlemshake": "emote-harlemshake",
		"happy": "emote-happy",
		"handstand": "emote-handstand",
		"greedy": "emote-greedy",
		"graceful": "emote-graceful",
		"moonwalk": "emote-gordonshuffle",
		"ghostfloat": "emote-ghost-idle",
		"gangnamstyle": "emote-gangnam",
		"frolic": "emote-frollicking",
		"faint": "emote-fainting",
		"clumsy": "emote-fail2",
		"fall": "emote-fail1",
		"facepalm": "emote-exasperatedb",
		"exasperated": "emote-exasperated",
		"elbowbump": "emote-elbowbump",
		"disco": "emote-disco",
		"blastoff": "emote-disappear",
		"faintdrop": "emote-deathdrop",
		"collapse": "emote-death2",
		"revival": "emote-death",
		"dab": "emote-dab",
		"curtsy": "emote-curtsy",
		"confusion": "emote-confused",
		"cold": "emote-cold",
		"charging": "emote-charging",
		"bunnyhop": "emote-bunnyhop",
		"bow": "emote-bow",
		"boo": "emote-boo",
		"homerun": "emote-baseball",
		"fallingapart": "emote-apart",

		"thumbsup": "emoji-thumbsup",
		"point": "emoji-there",
		"sneeze": "emoji-sneeze",
		"smirk": "emoji-smirking",
		"sick": "emoji-sick",
		"gasp": "emoji-scared",
		"punch": "emoji-punch",
		"pray": "emoji-pray",
		"stinky": "emoji-poop",
		"naughty": "emoji-naughty",
		"mindblown": "emoji-mind-blown",
		"lying": "emoji-lying",
		"levitate": "emoji-halo",
		"fireball": "emoji-hadoken",
		"giveup": "emoji-give-up",
		"tummyache": "emoji-gagging",
		"flex": "emoji-flex",
		"stunned": "emoji-dizzy",
		"cursing": "emoji-cursing",
		"sob": "emoji-crying",
		"clap": "emoji-clapping",
		"celebrate": "emoji-celebrate",
		"arrogance": "emoji-arrogance",
		"angry": "emoji-angry",

		"voguehands": "dance-voguehands",
		"savage": "dance-tiktok8",
		"dontstartnow": "dance-tiktok2",
		"yogaflow": "dance-spiritual",
		"smoothwalk": "dance-smoothwalk",
		"singleladies": "dance-singleladies",
		"shoppingcart": "dance-shoppingcart",
		"russian": "dance-russian",
		"robotic": "dance-robotic",
		"pennywise": "dance-pennywise",
		"orangejustice": "dance-orangejustice",
		"rockout": "dance-metal",
		"karate": "dance-martial-artist",
		"macarena": "dance-macarena",
		"handsup": "dance-handsup",
		"floss": "dance-floss",
		"duckwalk": "dance-duckwalk",
		"breakdance": "dance-breakdance",
		"kpop": "dance-blackpink",
		"pushups": "dance-aerobics",

		"hyped": "emote-hyped",
		"jinglebell": "dance-jinglebell",
		"nervous": "idle-nervous",
		"toilet": "idle-toilet",
		"attention": "emote-attention",
		"astronaut": "emote-astronaut",
		"dancezombie": "dance-zombie",
		"ghost": "emoji-ghost",
		"hearteyes": "emote-hearteyes",
		"swordfight": "emote-swordfight",
		"timejump": "emote-timejump",
		"worm": "emote-snake",
		"snake": "emote-snake",
		"heartfingers": "emote-heartfingers",
		"heartshape": "emote-heartshape",
		"hug": "emote-hug",
		"eyeroll": "emoji-eyeroll",
		"embarrassed": "emote-embarrassed",
		"float": "emote-float",
		"telekinesis": "emote-telekinesis",
		"sexydance": "dance-sexy",
		"puppet": "emote-puppet",
		"fighteridle": "idle-fighter",
		"penguindance": "dance-pinguin",
		"creepypuppet": "dance-creepypuppet",
		"sleigh": "emote-sleigh",
		"maniac": "emote-maniac",
		"energyball": "emote-energyball",
		"singing": "idle_singing",
		"frog": "emote-frog",
		"superpose": "emote-superpose",
		"cute": "emote-cute",
		"tiktok9": "dance-tiktok9",
		"weird": "dance-weird",
		"tiktok10": "dance-tiktok10",
		"pose7": "emote-pose7",
		"pose8": "emote-pose8",
		"casualdance": "idle-dance-casual",
		"pose1": "emote-pose1",
		"pose3": "emote-pose3",
		"pose5": "emote-pose5",
		"cutey": "emote-cutey",
		"punkguitar": "emote-punkguitar",
		"zombierun": "emote-zombierun",
		"fashionista": "emote-fashionista",
		"gravity": "emote-gravity",
		"icecream": "dance-icecream",
		"wrong": "dance-wrong",
		"uwu": "idle-uwu",
		"tiktok4": "idle-dance-tiktok4",
		"advancedshy": "emote-shy2",
		"anime": "dance-anime",
		"kawaii": "dance-kawai",
		"scritchy": "idle-wild",
		"iceskating": "emote-iceskating",
		"surprisebig": "emote-pose6",
		"celebrationstep": "emote-celebrationstep",
		"creepycute": "emote-creepycute",
		"frustrated": "emote-frustrated",
		"pose10": "emote-pose10",
		"sitrelaxed": "sit-relaxed",
		"laidback": "sit-open",
		"stargazing": "emote-stargaze",
		"slap": "emote-slap",
		"boxer": "emote-boxer",
		"headblowup": "emote-headblowup",
		"kawaiigogo": "emote-kawaiigogo",
		"repose": "emote-repose",
		"tiktok7": "idle-dance-tiktok7",
		"shrink": "emote-shrink",
		"ditzypose": "emote-pose9",
		"teleporting": "emote-teleporting",
		"touch": "dance-touch",
		"airguitar": "idle-guitar",
		"gift": "emote-gift",
		"pushit": "dance-employee",
		"stormgroove": "emote-rainstruck-success",
		"stormmood": "emote-rainstruck-fail",
		"bloomflutter": "emote-bloomify-pose1",
		"bloomcharm": "emote-bloomify-pose2",
		"bloomradiance": "emote-bloomify-pose3",
		"midnightpose": "emote-pose-goth1",
		"chaoscutie": "emote-punkandlaces-pose1",
		"rebeldarling": "emote-punkandlaces-pose2",
		"sweettease": "emote-punkandlaces-pose3"
	};
	const EMOTE_KEYS = Object.keys(EMOTE_MAP);
	const EMOTE_LIST = Object.values(EMOTE_MAP);

	const EMOTE_DURATIONS = {
		'emote-hello': 2000,
		'emote-shy': 2000,
		'emote-sad': 2000,
		'emote-kiss': 2000,
		'emote-laughing': 2500,
		'emote-thoughtful': 3000,
		'emote-lust': 2500,
		'emote-curtsy': 2500,
		'emote-greedy': 2500,
		'emote-flex': 3000,
		'emote-tired': 3000,
		'emote-gag': 3000,
		'emote-thumbsup': 2000,
		'emote-no': 2000,
		'emote-yes': 2000,
		'emote-faint': 3500,
		'emote-sleepy': 3500,
		'emote-hot': 2500,
		'emote-snowangel': 4500,
		'emote-snowball': 2500,
		'emote-confused': 3000,
		'emote-celebrate': 3500,
		'emote-peace': 2500,
		'emote-zombie': 4000,
		'emote-monster': 4000,
		'dance-macarena': 10000,
		'emote-ghost-float': 10000,
		'emote-laidback': 15000,
		'emote-gravity': 9000,
		'emote-teleporting': 3000,
		'dance-handsup': 4500,
		'dance-shoppingcart': 5000,
		'dance-paparazzi': 5000,
		'dance-russian': 5000,
		'dance-voguehands': 4500,
		'dance-weird': 5000,
		'emote-rainstruck-success': 4000,
		'emote-rainstruck-fail': 10000,
		'emote-bloomify-pose1': 3000,
		'emote-bloomify-pose2': 3000,
		'emote-bloomify-pose3': 3000,
		'emote-pose-goth1': 4000,
		'emote-punkandlaces-pose1': 4000,
		'emote-punkandlaces-pose2': 4000,
		'emote-punkandlaces-pose3': 4000
	};

	const cooldowns = new Map(); // per-bot, declared inside spawnBot closure

	/**
	 * Checks if a user is on cooldown for a specific command.
	 * @param {string} userId - The ID of the user.
	 * @param {string} cmd - The command name.
	 * @returns {boolean} - True if on cooldown, false otherwise.
	 */
	function isOnCooldown(userId, cmd) {
		const key = `${userId}-${cmd}`;
		const now = Date.now();
		if (cooldowns.has(key) && now - cooldowns.get(key) < 2000) {
			return true;
		}
		cooldowns.set(key, now);
		return false;
	}

	const activeUserEmotes = new Map(); // userId -> { timer, emoteId, ms }

	function loopEmoteForUser(userId, emoteId, customMs = null) {
		// Clear existing loop if any
		stopEmoteForUser(userId);

		// Determine duration
		const ms = customMs || EMOTE_DURATIONS[emoteId] || 3500;

		// Recursive trigger function
		const run = () => {
			bot.player.emote(userId, emoteId);
			const timer = setTimeout(run, ms);
			activeUserEmotes.set(userId, { timer, emoteId, ms });
		};

		run();
	}

	function stopEmoteForUser(userId) {
		if (activeUserEmotes.has(userId)) {
			const rec = activeUserEmotes.get(userId);
			clearTimeout(rec.timer);
			activeUserEmotes.delete(userId);
			// Optionally send "0" to stop the animation immediately (best-effort)
			bot.player.emote(userId, '0');
		}
	}

	let activeBotDanceTimer = null; // per-bot, declared inside spawnBot closure
	let activeDancePartyTimer = null; // per-bot
	let activeDanceParty = { emoteId: null, userIds: [] }; // per-bot

	async function performBotEmote(emoteId) {
		try {
			if (!botUserId) {
				console.warn('[EMOTE] botUserId not yet cached, skipping emote.');
				return;
			}
			bot.player.emote(botUserId, emoteId);
		} catch (e) {
			console.error('Error performing bot emote:', e);
		}
	}

	function startBotDance() {
		if (activeBotDanceTimer) stopBotDance();
		const emotes = Object.values(EMOTE_MAP);
		let index = 0;
		activeBotDanceTimer = setInterval(() => {
			performBotEmote(emotes[index]);
			index = (index + 1) % emotes.length;
		}, 4000);
	}

	function stopBotDance() {
		if (activeBotDanceTimer) {
			clearInterval(activeBotDanceTimer);
			activeBotDanceTimer = null;
		}
	}

	async function handleChatCommand(sender, text, isDm) {
		if (typeof text !== 'string') return;
		// Skip processing the bot's own messages
		if (sender.username === 'ZillaBOT' || (botUserId && sender.id === botUserId)) return;

		const message = text.trim();
		if (!message.startsWith('!')) {
			// --- Chat Triggers (No Prefix) ---
			const lowerMsg = message.toLowerCase();
			if (lowerMsg === 'stop' || lowerMsg === '0') {
				stopEmoteForUser(sender.id);
				return send(isDm ? sender.id : null, 'Emote loop stopped.', isDm);
			}

			// Smart matching: try exact match, spaceless match, or numeric index
			const spacelessMsg = lowerMsg.replace(/\s+/g, '');
			let emoteId = EMOTE_MAP[lowerMsg] || EMOTE_MAP[spacelessMsg];

			if (!emoteId && /^\d+$/.test(lowerMsg)) {
				const idx = parseInt(lowerMsg);
				if (idx >= 1 && idx <= EMOTE_LIST.length) emoteId = EMOTE_LIST[idx - 1];
				else emoteId = lowerMsg; // Literal ID
			}

			if (emoteId) {
				loopEmoteForUser(sender.id, emoteId);
				return;
			}

			// Handle numeric triggers and points (only in room chat)
			if (!isDm) {
				const parts = parseArgs(message);
				const token = (parts[0] || '').toLowerCase();

				if (token === 'points' || token === 'floors') {
					if (await hasModeratorRights(sender.id)) return send(null, formatTeleportPointsList(), false);
				}

				const roomState = getRoomData(bot.roomId, state);

				if (token === 'vip') {
					const isMod = await hasModeratorRights(sender.id);
					const isV = isVip(sender.id);
					if (isMod || isV) {
						if (!roomState.vipSpawnPos) return send(null, 'VIP spawn not set.', false);
						await bot.player.teleport(sender.id, roomState.vipSpawnPos.x, roomState.vipSpawnPos.y, roomState.vipSpawnPos.z, roomState.vipSpawnPos.facing || 'FrontRight');
						return;
					}
				}

				// Match teleport points (f1, f2, etc)
				// We check both the raw token (f1) and the prefix-stripped token (!f1)
				const cleanToken = token.startsWith('!') ? token.substring(1) : token;

				if (roomState.teleportPoints?.[cleanToken]) {
					return teleportToSavedPoint(sender.id, cleanToken, false, null);
				}

				if (parts.length >= 2) {
					const lastArgOriginal = parts[parts.length - 1];
					const lastArgLower = lastArgOriginal.toLowerCase();

					if (lastArgLower === 'all' || lastArgLower.startsWith('@')) {
						const emoteNameRaw = parts.slice(0, -1).join(' ').toLowerCase();
						const spacelessName = emoteNameRaw.replace(/\s+/g, '');
						let emoteId = EMOTE_MAP[emoteNameRaw] || EMOTE_MAP[spacelessName];

						// Check numeric index for broad triggers
						if (!emoteId && /^\d+$/.test(emoteNameRaw)) {
							const idx = parseInt(emoteNameRaw);
							if (idx >= 1 && idx <= EMOTE_LIST.length) emoteId = EMOTE_LIST[idx - 1];
							else emoteId = emoteNameRaw;
						}

						if (emoteId) {
							if (lastArgLower === 'all') {
								const allowed = (await hasModeratorRights(sender.id)) || isVip(sender.id);
								if (!allowed) return;

								const entries = bot.room.players.cache.get ? bot.room.players.cache.get() : (await bot.room.players.fetch());
								const ids = entries.map(([u]) => u.id).filter(id => id && id !== botUserId);
								for (const uid of ids) bot.player.emote(uid, emoteId);

								// Bot also joins the fun
								await performBotEmote(emoteId);

								return send(null, `Emote "${emoteNameRaw}" performed by everyone.`, false);
							}
							// Targeting a specific user performs it once
							// Use original casing for mentions to help the SDK/lookup
							const targetId = await resolveUserIdByMention(lastArgOriginal);
							if (targetId) bot.player.emote(targetId, emoteId);
							return;
						}
					}
				}
			}
			return;
		}

		// --- Command Dispatcher ---
		const args = parseArgs(message);
		const cmdName = args[0].toLowerCase();

		if (isOnCooldown(sender.id, cmdName)) {
			return send(isDm ? sender.id : null, `Slow down! This command is on cooldown.`, isDm);
		}

		// Dynamic floor parsing (e.g., !setf1, !setf2vip, !setmodf3)
		const floorMatch = cmdName.match(/^!set(vip|mod)?f(\d+)(vip|mod)?$/);
		if (floorMatch) {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			const tag = (floorMatch[1] || floorMatch[3] || '').toLowerCase();
			const isVip = tag === 'vip';
			const isModOnly = tag === 'mod';
			const floorNum = floorMatch[2];
			const floorName = `f${floorNum}`;

			const pos = await getUserPosition(sender.id);
			const roomState = getRoomData(bot.roomId, state);
			roomState.teleportPoints[floorName] = {
				pos: { x: pos.x, y: pos.y, z: pos.z, facing: pos.facing || 'FrontRight' },
				scope: isModOnly ? ['m'] : (isVip ? ['v'] : ['e'])
			};
			saveState();

			let scopeLabel = 'Everyone';
			if (isVip) scopeLabel = 'VIP & Mods';
			if (isModOnly) scopeLabel = 'MODS & Owner ONLY';

			send(isDm ? sender.id : null, `Saved ${scopeLabel} floor: ${floorName}. (Type "${floorName}" to go here!)`, isDm);
			return;
		}

		const handler = COMMAND_DISPATCHER[cmdName];
		if (handler) {
			try {
				await handler(sender, args, isDm);
			} catch (error) {
				console.error(`[ERROR] Command ${cmdName} failed:`, error);
				send(isDm ? sender.id : null, `An error occurred while executing ${cmdName}.`, isDm);
			}
		}
	}

	// Targeted help function for moderation commands
	async function withTarget(sender, args, actionCmd, actionType, isDm, userMentionIndex = 1, durationSeconds = null) {
		const mention = args[userMentionIndex];
		const targetId = mention ? await resolveUserIdByMention(mention) : null;
		if (!targetId) {
			send(isDm ? sender.id : null, 'User not found.', isDm);
			return null;
		}
		if (isProtected(targetId)) {
			send(isDm ? sender.id : null, 'That user is protected.', isDm);
			return null;
		}
		const before = lastApiError.at;
		try {
            // SAFE MODERATION WRAPPER
            // Intercepts SDK stream errors during low-level mod actions
            const safeMod = async (fn) => {
                try { await fn(); return true; } 
                catch (e) { 
                    console.warn(`[SAFE-MOD] Action intercepted error:`, e.message); 
                    return false; 
                }
            };

			if (actionCmd === '!kick') await safeMod(() => bot.player.kick(targetId));
			else if (actionCmd === '!mute') await safeMod(() => bot.player.mute(targetId, durationSeconds));
			else if (actionCmd === '!unmute') await safeMod(() => bot.player.unmute(targetId));
			else if (actionCmd === '!ban') await safeMod(() => bot.player.ban(targetId, durationSeconds));
			else if (actionCmd === '!unban') await safeMod(() => bot.player.unban(targetId));
			else await safeMod(() => bot.player.moderateRoom({ user_id: targetId, moderation_action: actionType, action_length: durationSeconds }));

			await delay(800);
			if (lastApiError.at > before && lastApiError.message.toLowerCase().includes('not authorized')) {
				send(isDm ? sender.id : null, lastApiError.message, isDm);
				return null;
			}
			return targetId;
		} catch (e) {
			console.error(`[MOD] Action ${actionCmd} fatally failed:`, e.message);
			return null;
		}
	}

	const COMMAND_DISPATCHER = {
		'!help': async (sender, args, isDm) => {
			const role = (args[1] || '').toLowerCase();

			if (role === 'owner') {
				if (!(await isOwnerOnly(sender))) return send(sender.id, '❌ This menu is for the **Owner** only.', isDm || true);
				const msgs = [
					"<color=#FFFF00>👑 **OWNER CONTROL CENTER**</color>\n\n" +
					"--- **BOT CONTROL** ---\n" +
					"• !come / !here - Bot walks to you\n" +
					"• !walk x y z - Bot walks to coords\n" +
					"• !follow @user - Bot follows user\n" +
					"• !stay / !stopfollow - Stop following\n" +
					"• !botdance - Bot cycles all emotes\n" +
					"• !stopbot - Stop bot dancing\n" +
					"• !copy @user - Bot copies user's outfit\n" +
					"• !changeroom - Transfer bot to new room\n" +
					"• !autotele on/off - Toggle auto-teleport\n\n" +
					"--- **FLOOR MANAGEMENT** ---\n" +
					"• !setf[num] - Set a public floor\n" +
					"• !setf[num]vip - Set a VIP-only floor\n" +
					"• !setf[num]mod - Set a mod-only floor\n" +
					"• !delfloor [name] - Delete a floor\n" +
					"• !wipefloors - Clear all floors\n" +
					"• !setspawn - Set bot's spawn point\n" +
					"• !setvipspawn - Set VIP spawn point",

					"--- **VIP MANAGEMENT** ---\n" +
					"• !vipadd @user [days] - Grant VIP\n" +
					"• !vipdel @user - Revoke VIP\n" +
					"• !viplist - List all VIPs\n" +
					"• !setvipprice [amount] - VIP cost\n" +
					"• !setvipduration [days] - VIP length\n\n" +
					"--- **WALLET & FINANCE** ---\n" +
					"• !wallet - Check bot's gold balance\n" +
					"• !withdraw [amount] - Bot tips gold to you\n\n" +
					"--- **BROADCASTING** ---\n" +
					"• !broadcast [message] - DM all subs\n" +
					"• !inviteall - Invite all subs to room\n" +
					"• !subalert - Connect Inbox for alerts\n\n" +
					"--- **ROLES** ---\n" +
					"• !fullc @user - Grant full controller"
				];
				for (const msg of msgs) { await send(sender.id, msg, isDm || true); await delay(400); }

			} else if (role === 'master') {
				const uid = sender.actorId || sender.id;
				if (!OWNER_USER_IDS.includes(uid)) return send(sender.id, '❌ This menu is for the **Ultimate Master** only.', isDm || true);
				const msgs = [
					"<color=#FF0000>👑 **MASTER NETWORK CENTER**</color>\n\n" +
					"--- **MULTI-BOT FLEET** ---\n" +
					"• !botadd <name> <token> <room> - Spin up new bot\n" +
					"• !addbot <name> <token> <room> - Same as above\n" +
					"• !botlist - View all connected bots\n" +
					"• !botdel <name> - Terminate and drop a bot\n" +
					"• !alert <msg> - Send a global server alert"
				];
				for (const msg of msgs) { await send(sender.id, msg, isDm || true); await delay(400); }

			} else if (role === 'mod') {
				if (!(await hasModeratorRights(sender))) return send(sender.id, '❌ This menu is for **Staff** only.', isDm || true);
				const msgs = [
					"<color=#FFFF00>🛡️ **MODERATOR SUITE**</color>\n\n" +
					"--- **MODERATION** ---\n" +
					"• !kick @user - Kick a user\n" +
					"• !ban @user [mins] - Ban a user\n" +
					"• !unban @user - Unban a user\n" +
					"• !mute @user [mins] - Mute a user\n" +
					"• !unmute @user - Unmute a user\n" +
					"• !freeze @user - Freeze a user\n" +
					"• !unfreeze @user - Unfreeze a user\n" +
					"• !unfreezeall - Unfreeze everyone\n" +
					"• !void @user - Void a user\n" +
					"• !protect @user - Protect a user\n" +
					"• !unprotect @user - Remove protection\n" +
					"• !protected - List protected users\n\n" +
					"--- **PRISON** ---\n" +
					"• !prison @user [mins] - Send to prison\n" +
					"• !release @user - Release early\n" +
					"• !releaseall - Release all prisoners",

					"--- **TELEPORTATION** ---\n" +
					"• !send @user [floor] - Send to floor\n" +
					"• !sendall [floor] - Send everyone to floor\n" +
					"• !move @user [link] - Move to room\n" +
					"• !moveall [link] - Move everyone to room\n\n" +
					"--- **EMOTES** ---\n" +
					"• [emote_name] all - Play on everyone\n" +
					"• !party @u1 @u2 [emote] - Start party\n" +
					"• !partys - Stop emote party\n\n" +
					"--- **LOGS & INFO** ---\n" +
					"• !bans / !kicks / !mutes / !voids\n" +
					"• !slaps / !punches / !freezes\n" +
					"• !subcounts - Subscriber count\n" +
					"• !floors / points - List all floors"
				];
				for (const msg of msgs) { await send(sender.id, msg, isDm || true); await delay(400); }

			} else if (role === 'vip') {
				const msgs = [
					"<color=#FFFF00>💎 **VIP EXCLUSIVE PERKS**</color>\n\n" +
					"--- **TELEPORTATION** ---\n" +
					"• !tp @user - Teleport to a user\n" +
					"• !summon @user - Pull user to you\n" +
					"• vip - Go to VIP spawn point\n" +
					"• f3, etc. - Access VIP-only floors\n\n" +
					"--- **EMOTES** ---\n" +
					"• [emote_name] all - Play on everyone"
				];
				for (const msg of msgs) { await send(sender.id, msg, isDm || true); await delay(400); }

			} else if (role === 'user') {
				const msgs = [
					"🎮 **USER COMMANDS** (Interaction):\n\n" +
					"--- **INTERACTION** ---\n" +
					"• !punch/!slap/!cut @user : Trigger an action against someone.\n" +
					"• !rizz @user : Send a random pick-up line.\n" +
					"• !roast @user : Burn someone with a random roast.\n" +
					"• !tp @user : Instantly teleport yourself to another player.",

					"--- **SELF EXPRESSION** ---\n" +
					"• !emotes : View the list of all emotes by ID.\n" +
					"• [emote_id] : Type an emote ID to loop that emote.",

					"--- **SUBSCRIPTIONS** (DM ONLY) ---\n" +
					"• !sub : Subscribe to bot announcements and news.\n" +
					"• !unsub : Opt-out of bot messaging anytime."
				];
				for (const msg of msgs) { await send(sender.id, msg, isDm); await delay(400); }

			} else {
				await send(sender.id, "📚 **ZillaBot Comprehensive Help System**\n\nPlease select your command category for more details:\n\n• !help owner\n• !help mod\n• !help vip\n• !help user\n\n*(Reminder: Help is only shown here in Private DMs!)*", isDm);
			}
		},
		'!walk': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			if (args.length < 4) return send(isDm ? sender.id : null, 'Usage: !walk <x> <y> <z> [facing]', isDm);
			const x = parseFloat(args[1]), y = parseFloat(args[2]), z = parseFloat(args[3]);
			if (isNaN(x) || isNaN(y) || isNaN(z)) return send(isDm ? sender.id : null, 'Invalid coords.', isDm);
			bot.move.walk(x, y, z, args[4] || 'FrontLeft');
		},
		'!come': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const players = await bot.room.players.fetch();
			const realId = sender.actorId || sender.id;
			const player = players.find(([u]) => u.id === realId);
			if (player?.[1]) {
				followUserId = null;
				bot.move.walk(player[1].x, player[1].y, player[1].z, player[1].facing);
			} else send(isDm ? sender.id : null, 'Move once first!', isDm);
		},
		'!here': (s, a, d) => COMMAND_DISPATCHER['!come'](s, a, d),
		'!follow': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const target = args[1] || 'me';
			const targetId = (target.toLowerCase() === 'me') ? (sender.actorId || sender.id) : await resolveUserIdByMention(target);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			followUserId = targetId;
			send(isDm ? sender.id : null, `Following ${target}.`, isDm);
		},
		'!stay': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			followUserId = null;
			send(isDm ? sender.id : null, 'Stopped following.', isDm);
		},
		'!stopfollow': (s, a, d) => COMMAND_DISPATCHER['!stay'](s, a, d),
		'!emote': async (sender, args, isDm) => {
			if (!args[1]) return;
			const name = args[1].toLowerCase();
			await performBotEmote(EMOTE_MAP[name] || name);
		},
		'!emotes': async (sender, args, isDm) => {
			for (let i = 0; i < EMOTE_KEYS.length; i += 10) {
				const chunk = EMOTE_KEYS.slice(i, i + 10).map((name, idx) => `${i + idx + 1}. ${name}`);
				// Force type to true (whisper) if it's not already in an inbox DM
				await send(sender.id, `Emotes: ${chunk.join(', ')}`, isDm || true);
				await delay(500);
			}
		},
		'!autotele': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const arg = (args[1] || '').toLowerCase();
			if (arg === 'on') {
				state.autoTeleEnabled = true;
				saveState();
				send(isDm ? sender.id : null, 'Auto-Teleport: ENABLED (Long walks will be skipped).', isDm);
			} else if (arg === 'off') {
				state.autoTeleEnabled = false;
				saveState();
				send(isDm ? sender.id : null, 'Auto-Teleport: DISABLED.', isDm);
			} else {
				send(isDm ? sender.id : null, 'Usage: !autotele on / off', isDm);
			}
		},
		'!wallet': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			// Force raw fetch to bypass SDK's internal caching
			const walletData = await bot.wallet.fetch();
			const gold = (walletData || []).find(item => item.type === 'gold')?.amount || 0;

			send(sender.id, `📈 **REAL BOT WALLET**: ${gold} Gold Bars.`, true);
		},
		'!withdraw': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			const amount = parseInt(args[1]);
			const validBars = [1, 5, 10, 50, 100, 500, 1000, 5000, 10000];

			if (!validBars.includes(amount)) {
				return send(isDm ? sender.id : null, `❌ Invalid bar size! Supported bars: ${validBars.join(', ')}g.`, isDm);
			}

			const currentBalance = await bot.wallet.get.gold.amount();
			if (currentBalance < amount) {
				return send(isDm ? sender.id : null, `❌ Insufficient funds! Bot only has ${currentBalance} gold.`, isDm);
			}

			try {
				await bot.player.tip(sender.id, amount);
				send(isDm ? sender.id : null, `💸 **WITHDRAWAL SUCCESS!** Bot tipped you **${amount}g**.`, isDm);
			} catch (e) {
				send(isDm ? sender.id : null, `⚠️ Withdrawal failed: ${e.message}`, isDm);
			}
		},
		'!setvipprice': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const amount = parseInt(args[1]);
			if (isNaN(amount)) return send(sender.id, 'Usage: !setvipprice <gold_amount>', true);
			state.vipPrice = amount;
			saveState();
			send(sender.id, `🏷️ VIP Price set to: **${amount} Gold Bars**.`, true);
		},
		'!setvipduration': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const days = parseInt(args[1]);
			if (isNaN(days)) return send(sender.id, 'Usage: !setvipduration <days>', true);
			state.vipDurationDays = days;
			saveState();
			send(sender.id, `⏱️ VIP Duration set to: **${days} Days**.`, true);
		},
		'!vipinfo': async (sender, args, isDm) => {
			const msg = `🏢 **VIP CLUB INFORMATION** 💎\n\n` +
				`🚀 **How to Join:**\n` +
				`• Tip the bot exactly **${state.vipPrice} Gold Bars** while in the room!\n\n` +
				`💎 **Benefits:**\n` +
				`• Access to exclusive VIP floors (f3, etc.)\n` +
				`• VIP-only spawn point access\n` +
				`• Status for **${state.vipDurationDays} days** per payment.\n\n` +
				`✨ Tip the bot now to upgrade!`;
			await send(sender.id, msg, isDm || true);
		},
		'!myvip': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			const expiry = state.vipSubscriptions[uid];
			if (!expiry || Date.now() > expiry) {
				if (state.vipUserIds.includes(uid)) {
					return send(sender.id, `💎 You are a **Permanent VIP**!`, isDm || true);
				}
				return send(sender.id, `❌ You don't have an active VIP subscription. Use !vipinfo to join!`, isDm || true);
			}

			const daysLeft = Math.ceil((expiry - Date.now()) / (1000 * 60 * 60 * 24));
			const expiryDate = new Date(expiry).toLocaleString();
			send(sender.id, `💎 **YOUR VIP STATUS**\n✨ Days Left: **${daysLeft}**\n📅 Expires on: **${expiryDate}**`, isDm || true);
		},
		'!viplist': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			const getName = (id) => {
				const cached = state.usernameCache?.[id];
				return cached ? `@${cached}` : `[ID: ${id}]`;
			};

			const manualVips = state.vipUserIds || [];
			const subIds = Object.keys(state.vipSubscriptions).filter(id => state.vipSubscriptions[id] > Date.now());

			// Create a unique set of all VIP IDs to avoid duplicates
			const allVipIds = Array.from(new Set([...manualVips, ...subIds]));

			if (allVipIds.length === 0) {
				return send(sender.id, "❌ No active VIPs found.", true);
			}

			await send(sender.id, "📋 **ACTIVE VIP RECOGNITION** 💎", true);

			let chunks = [];
			for (const id of allVipIds) {
				const pName = getName(id);

				if (manualVips.includes(id)) {
					chunks.push(`• ${pName} - **Lifetime Access**`);
				} else {
					const expiry = new Date(state.vipSubscriptions[id]).toLocaleString();
					chunks.push(`• ${pName} - Expires: ${expiry}`);
				}
			}

			// Split chunks into groups of 5 to stay under the string length limit
			for (let i = 0; i < chunks.length; i += 5) {
				const subPart = chunks.slice(i, i + 5).join('\n');
				await send(sender.id, subPart, true);
				await delay(400);
			}
		},
		'!role': async (sender, args, isDm) => {
			const mention = args[1];
			const targetId = mention ? await resolveUserIdByMention(mention) : sender.id;
			if (!targetId) return send(sender.id, 'User not found.', true);

			const getName = (id) => {
				const cached = state.usernameCache?.[id];
				return cached ? `@${cached}` : `[ID: ${id}]`;
			};

			const perms = await getRoomPermissions(targetId);
			const isV = isVip(targetId);

			const pName = getName(targetId);
			send(sender.id, `👤 **ROLES FOR ${pName}**\n🔧 Moderator: **${!!perms?.moderator}**\n🎨 Designer: **${!!perms?.designer}**\n💎 VIP: **${isV}**`, true);
		},
		'!info': async (sender, args, isDm) => {
			const mention = args[1];
			const targetId = mention ? await resolveUserIdByMention(mention) : sender.id;
			if (!targetId) return send(sender.id, 'User not found.', true);

			// Permissions Check: Only staff/owner can see info for others
			if (targetId !== (sender.actorId || sender.id) && !(await hasModeratorRights(sender))) {
				return send(sender.id, 'No permission to view others.', true);
			}

			const getName = (id) => {
				const cached = state.usernameCache?.[id];
				return cached ? `@${cached}` : `[ID: ${id}]`;
			};

			const perms = await getRoomPermissions(targetId);
			const isV = isVip(targetId);
			const pName = getName(targetId);

			let msg = `📜 **PLAYER INTEL: ${pName}**\n` +
				`🆔 ID: \`${targetId}\`\n` +
				`🔧 Mod: ${!!perms?.moderator} | 💎 VIP: ${isV}\n`;

			const players = await bot.room.players.fetch();
			const targetData = players.find(([u]) => u.id === targetId);
			if (targetData?.[1]) {
				const pos = targetData[1];
				msg += `📍 Position: [${pos.x.toFixed(1)}, ${pos.y.toFixed(1)}, ${pos.z.toFixed(1)}]`;
			}

			send(sender.id, msg, true);
		},
		'!vipadd': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(sender.id, 'Usage: !vipadd @user [days]', true);

			const cleanMention = args[1]?.startsWith('@') ? args[1].substring(1) : args[1];
			const days = parseInt(args[2]);
			if (!isNaN(days)) {
				// Add as subscription
				const ms = days * 24 * 60 * 60 * 1000;
				state.vipSubscriptions[targetId] = Date.now() + ms;
				send(sender.id, `🎖️ Added **@${cleanMention}** as VIP for **${days} days**.`, true);
			} else {
				// Add as permanent
				if (!state.vipUserIds.includes(targetId)) state.vipUserIds.push(targetId);
				send(sender.id, `🎖️ Added **@${cleanMention}** as **Permanent VIP**.`, true);
			}
			saveState();
		},
		'!vipdel': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(sender.id, 'Usage: !vipdel @user', true);

			state.vipUserIds = state.vipUserIds.filter(id => id !== targetId);
			delete state.vipSubscriptions[targetId];
			saveState();

			send(sender.id, `🚫 Revoked VIP status from @${args[1]}.`, true);
		},
		'!botdance': async (sender, args, isDm) => (startBotDance(), send(isDm ? sender.id : null, 'Bot dancing.', isDm)),
		'!stopbot': async (sender, args, isDm) => (stopBotDance(), send(isDm ? sender.id : null, 'Bot stopped.', isDm)),
		'!setdur': async (sender, args, isDm) => {
			const ms = parseInt(args[1]);
			if (isNaN(ms) || ms < 500) return send(isDm ? sender.id : null, 'Usage: !setdur <ms>', isDm);
			const rec = activeUserEmotes.get(sender.id);
			if (!rec) return send(isDm ? sender.id : null, 'No active loop.', isDm);
			loopEmoteForUser(sender.id, rec.emoteId, ms);
		},
		'!copy': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const targetId = (args[1]?.toLowerCase() === 'me' || !args[1]) ? sender.id : await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			const outfit = await bot.player.outfit.get(targetId);
			if (outfit?.length) await bot.outfit.change(outfit);
			send(isDm ? sender.id : null, 'Outfit copied!', isDm);
		},
		'!protect': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			addProtection(targetId);
			send(isDm ? sender.id : null, `Protected ${args[1]}.`, isDm);
		},
		'!unprotect': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			removeProtection(targetId);
			send(isDm ? sender.id : null, `Unprotected ${args[1]}.`, isDm);
		},
		'!prison': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			if (isProtected(targetId)) return send(isDm ? sender.id : null, 'User is protected.', isDm);
			const mins = parseInt(args[2]) || 10;
			await prisonUser(targetId, mins);
			send(isDm ? sender.id : null, `Sent ${args[1]} to prison for ${mins}m.`, isDm);
		},
		'!release': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			if (!args[1]) return send(isDm ? sender.id : null, 'Usage: !release @user', isDm);

			const targetId = await resolveUserIdByMention(args[1]);

			if (targetId && state.globalPrison?.[targetId]) {
				await releaseUser(targetId);
				send(isDm ? sender.id : null, `🌟 Sentence cleared for ${args[1]}. They are now free to re-enter!`, isDm);
			} else {
				send(isDm ? sender.id : null, `User ${args[1]} is not currently in prison.`, isDm);
			}
		},
		'!releaseall': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			await releaseAll();
			send(isDm ? sender.id : null, 'Released everyone.', isDm);
		},
		'!move': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			const result = await moveUserToRoom(targetId, args[2]);
			if (!result.ok) return send(isDm ? sender.id : null, result.error, isDm);
			send(isDm ? sender.id : null, `Moved ${args[1]} to ${result.roomId}.`, isDm);
		},
		'!moveall': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const result = await moveAllToRoom(args[1]);
			send(isDm ? sender.id : null, `Moved ${result.moved}. Skipped ${result.skipped}.`, isDm);
		},
		'!tp': async (sender, args, isDm) => {
			const isVip = (state.vipUserIds || []).includes(sender.id);
			if (!(await hasModeratorRights(sender.id)) && !isVip) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			const pos = await getUserPosition(targetId);
			await bot.player.teleport(sender.id, pos.x, pos.y, pos.z, pos.facing || 'FrontRight');
		},
		'!summon': async (sender, args, isDm) => {
			const isVip = (state.vipUserIds || []).includes(sender.id);
			if (!(await hasModeratorRights(sender.id)) && !isVip) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);
			const pos = await getUserPosition(sender.id);
			await bot.player.teleport(targetId, pos.x, pos.y, pos.z, pos.facing || 'FrontRight');
		},

		'!delfloor': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const roomState = getRoomData(bot.roomId, state);
			if (roomState.teleportPoints?.[args[1]]) {
				delete roomState.teleportPoints[args[1]];
				saveState();
				send(isDm ? sender.id : null, `Deleted floor ${args[1]}.`, isDm);
			} else {
				send(isDm ? sender.id : null, `Floor ${args[1]} not found.`, isDm);
			}
		},
		'!wipefloors': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const roomState = getRoomData(bot.roomId, state);
			roomState.teleportPoints = {};
			saveState();
			send(isDm ? sender.id : null, 'Wiped all custom floors for this room.', isDm);
		},
		'!floors': async (sender, args, isDm) => {
			send(isDm ? sender.id : null, formatTeleportPointsList(), isDm);
		},
		'!send': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			const roomState = getRoomData(bot.roomId, state);
			const point = roomState.teleportPoints?.[args[2]];
			if (targetId && point) await bot.player.teleport(targetId, point.pos.x, point.pos.y, point.pos.z, point.pos.facing || 'FrontRight');
		},
		'!sendall': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const roomState = getRoomData(bot.roomId, state);
			const point = roomState.teleportPoints?.[args[1]];
			if (!point) return;
			const entries = bot.room.players.cache.get ? bot.room.players.cache.get() : (await bot.room.players.fetch());
			for (const [u] of entries) {
				if (u.id && u.id !== botUserId) await bot.player.teleport(u.id, point.pos.x, point.pos.y, point.pos.z, point.pos.facing || 'FrontRight');
			}
			send(isDm ? sender.id : null, 'Sent everyone to point.', isDm);
		},
		'!party': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const emoteId = args[args.length - 1];
			const mentions = args.slice(1, -1);
			const ids = [];
			for (const m of mentions) {
				const uid = await resolveUserIdByMention(m);
				if (uid && uid !== botUserId) ids.push(uid);
			}
			if (!ids.length) return;
			if (activeDancePartyTimer) clearInterval(activeDancePartyTimer);
			activeDanceParty = { emoteId: String(emoteId), userIds: [...new Set(ids)] };
			activeDancePartyTimer = setInterval(() => {
				for (const uid of activeDanceParty.userIds) bot.player.emote(uid, activeDanceParty.emoteId);
			}, 2500);
			send(isDm ? sender.id : null, `Party started!`, isDm);
		},
		'!partys': async (sender, args, isDm) => {
			if (activeDancePartyTimer) {
				clearInterval(activeDancePartyTimer);
				activeDancePartyTimer = null;
				send(isDm ? sender.id : null, 'Party stopped.', isDm);
			}
		},
		'!protected': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const list = (state.protectedUserIds || []).map(getCachedUsername).join(', ');
			send(isDm ? sender.id : null, list ? `Protected: ${list}` : 'None.', isDm);
		},
		'!sub': async (sender, args, isDm) => {
			if (isDm !== 'inbox') return send(sender.id, 'Please use !sub in a Private DM to subscribe!', isDm);
			if (!state.subscribers) state.subscribers = [];
			if (state.subscribers.includes(sender.id)) return send(sender.id, "You're already subscribed!", isDm);

			state.subscribers.push(sender.id);
			saveState();
			send(sender.id, WELCOME_TEMPLATE, isDm);
		},
		'!subalert': async (sender, args, isDm) => {
			if (isDm !== 'inbox') return send(sender.id, '❌ Please use !subalert in a Private DM to connect your channel!', isDm);

			const uid = sender.actorId || sender.id;
			if (botConfig.addedBy !== uid) return send(sender.id, "❌ Only the officially registered Bot Owner can link their offline inbox!", isDm);

			botConfig.ownerConversationId = sender.id;
			try { await BotConfig.updateOne({ token: botConfig.token }, { ownerConversationId: sender.id }); } catch (e) { }

			send(sender.id, "✅ **OFFLINE CHANNEL LINKED!** ✅\nYou are now confirmed and will receive Master `!alert` notifications directly to this Inbox!", isDm);
		},
		'!unsub': async (sender, args, isDm) => {
			if (isDm !== 'inbox') return send(sender.id, 'Please use !unsub in a Private DM.', isDm);
			if (!state.subscribers) state.subscribers = [];

			const idx = state.subscribers.indexOf(sender.id);
			if (idx === -1) return send(sender.id, "You are not currently subscribed.", isDm);

			state.subscribers.splice(idx, 1);
			saveState();
			send(sender.id, "Successfully unsubscribed. You will no longer receive inbox updates.", isDm);
		},
		'!broadcast': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const text = args.slice(1).join(' ');
			if (!text) return send(isDm ? sender.id : null, 'Usage: !broadcast <message>', isDm);

			const subs = state.subscribers || [];
			if (!subs.length) return send(isDm ? sender.id : null, 'No subscribers found.', isDm);

			let count = 0;
			for (const subId of subs) {
				try {
					await bot.direct.send(subId, `📢 **EXCLUSIVE ANNOUNCEMENT**\n\n${text}`);
					count++;
					await delay(1000); // Prevent rate limits
				} catch (e) {
					console.warn(`Failed to message sub ${subId}: ${e.message}`);
				}
			}
			send(isDm ? sender.id : null, `Broadcast sent to ${count} users.`, isDm);
		},
		'!subcounts': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const subs = state.subscribers || [];
			send(isDm ? sender.id : null, `Total subscribers: ${subs.length}`, isDm);
		},
		'!inviteall': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			const subs = state.subscribers || [];
			if (!subs.length) return send(isDm ? sender.id : null, 'No subscribers found.', isDm);

			let count = 0;
			for (const subId of subs) {
				try {
					// 1. Send the invitation message as a standard DM
					await bot.direct.send(subId, INVITE_MESSAGE);
					// 2. Send the official Room Invite widget using the dedicated SDK method
					await bot.invite.send(subId, bot.roomId);
					count++;
					await delay(1000); // Prevent rate limits
				} catch (e) {
					console.warn(`Failed to invite sub ${subId}: ${e.message}`);
				}
			}
			send(isDm ? sender.id : null, `Invites sent to ${count} subscribers.`, isDm);
		},
		'!kick': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!kick', 'kick', isDm);
			if (ok) send(isDm ? sender.id : null, `Kicked ${args[1]}.`, isDm);
		},
		'!mute': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const mins = parseInt(args[2]) || 60;
			const ok = await withTarget(sender, args, '!mute', 'mute', isDm, 1, mins * 60);
			if (ok) send(isDm ? sender.id : null, `Muted ${args[1]} for ${mins}m.`, isDm);
		},
		'!unmute': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!unmute', 'unmute', isDm);
			if (ok) send(isDm ? sender.id : null, `Unmuted ${args[1]}.`, isDm);
		},
		'!ban': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const mins = parseInt(args[2]) || 60;
			const ok = await withTarget(sender, args, '!ban', 'ban', isDm, 1, mins * 60);
			if (ok) send(isDm ? sender.id : null, `Banned ${args[1]} for ${mins}m.`, isDm);
		},
		'!unban': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!unban', 'unban', isDm);
			if (ok) send(isDm ? sender.id : null, `Unbanned ${args[1]}.`, isDm);
		},
		'!freeze': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!freeze', 'freeze', isDm);
			if (ok) send(isDm ? sender.id : null, `Froze ${args[1]}.`, isDm);
		},
		'!unfreeze': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!unfreeze', 'unfreeze', isDm);
			if (ok) send(isDm ? sender.id : null, `Unfroze ${args[1]}.`, isDm);
		},
		'!unfreezeall': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const entries = bot.room.players.cache.get ? bot.room.players.cache.get() : (await bot.room.players.fetch());
			let count = 0;
			for (const [u] of entries) {
				if (u.id && u.id !== botUserId && !isProtected(u.id)) {
					await bot.player.moderateRoom({ user_id: u.id, moderation_action: 'unfreeze' });
					count++;
					await delay(300);
				}
			}
			send(isDm ? sender.id : null, `Unfroze ${count} users.`, isDm);
		},
		'!void': async (sender, args, isDm) => {
			if (!(await hasModeratorRights(sender.id))) return send(isDm ? sender.id : null, 'No permission.', isDm);
			const ok = await withTarget(sender, args, '!void', 'void', isDm);
			if (ok) send(isDm ? sender.id : null, `Voided ${args[1]}.`, isDm);
		},
		'!addtime': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const days = parseInt(args[1]);
            const botName = args.slice(2).join(' ');
			if (isNaN(days) || !botName) return send(isDm ? sender.id : null, 'Usage: !addtime <days> <target_bot_name>', isDm);

			const botDoc = await BotConfig.findOne({ name: new RegExp(`^${botName}$`, 'i') });
			if (!botDoc) return send(isDm ? sender.id : null, `Bot "${botName}" not found.`, isDm);

            const currentExpiry = botDoc.expiresAt || new Date();
            const newExpiry = new Date(currentExpiry.getTime() + days * 24 * 60 * 60 * 1000);
            
			await BotConfig.updateOne({ _id: botDoc._id }, { expiresAt: newExpiry, isPermanent: false });
			send(isDm ? sender.id : null, `✅ Added ${days} days to **${botDoc.name}**.\nNew Expiry: ${newExpiry.toLocaleDateString()}`, isDm);
		},
		'!setpermanent': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
            const botName = args.slice(1).join(' ');
			if (!botName) return send(isDm ? sender.id : null, 'Usage: !setpermanent <bot_name>', isDm);

			const botDoc = await BotConfig.findOne({ name: new RegExp(`^${botName}$`, 'i') });
			if (!botDoc) return send(isDm ? sender.id : null, `Bot "${botName}" not found.`, isDm);

			await BotConfig.updateOne({ _id: botDoc._id }, { isPermanent: true });
			send(isDm ? sender.id : null, `💎 **${botDoc.name}** is now PERMANENT! It will never expire.`, isDm);
		},
        '!botinfo': async (sender, args, isDm) => {
            const botDoc = await BotConfig.findOne({ token: botConfig.token });
            if (!botDoc) return;
            const daysLeft = Math.ceil((botDoc.expiresAt - new Date()) / (1000 * 60 * 60 * 24));
            const subStatus = botDoc.isPermanent ? 'PERMANENT 🎖️' : (daysLeft > 0 ? daysLeft + ' days left' : 'EXPIRED!');
            send(isDm ? sender.id : null, `🤖 **Bot Info**\nName: ${botDoc.name}\nRunner: ${botDoc.assignedRunnerId}\nStatus: ${botDoc.isOnline ? 'Online' : 'Offline'}\nSubscription: ${subStatus}`, isDm);
        },
		'!punch': async (sender, args, isDm) => {
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);

			bot.player.emote(sender.id, EMOTE_MAP['punch'] || 'emoji-punch');
			bot.player.emote(targetId, EMOTE_MAP['collapse'] || 'emote-death2');
			send(isDm ? sender.id : null, `${sender.username} punched ${args[1]}!`, isDm);
		},
		'!cut': async (sender, args, isDm) => {
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);

			bot.player.emote(sender.id, EMOTE_MAP['swordfight'] || 'emote-swordfight');
			bot.player.emote(targetId, EMOTE_MAP['fallingapart'] || 'emote-apart');
			send(isDm ? sender.id : null, `${sender.username} cut ${args[1]}!`, isDm);
		},
		'!bomb': async (sender, args, isDm) => {
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);

			bot.player.emote(sender.id, EMOTE_MAP['energyball'] || 'emote-energyball');
			bot.player.emote(targetId, EMOTE_MAP['faint'] || 'emote-fainting');
			send(isDm ? sender.id : null, `${sender.username} threw a bomb at ${args[1]}!`, isDm);
		},
		'!rizz': async (sender, args, isDm) => {
			const target = args[1] || sender.username;
			let idx;
			do {
				idx = Math.floor(Math.random() * RIZZ_MESSAGES.length);
			} while (idx === lastRizzIdx && RIZZ_MESSAGES.length > 1);
			lastRizzIdx = idx;
			const msg = RIZZ_MESSAGES[idx];
			send(isDm ? sender.id : null, `💕 @${sender.username} to ${target}: ${msg}`, isDm);
		},
		'!roast': async (sender, args, isDm) => {
			const target = args[1] || sender.username;
			let idx;
			do {
				idx = Math.floor(Math.random() * ROAST_MESSAGES.length);
			} while (idx === lastRoastIdx && ROAST_MESSAGES.length > 1);
			lastRoastIdx = idx;
			const msg = ROAST_MESSAGES[idx];
			send(isDm ? sender.id : null, `🔥 @${sender.username} to ${target}: ${msg}`, isDm);
		},
		'!slap': async (sender, args, isDm) => {
			const targetId = await resolveUserIdByMention(args[1]);
			if (!targetId) return send(isDm ? sender.id : null, 'User not found.', isDm);

			bot.player.emote(sender.id, EMOTE_MAP['slap'] || 'emote-slap');
			bot.player.emote(targetId, EMOTE_MAP['stunned'] || 'emoji-dizzy');
			send(isDm ? sender.id : null, `${sender.username} slapped ${args[1]}!`, isDm);
		},
		'!bans': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.bans), d),
		'!kicks': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.kicks), d),
		'!mutes': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.mutes), d),
		'!punches': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.punches), d),
		'!cuts': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.cuts), d),
		'!voids': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.voids), d),
		'!freezes': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.freezes), d),
		'!unbans': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.unbans), d),
		'!slaps': (s, a, d) => send(d ? s.id : null, formatLogItems(state.logs?.slaps), d),
		'!fullc': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const targetId = await resolveUserIdByMention(args[1]);
			if (targetId && !(state.fullControllerUserIds || []).includes(targetId)) {
				state.fullControllerUserIds.push(targetId);
				saveState();
				send(isDm ? sender.id : null, `Granted full control to ${args[1]}.`, isDm);
			}
		},
		'!setspawn': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const pos = await getUserPosition(sender.id);
			const roomState = getRoomData(bot.roomId, state);
			roomState.spawnPos = { x: pos.x, y: pos.y, z: pos.z, facing: pos.facing || 'FrontRight' };
			saveState();

			// Walk to new spawn immediately with validation
            if (typeof pos.x === 'number' && typeof pos.y === 'number' && typeof pos.z === 'number') {
    			bot.move.walk(pos.x, pos.y, pos.z, pos.facing || 'FrontRight');
            }
			send(isDm ? sender.id : null, 'Spawn set. Moving there now!', isDm);
		},
		'!setvipspawn': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);
			const pos = await getUserPosition(sender.id);
			const roomState = getRoomData(bot.roomId, state);
			roomState.vipSpawnPos = { x: pos.x, y: pos.y, z: pos.z, facing: pos.facing || 'FrontRight' };
			saveState();
			send(isDm ? sender.id : null, 'VIP spawn set.', isDm);
		},
		'!whereami': async (sender, args, isDm) => {
			const players = await bot.room.players.fetch();
			const realId = sender.actorId || sender.id;
			const p = players.find(([u]) => u.id === realId);
			if (p?.[1]) send(isDm ? sender.id : null, `Pos: ${p[1].x.toFixed(1)} ${p[1].y.toFixed(1)} ${p[1].z.toFixed(1)}`, isDm);
		},
		'!changeroom': async (sender, args, isDm) => {
			if (!(await isOwnerOnly(sender))) return send(isDm ? sender.id : null, 'Owner only.', isDm);

			// Store the REAL userId to match against the link when it arrives in inbox
			state.waitingForInviteUser = sender.actorId || sender.id;
			saveState();

			send(isDm ? sender.id : null, "Okay. Go to the new room, tap the Room Name -> 'Share' -> 'Copy Link', and paste the link to me here!", isDm);
		},
		'!botadd': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return send(isDm ? sender.id : null, '❌ Ultimate Master Only.', isDm);

			activeBotSetups[uid] = { step: 1 };
			send(isDm ? sender.id : null, "🤖 **Interactive Bot Setup Wizard Started!**\n(Type `!cancel` at any point to exit)\n\nFirst question: What is the **Name** of the new Bot in the database?", isDm);
		},
		'!alert': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return send(isDm ? sender.id : null, '❌ Ultimate Master Only.', isDm);

			const text = args.slice(1).join(' ');
			if (!text) return send(isDm ? sender.id : null, 'Usage: !alert <message>', isDm);

			send(isDm ? sender.id : null, `📡 Broadcasting system alert to all running bots...`, isDm);
			let delivered = 0;

			for (const b of GLOBAL_BOTS) {
				try {
					// Bots shout it into their current room
					await b.message.send(`📢 **SYSTEM ALERT**: ${text}`);

					// If the owner is right here with them, whisper them personally
					if (b.botConfig && b.botConfig.addedBy) {
						const ownerId = b.botConfig.addedBy;
						try {
							const entries = b.room.players.cache.get ? b.room.players.cache.get() : (await b.room.players.fetch());
							if (entries && typeof entries.find === 'function' && entries.find(([u]) => u.id === ownerId)) {
								// The owner is standing in the room! Whisper them!
								await b.message.send(`Master Update: ${text}`, ownerId);
							}
						} catch (e) { }
					}

					// If owner has completed !subalert, drop it safely into their true Inbox!
					if (b.botConfig && b.botConfig.ownerConversationId) {
						try {
							await b.direct.send(b.botConfig.ownerConversationId, `📢 **MASTER SYSTEM ALERT**: \n${text}`);
						} catch (e) { }
					}
					delivered++;
				} catch (e) { }
			}

			send(isDm ? sender.id : null, `✅ Alert dropped into ${delivered} active rooms!`, isDm);
		},
		'!danceall': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return;
			const emote = args[1] || 'tiktok8';
			GLOBAL_BOTS.forEach(b => {
				try { b.move.emote(emote); } catch (e) { }
			});
			send(isDm ? sender.id : null, `💃 All bots are now performing: ${emote}`, isDm);
		},
		'!debug': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return;
			send(isDm ? sender.id : null, `🛠️ **BOT DIAGNOSTICS [${botName}]**\n• RoomID: ${bot.roomId}\n• UserID: ${botUserId}\n• Connected: ${bot.connected}\n• Owner: ${roomOwnerId}`, isDm);
		},
		'!addbot': (s, a, d) => COMMAND_DISPATCHER['!botadd'](s, a, d),
		'!botlist': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return send(isDm ? sender.id : null, '❌ Ultimate Master Only.', isDm);
			const all = await BotConfig.find();
			send(isDm ? sender.id : null, `Bots:\n${all.map(b => b.name).join(', ')}`, isDm);
		},
		'!botdel': async (sender, args, isDm) => {
			const uid = sender.actorId || sender.id;
			if (!OWNER_USER_IDS.includes(uid)) return send(isDm ? sender.id : null, '❌ Ultimate Master Only.', isDm);
			if (!args[1]) return send(isDm ? sender.id : null, '❌ Missing Bot Name! Use: `!botdel <BotName>`', isDm);

			await BotConfig.deleteOne({ name: new RegExp(`^${args[1]}$`, 'i') });
			send(isDm ? sender.id : null, `🗑️ Deleted Bot ${args[1]} from DB!`, isDm);
		}
	};

	return bot;

} // End of spawnBot()

async function shutdownAllAndExit(reason) {
	console.log(`[SHUTDOWN] Initiating graceful exit: ${reason}`);
	for (const bot of GLOBAL_BOTS) {
		try {
			console.log(`[SHUTDOWN] Logging out ${bot.botName}...`);
			bot.logout();
		} catch (e) { }
	}
	// Give Highrise 5 seconds to clear the server sessions
	console.log(`[SHUTDOWN] Waiting 5s for clean session drop...`);
	setTimeout(() => {
		console.log(`[SHUTDOWN] Exit complete.`);
		process.exit(0);
	}, 5000);
}

// Suicide loop: Check if this node is still the "Active" node for its specific task
async function startSuicideCheck() {
    const lockKey = ROLE === 'MASTER' ? 'master_lock' : `runner_lock_${RUNNER_ID}`;
	setInterval(async () => {
		try {
			const lock = await SystemLock.findOne({ cluster: lockKey });
			if (lock && lock.activeInstanceId !== INSTANCE_ID) {
				console.error(`[LOCK-COLLISION] New deployment detected (${lock.activeInstanceId})! This instance (${INSTANCE_ID}) is now obsolete. Shutting down...`);
				shutdownAllAndExit("Deployment Overlap Detection");
			} else {
                // Keep-alive
                await SystemLock.updateOne({ cluster: lockKey }, { lastHeartbeat: new Date() });
            }
		} catch (e) {
			console.error(`[LOCK-CHECK-ERR]`, e.message);
		}
	}, 20000); // Check every 20s (Render's overlap is usually 30-45s)
}

process.on('SIGTERM', () => shutdownAllAndExit("SIGTERM (Render Deployment Swap)"));
process.on('SIGINT', () => shutdownAllAndExit("SIGINT (Manual Stop)"));

async function bootstrapMultiBot() {
	try {
        const uri = process.env.MONGODB_URI || "mongodb+srv://heiszilla_db_user:wXYE76B8jjaVbWOe@cluster0.1oxqb8a.mongodb.net/?appName=Cluster0";
		await mongoose.connect(uri);
		console.log("📦 Connected to MongoDB (Cluster0)");

		// --- ACTIVATE DEPLOYMENT LOCK ---
        const lockKey = ROLE === 'MASTER' ? 'master_lock' : `runner_lock_${RUNNER_ID}`;
		console.log(`[LOCK] Claiming ${lockKey} for instance: ${INSTANCE_ID}`);
		await SystemLock.updateOne(
			{ cluster: lockKey },
			{ activeInstanceId: INSTANCE_ID, lastHeartbeat: new Date() },
			{ upsert: true }
		);
		startSuicideCheck();

		// --- ROLE BRANCHING ---
        if (ROLE === 'MASTER' || ROLE === 'BOTH') {
            if (ROLE === 'BOTH') console.log("[SYSTEM] Running in Hybrid BOTH mode (Master + Runner)");
            console.log("[MASTER] Dashboard and Controller systems online.");
            
            // --- MIGRATION: Auto-assign "homeless" bots to default runner ---
            const homeless = await BotConfig.updateMany(
                { assignedRunnerId: { $exists: false } }, 
                { assignedRunnerId: 'default_runner' }
            );
            if (homeless.modifiedCount > 0) {
                console.log(`[MASTER] Assigned ${homeless.modifiedCount} homeless bots to default_runner.`);
            }

            // --- MIGRATION: Ensure all bots have an expiry date ---
            const undated = await BotConfig.updateMany(
                { expiresAt: { $exists: false } },
                { expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) }
            );
            if (undated.modifiedCount > 0) {
                console.log(`[MASTER] Set 30-day expiry for ${undated.modifiedCount} legacy bots.`);
            }

            let bots = await BotConfig.find();
            if (bots.length === 0 && process.env.BOT_TOKEN) {
                console.log("🤖 First-time boot: Seeding default bot from .env!");
                const newBot = new BotConfig({ 
                    name: "Zilla Master", 
                    token: process.env.BOT_TOKEN, 
                    roomId: process.env.ROOM_ID, 
                    targetRoomId: process.env.ROOM_ID,
                    assignedRunnerId: 'default_runner' 
                });
                await newBot.save();
            }
        } 
        
        if (ROLE === 'RUNNER' || ROLE === 'BOTH') {
            console.log(`[RUNNER] Bot Engine operational. Watching for assigned bots for: ${RUNNER_ID}...`);
            await startRunnerLoop();
        }

	} catch (e) { console.error("❌ MongoDB Boot Failed:", e.message); }
}

async function startRunnerLoop() {
    // This loop checks for bots assigned to this specific runner
    setInterval(async () => {
        try {
            const assignedBots = await BotConfig.find({ assignedRunnerId: RUNNER_ID });
            const dbTokens = assignedBots.map(b => b.token);

            // --- CLEANUP STEP: Stop bots that were deleted or EXPIRED ---
            for (let i = GLOBAL_BOTS.length - 1; i >= 0; i--) {
                const activeBot = GLOBAL_BOTS[i];
                const dbBot = assignedBots.find(b => b.token === activeBot.token);
                
                const isDeleted = !dbTokens.includes(activeBot.token);
                const isExpired = dbBot && !dbBot.isPermanent && dbBot.expiresAt && new Date() > dbBot.expiresAt;

                if (isDeleted || isExpired) {
                    const reason = isDeleted ? 'Deleted from DB' : 'Subscription Expired';
                    console.log(`[CLEANUP] Bot ${activeBot.botName} shut down. Reason: ${reason}`);
                    activeBot.isTerminated = true; 
                    if (activeBot.logout) {
                        try { activeBot.logout(); } catch(e){}
                    }
                    GLOBAL_BOTS.splice(i, 1);
                }
            }
            
            // --- SYNC STEP: Start or Transfer bots ---
            for (const b of assignedBots) {
                // Skip if expired (unless permanent)
                if (!b.isPermanent && b.expiresAt && new Date() > b.expiresAt) continue;

                // 1. IS BOT SPAWNED LOCALLY?
                let activeBot = GLOBAL_BOTS.find(gb => gb.token === b.token);
                
                if (!activeBot) {
                    console.log(`[RUNNER] Found new bot job: ${b.name}. Spawning...`);
                    
                    // --- RACE CONDITION PREVENTER ---
                    // Immediately push a placeholder so we don't start it twice
                    GLOBAL_BOTS.push({ token: b.token, botName: b.name, isSpawning: true });
                    
                    try {
                        const botInstance = await spawnBot(b);
                        // Find the placeholder and replace it with the real instance
                        const pIdx = GLOBAL_BOTS.findIndex(gb => gb.token === b.token && gb.isSpawning);
                        if (pIdx !== -1) {
                            botInstance.token = b.token;
                            botInstance.botName = b.name;
                            GLOBAL_BOTS[pIdx] = botInstance;
                            
                            // Staggered login
                            setTimeout(() => {
                                if (botInstance.isTerminated) return;
                                console.log(`[RUNNER] Logging in ${b.name} to room ${b.roomId}...`);
                                botInstance.login(b.token, b.roomId);
                            }, 5000);
                        }
                    } catch (e) {
                        console.error(`[SPAWN-ERR] ${b.name}:`, e);
                        // Cleanup placeholder on failure
                        const pIdx = GLOBAL_BOTS.findIndex(gb => gb.token === b.token && gb.isSpawning);
                        if (pIdx !== -1) GLOBAL_BOTS.splice(pIdx, 1);
                    }
                    continue;
                }

                // Safety: Skip if still in spawning/placeholder phase
                if (activeBot.isSpawning) continue;

                // 2. DETECT ROOM TRANSFER REQUEST
                if (b.targetRoomId && b.targetRoomId !== b.roomId) {
                    console.log(`[TRANSFER] ${b.name} room change detected: ${b.roomId} -> ${b.targetRoomId}`);
                    
                    // SAFE TRANSFER CYCLE (Kills Multilogin ghost sessions)
                    try {
                        if (activeBot && activeBot.logout) {
                            activeBot.logout();
                        } else {
                            console.warn(`[TRANSFER-WARN] ${b.name} skip: Bot instance not fully ready.`);
                            continue;
                        }
                        console.log(`[TRANSFER] ${b.name} logged out. Waiting 15s for session cleanup...`);
                        
                        // Update current room ID in DB to "BOOTING" to prevent repeat loops
                        await BotConfig.updateOne({ token: b.token }, { roomId: 'TRANSFERRING' });

                        setTimeout(async () => {
                            console.log(`[TRANSFER] Re-logging ${b.name} into target room: ${b.targetRoomId}`);
                            activeBot.login(b.token, b.targetRoomId);
                            // Update DB after successful login attempt
                            await BotConfig.updateOne({ token: b.token }, { roomId: b.targetRoomId });
                        }, 15000);
                    } catch (e) { console.error(`[TRANSFER-ERR]`, e); }
                }
            }
        } catch (e) { console.error("[RUNNER-LOOP-ERR]", e); }
    }, 15000); // Check every 15s for snappy starts
}

bootstrapMultiBot();

