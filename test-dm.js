require('dotenv').config();
const { Highrise } = require('highrise.sdk');
const bot = new Highrise({ Events: ['Messages'] });
bot.on('ready', async () => {
    try {
        console.log("SENDING");
        await bot.direct.send('6446fc09d86431b11043cb18', 'Test Master DM via userId');
        console.log("SUCCESS");
    } catch(e) {
        console.error("FAIL", e.message);
    }
    process.exit(0);
});
bot.login(process.env.BOT_TOKEN, process.env.ROOM_ID);
