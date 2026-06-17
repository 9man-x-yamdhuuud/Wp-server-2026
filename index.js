import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, {
  useSingleFileAuthState,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  Browsers,
  fetchLatestBaileysVersion
} from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { Boom } from '@hapi/boom';
import cors from 'cors';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 5000;
const upload = multer({ dest: 'uploads/' });

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

// ---------- session management ----------
const SESSION_FILE = './running_sessions.json';
const userSessions = {};
const stopFlags = {};
const activeSockets = {};
const messageQueues = {};
const reconnectAttempts = {};
const sessionStats = {};

const saveSessions = () => {
  try {
    fs.writeFileSync(SESSION_FILE, JSON.stringify(userSessions, null, 2));
  } catch (error) {
    console.error(chalk.red(`Error saving sessions: ${error.message}`));
  }
};

const removeFile = (filePath) => {
  try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (_) {}
};

const generateUniqueKey = () => crypto.randomBytes(16).toString('hex');

const cleanupSession = (uniqueKey) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }
  delete stopFlags[uniqueKey];
  delete messageQueues[uniqueKey];
  delete activeSockets[uniqueKey];
};

// ---------- messaging ----------
const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false
    };
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
  }

  const queue = messageQueues[uniqueKey];

  const sendNextMessage = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete messageQueues[uniqueKey];
      return;
    }

    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket disconnected for ${uniqueKey}, waiting...`));
      return;
    }

    if (queue.isSending || queue.messages.length === 0) return;
    queue.isSending = true;

    let chatId = target.includes('@') ? target : `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
    const currentMessage = queue.messages[queue.currentIndex];
    const formattedMessage = hatersName ? `${hatersName} ${currentMessage}` : currentMessage;

    try {
      await MznKing.sendMessage(chatId, { text: formattedMessage });
      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = formattedMessage.substring(0, 60);
      console.log(chalk.green(`✉️ [${sessionStats[uniqueKey].sent}] Sent: ${formattedMessage.substring(0, 50)}...`));
      queue.currentIndex = (queue.currentIndex + 1) % queue.messages.length;
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Send failed: ${err.message}`));
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[uniqueKey] = { stopped: false, interval: messageInterval };
  console.log(chalk.cyan(`📨 Messaging started! Every ${speed}s → ${target}`));
  sendNextMessage();
};

// ---------- connect with creds.json ----------
const connectAndLogin = async (uniqueKey, credsData) => {
  const sessionPath = `./sessions/${uniqueKey}.json`;

  // Write uploaded creds.json to session file
  if (credsData) {
    fs.mkdirSync('./sessions', { recursive: true });
    fs.writeFileSync(sessionPath, credsData);
  } else if (!fs.existsSync(sessionPath)) {
    throw new Error('No session file found and no creds uploaded.');
  }

  const startConnection = async () => {
    try {
      console.log(chalk.magenta(`🚀 Connecting session ${uniqueKey}`));

      const { state, saveCreds } = useSingleFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      const MznKing = makeWASocket({
        version,
        logger: pino.default({ level: 'silent' }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, pino.default({ level: 'silent' }))
        },
        printQRInTerminal: false,
        generateHighQualityLinkPreview: true,
        markOnlineOnConnect: true,
        getMessage: async () => undefined,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        retryRequestDelayMs: 250,
      });

      activeSockets[uniqueKey] = MznKing;

      MznKing.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          console.log(chalk.green(`✅✅✅ Connected! [${uniqueKey}]`));
          reconnectAttempts[uniqueKey] = 0;
          userSessions[uniqueKey] = {
            ...userSessions[uniqueKey],
            uniqueKey,
            connected: true,
            lastUpdateTimestamp: Date.now()
          };
          saveSessions();

          // Resume messaging if previously set
          if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
            const { target, hatersName, messages, speed } = userSessions[uniqueKey];
            console.log(chalk.cyan(`🔄 Resuming messaging for ${uniqueKey}...`));
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = {
                messages: [...messages],
                currentIndex: 0,
                isSending: false
              };
            }
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }
        }

        if (connection === 'close') {
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;

          console.log(chalk.red(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`));

          if ([DisconnectReason.badSession, DisconnectReason.loggedOut, 401].includes(reason)) {
            console.log(chalk.red(`Session invalid, cleaning up...`));
            removeFile(sessionPath);
            cleanupSession(uniqueKey);
            if (userSessions[uniqueKey]) {
              userSessions[uniqueKey].connected = false;
              userSessions[uniqueKey].messaging = false;
              saveSessions();
            }
            return;
          }

          if (!stopFlags[uniqueKey]?.stopped) {
            reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
            const delay = Math.min(3000 * reconnectAttempts[uniqueKey], 30000);
            console.log(chalk.yellow(`🔄 Reconnecting in ${delay / 1000}s...`));
            setTimeout(startConnection, delay);
          }
        }
      });

      MznKing.ev.on('creds.update', saveCreds);

    } catch (error) {
      console.error(chalk.red(`❌ ERROR: ${error.message}`));
      if (!stopFlags[uniqueKey]?.stopped) {
        reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
        const delay = Math.min(5000 * reconnectAttempts[uniqueKey], 30000);
        setTimeout(startConnection, delay);
      }
    }
  };

  await startConnection();
};

// ---------- restore sessions ----------
const restoreSessions = async () => {
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const data = fs.readFileSync(SESSION_FILE, 'utf8');
      const saved = JSON.parse(data);
      Object.assign(userSessions, saved);

      console.log(chalk.green(`📂 Found ${Object.keys(userSessions).length} saved sessions`));

      for (const [key, session] of Object.entries(userSessions)) {
        const sessionPath = `./sessions/${key}.json`;
        if (fs.existsSync(sessionPath) && session.uniqueKey) {
          console.log(chalk.cyan(`🔄 Restoring: ${key}`));
          stopFlags[key] = { stopped: false };
          reconnectAttempts[key] = 0;

          if (session.messaging && session.messages) {
            messageQueues[key] = {
              messages: [...session.messages],
              currentIndex: 0,
              isSending: false
            };
          }

          await connectAndLogin(key, null);
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
      console.log(chalk.green(`✅ Session restoration complete!`));
    } catch (err) {
      console.error(chalk.red(`Error loading sessions: ${err.message}`));
    }
  }
};

// ---------- Routes ----------

// Login with creds.json upload
app.post('/login', upload.single('credsFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, message: 'Please upload creds.json file.' });
    }

    const credsData = fs.readFileSync(req.file.path);
    removeFile(req.file.path);  // clean temp

    const uniqueKey = generateUniqueKey();
    stopFlags[uniqueKey] = { stopped: false };
    reconnectAttempts[uniqueKey] = 0;

    // Store basic info
    userSessions[uniqueKey] = {
      uniqueKey,
      connected: false,
      messaging: false,
      lastUpdateTimestamp: Date.now()
    };
    saveSessions();

    // Start connection with the uploaded creds
    await connectAndLogin(uniqueKey, credsData);

    res.json({
      success: true,
      message: 'Session created and connecting...',
      uniqueKey
    });
  } catch (error) {
    console.error(chalk.red(`Login error: ${error.message}`));
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// Get groups (optional)
app.post('/getGroupUID', async (req, res) => {
  try {
    const { uniqueKey } = req.body;
    if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No active session' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected' });

    const MznKing = activeSockets[uniqueKey];
    await new Promise(resolve => setTimeout(resolve, 1000));

    const groups = await MznKing.groupFetchAllParticipating();
    const groupUIDs = Object.values(groups).map(group => ({
      groupName: group.subject,
      groupId: group.id,
    }));

    console.log(chalk.green(`✅ Fetched ${groupUIDs.length} groups for ${uniqueKey}`));
    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
  }
});

// Start messaging
app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;

    if (!uniqueKey || !target || !speed || !filePath) {
      return res.status(400).json({ success: false, message: 'Missing required fields or file.' });
    }
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key.' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected.' });

    let messages = [];
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0) return res.status(400).json({ success: false, message: 'File has no valid messages.' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file.' });
    } finally {
      try { fs.unlinkSync(filePath); } catch (_) {}
    }

    const MznKing = activeSockets[uniqueKey];

    userSessions[uniqueKey].target = target;
    userSessions[uniqueKey].hatersName = hatersName || '';
    userSessions[uniqueKey].messages = messages;
    userSessions[uniqueKey].speed = speed;
    userSessions[uniqueKey].messaging = true;
    saveSessions();

    delete messageQueues[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };

    startMessaging(MznKing, uniqueKey, target, hatersName || '', messages, speed);

    res.json({
      success: true,
      message: 'Messaging started!',
      uniqueKey,
      messageCount: messages.length,
      target
    });
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// Get session status
app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session = userSessions[uniqueKey];
  const stats = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  res.json({
    exists: true,
    connected: !!activeSockets[uniqueKey],
    messaging: session.messaging && !stopFlags[uniqueKey]?.stopped,
    sent: stats.sent,
    failed: stats.failed,
    lastMessage: stats.lastMessage,
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || 0,
  });
});

// Stop & logout
app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
  if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    if (stopFlags[uniqueKey]?.interval) {
      stopFlags[uniqueKey].stopped = true;
      clearInterval(stopFlags[uniqueKey].interval);
    }
    delete stopFlags[uniqueKey];
    delete messageQueues[uniqueKey];
    delete sessionStats[uniqueKey];

    if (activeSockets[uniqueKey]) {
      try {
        await activeSockets[uniqueKey].logout();
      } catch (_) {}
      delete activeSockets[uniqueKey];
    }

    const sessionPath = `./sessions/${uniqueKey}.json`;
    removeFile(sessionPath);
    delete userSessions[uniqueKey];
    saveSessions();

    console.log(chalk.red(`✅ Stopped & logged out: ${uniqueKey}`));
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

// Serve HTML
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 CORS enabled for all origins`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

  await restoreSessions();
});
