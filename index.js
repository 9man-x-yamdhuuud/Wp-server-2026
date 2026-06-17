import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, {
  useMultiFileAuthState,
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
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static('public'));

const SESSION_FILE = './running_sessions.json';
const userSessions = {};
const stopFlags = {};
const activeSockets = {};
const messageQueues = {};
const reconnectAttempts = {};
const sessionStats = {};

const saveSessions = () => {
  try {
    fs.writeFileSync(SESSION_FILE, JSON.stringify(userSessions, null, 2), 'utf8');
  } catch (error) {
    console.error(chalk.red(`Error saving sessions: ${error.message}`));
  }
};

const removeDir = (dirPath) => {
  try {
    if (fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive: true, force: true });
    }
  } catch (e) {}
};

const generateUniqueKey = () => {
  return crypto.randomBytes(16).toString('hex');
};

const EXPIRY_TIME = Infinity;
const checkSessionExpiry = (sessionTimestamp, sessionMeta) => {
  if (sessionMeta?.neverExpire) return false;
  return (Date.now() - sessionTimestamp) > EXPIRY_TIME;
};

const cleanupSession = (sessionId) => {
  if (stopFlags[sessionId]?.interval) {
    clearInterval(stopFlags[sessionId].interval);
  }
  delete stopFlags[sessionId];
  delete messageQueues[sessionId];
  delete activeSockets[sessionId];
};

const startMessaging = (MznKing, sessionId, target, hatersName, messages, speed) => {
  if (stopFlags[sessionId]?.interval) {
    clearInterval(stopFlags[sessionId].interval);
  }

  if (!messageQueues[sessionId]) {
    messageQueues[sessionId] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false
    };
  }

  if (!sessionStats[sessionId]) {
    sessionStats[sessionId] = { sent: 0, failed: 0, lastMessage: '' };
  }

  const queue = messageQueues[sessionId];

  const sendNextMessage = async () => {
    if (stopFlags[sessionId]?.stopped) {
      clearInterval(stopFlags[sessionId].interval);
      delete messageQueues[sessionId];
      return;
    }

    if (!activeSockets[sessionId]) {
      console.log(chalk.yellow(`⚠️ Socket disconnected for ${sessionId}, waiting for reconnection...`));
      return;
    }

    if (queue.isSending) return;
    if (queue.messages.length === 0) return;

    queue.isSending = true;

    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      const cleanTarget = target.replace(/[^0-9]/g, '');
      chatId = `${cleanTarget}@s.whatsapp.net`;
    }

    const currentMessage = queue.messages[queue.currentIndex];
    const formattedMessage = hatersName ? `${hatersName} ${currentMessage}` : currentMessage;

    try {
      await MznKing.sendMessage(chatId, { text: formattedMessage });
      sessionStats[sessionId].sent++;
      sessionStats[sessionId].lastMessage = formattedMessage.substring(0, 60);
      console.log(chalk.green(`✉️ [${sessionStats[sessionId].sent}] Sent to ${chatId}: ${formattedMessage.substring(0, 50)}...`));

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        console.log(chalk.cyan(`🔄 All messages sent! Restarting from beginning...`));
        queue.currentIndex = 0;
      }
    } catch (err) {
      sessionStats[sessionId].failed++;
      console.error(chalk.red(`❌ Send failed: ${err.message}`));
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[sessionId] = { stopped: false, interval: messageInterval };
  console.log(chalk.cyan(`📨 Messaging started! Every ${speed}s → ${target}`));

  sendNextMessage();
};

// ─── CORE: Initialize socket with session path ────────────────────────────
const initSocket = async (sessionId, sessionPath, onPairingCode = null) => {
  try {
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
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
      defaultQueryTimeoutMs: undefined,
      retryRequestDelayMs: 250,
    });

    activeSockets[sessionId] = MznKing;

    // If creds not registered and we have a pairing code callback, request one
    if (!MznKing.authState.creds.registered && onPairingCode) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      try {
        const phoneNumber = userSessions[sessionId]?.phoneNumber?.replace(/[^0-9]/g, '') || '';
        if (phoneNumber) {
          const code = await MznKing.requestPairingCode(phoneNumber);
          const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
          onPairingCode(pairingCode, false);
        }
      } catch (error) {
        console.error(chalk.red(`❌ Pairing error: ${error.message}`));
        if (onPairingCode) onPairingCode(null, false, error.message);
      }
    } else if (MznKing.authState.creds.registered && onPairingCode) {
      onPairingCode(null, true);
    }

    MznKing.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        console.log(chalk.green(`✅✅✅ Connected! [${sessionId}]`));
        reconnectAttempts[sessionId] = 0;

        userSessions[sessionId] = {
          ...userSessions[sessionId],
          connected: true,
          lastUpdateTimestamp: Date.now()
        };
        saveSessions();

        if (userSessions[sessionId]?.messaging && userSessions[sessionId]?.messages) {
          const { target, hatersName, messages, speed } = userSessions[sessionId];
          console.log(chalk.cyan(`🔄 Resuming messaging for ${sessionId}...`));
          if (!messageQueues[sessionId]) {
            messageQueues[sessionId] = {
              messages: [...messages],
              currentIndex: 0,
              isSending: false
            };
          }
          startMessaging(MznKing, sessionId, target, hatersName, messages, speed);
        }
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;

        console.log(chalk.red(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`));

        if (reason === DisconnectReason.badSession) {
          console.log(chalk.red(`Bad session, deleting and reconnecting...`));
          removeDir(sessionPath);
        } else if (reason === DisconnectReason.connectionReplaced) {
          console.log(chalk.red(`Connection replaced, stopping...`));
          cleanupSession(sessionId);
          return;
        } else if (reason === DisconnectReason.loggedOut) {
          console.log(chalk.red(`Device logged out, stopping...`));
          removeDir(sessionPath);
          cleanupSession(sessionId);
          if (userSessions[sessionId]) {
            userSessions[sessionId].connected = false;
            userSessions[sessionId].messaging = false;
            saveSessions();
          }
          return;
        } else if (reason === 401) {
          console.log(chalk.red(`Unauthorized (401), session expired, stopping...`));
          removeDir(sessionPath);
          cleanupSession(sessionId);
          if (userSessions[sessionId]) {
            userSessions[sessionId].connected = false;
            userSessions[sessionId].messaging = false;
            saveSessions();
          }
          return;
        }

        if (!stopFlags[sessionId]?.stopped) {
          reconnectAttempts[sessionId] = (reconnectAttempts[sessionId] || 0) + 1;
          const delay = Math.min(3000 * reconnectAttempts[sessionId], 30000);
          console.log(chalk.yellow(`🔄 Reconnecting in ${delay / 1000}s... (Attempt ${reconnectAttempts[sessionId]})`));
          setTimeout(() => initSocket(sessionId, sessionPath, onPairingCode), delay);
        }
      }
    });

    MznKing.ev.on('creds.update', saveCreds);
    MznKing.ev.on('messages.upsert', () => {});

    return MznKing;
  } catch (error) {
    console.error(chalk.red(`❌ Socket init error: ${error.message}`));
    throw error;
  }
};

// ─── LOGIN (with pairing code) ─────────────────────────────────────────────
const connectAndLogin = async (phoneNumber, sessionId, sendPairingCode) => {
  const sessionPath = `./session/${sessionId}`;
  let pairingCodeSent = false;

  try {
    if (!fs.existsSync(sessionPath)) {
      fs.mkdirSync(sessionPath, { recursive: true });
    }

    userSessions[sessionId] = {
      ...userSessions[sessionId],
      phoneNumber,
      sessionId,
      connected: false,
      lastUpdateTimestamp: Date.now()
    };
    saveSessions();

    const onPairing = (pairingCode, isConnected, errorMsg) => {
      if (pairingCodeSent) return;
      pairingCodeSent = true;
      if (errorMsg) {
        sendPairingCode(null, false, errorMsg);
      } else if (isConnected) {
        sendPairingCode(null, true);
      } else {
        sendPairingCode(pairingCode, false);
      }
    };

    await initSocket(sessionId, sessionPath, onPairing);
  } catch (error) {
    console.error(chalk.red(`❌ Login error: ${error.message}`));
    if (!pairingCodeSent && sendPairingCode) {
      sendPairingCode(null, false, error.message);
    }
  }
};

// ─── RESTORE SESSIONS ──────────────────────────────────────────────────────
const restoreSessions = async () => {
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const data = fs.readFileSync(SESSION_FILE, 'utf8');
      const savedSessions = JSON.parse(data);
      Object.assign(userSessions, savedSessions);

      console.log(chalk.green(`📂 Found ${Object.keys(userSessions).length} saved sessions`));

      for (const [sessionId, session] of Object.entries(userSessions)) {
        if (session.phoneNumber && session.sessionId) {
          const sessionPath = `./session/${session.sessionId}`;
          if (fs.existsSync(sessionPath)) {
            console.log(chalk.cyan(`🔄 Restoring: ${session.sessionId} (${session.phoneNumber})`));
            stopFlags[session.sessionId] = { stopped: false };
            reconnectAttempts[session.sessionId] = 0;

            if (session.messaging && session.messages) {
              messageQueues[session.sessionId] = {
                messages: [...session.messages],
                currentIndex: 0,
                isSending: false
              };
            }

            await initSocket(session.sessionId, sessionPath, null);
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        }
      }
      console.log(chalk.green(`✅ Session restoration complete!`));
    } catch (err) {
      console.error(chalk.red(`Error loading sessions: ${err.message}`));
    }
  }
};

// ─── ROUTES ──────────────────────────────────────────────────────────────────

// ✅ NEW: Load creds.json directly
app.post('/loadCreds', async (req, res) => {
  try {
    const { creds } = req.body;
    if (!creds) {
      return res.status(400).json({ success: false, message: 'Missing creds object' });
    }

    // Basic validation
    if (!creds['WALoginInfo'] && !creds['noiseKey'] && !creds['me']) {
      return res.status(400).json({ success: false, message: 'Invalid creds.json format' });
    }

    const sessionId = generateUniqueKey();
    const sessionPath = `./session/${sessionId}`;

    // Create session directory and write creds.json
    if (!fs.existsSync(sessionPath)) {
      fs.mkdirSync(sessionPath, { recursive: true });
    }
    fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));

    // Store session metadata
    userSessions[sessionId] = {
      sessionId,
      connected: false,
      lastUpdateTimestamp: Date.now(),
      phoneNumber: creds.me?.user || 'unknown',
      messaging: false
    };
    saveSessions();

    stopFlags[sessionId] = { stopped: false };
    reconnectAttempts[sessionId] = 0;

    // Initialize socket with the creds (no pairing code needed)
    try {
      await initSocket(sessionId, sessionPath, null);
      // Wait a bit for connection to establish
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Check if connected
      const isConnected = !!activeSockets[sessionId]?.authState?.creds?.registered;

      if (isConnected) {
        userSessions[sessionId].connected = true;
        saveSessions();
        return res.json({
          success: true,
          sessionId,
          message: 'WhatsApp connected successfully with creds.json'
        });
      } else {
        // Still initializing, but we return the sessionId anyway
        // The frontend will poll /sessionStatus to check readiness
        return res.json({
          success: true,
          sessionId,
          message: 'Session created, connecting...'
        });
      }
    } catch (err) {
      // Clean up on failure
      removeDir(sessionPath);
      delete userSessions[sessionId];
      saveSessions();
      return res.status(500).json({
        success: false,
        message: 'Failed to initialize WhatsApp client',
        error: err.message
      });
    }
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ─── LOGIN (pairing code) ──────────────────────────────────────────────────
app.post('/login', async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone number is required!' });

    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    console.log(chalk.cyan(`📞 Login: ${phoneNumber}`));

    const sessionId = generateUniqueKey();
    stopFlags[sessionId] = { stopped: false };
    reconnectAttempts[sessionId] = 0;

    const sendPairingCode = (pairingCode, isConnected = false, errorMsg = null) => {
      if (errorMsg) {
        res.json({ success: false, message: 'Error generating pairing code', error: errorMsg, sessionId });
      } else if (isConnected) {
        res.json({ success: true, message: 'WhatsApp Connected!', connected: true, sessionId });
      } else {
        res.json({ success: true, message: 'Pairing code generated', pairingCode, sessionId });
      }
    };

    await connectAndLogin(phoneNumber, sessionId, sendPairingCode);
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ─── GET GROUPS ─────────────────────────────────────────────────────────────
app.post('/getGroupUID', async (req, res) => {
  try {
    const { sessionId } = req.body;
    if (!sessionId) return res.status(400).json({ success: false, message: 'Missing sessionId' });
    if (!userSessions[sessionId]) return res.status(400).json({ success: false, message: 'No active session' });
    if (!activeSockets[sessionId]) return res.status(400).json({ success: false, message: 'WhatsApp not connected' });

    const MznKing = activeSockets[sessionId];
    await new Promise(resolve => setTimeout(resolve, 1000));

    const groups = await MznKing.groupFetchAllParticipating();
    const groupUIDs = Object.values(groups).map(group => ({
      groupName: group.subject,
      groupId: group.id,
    }));

    console.log(chalk.green(`✅ Fetched ${groupUIDs.length} groups for ${sessionId}`));
    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
  }
});

// ─── START MESSAGING ───────────────────────────────────────────────────────
app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { sessionId, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;

    if (!sessionId || !target || !speed) {
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    }
    if (!userSessions[sessionId]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[sessionId]) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    if (!filePath) return res.status(400).json({ success: false, message: 'No message file uploaded!' });

    let messages = [];
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0) return res.status(400).json({ success: false, message: 'File has no valid messages!' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file!' });
    } finally {
      try { fs.unlinkSync(filePath); } catch (e) {}
    }

    const MznKing = activeSockets[sessionId];

    userSessions[sessionId].target = target;
    userSessions[sessionId].hatersName = hatersName || '';
    userSessions[sessionId].messages = messages;
    userSessions[sessionId].speed = speed;
    userSessions[sessionId].messaging = true;
    saveSessions();

    delete messageQueues[sessionId];
    sessionStats[sessionId] = { sent: 0, failed: 0, lastMessage: '' };

    startMessaging(MznKing, sessionId, target, hatersName || '', messages, speed);

    res.json({
      success: true,
      message: 'Message automation started!',
      sessionId,
      messageCount: messages.length,
      target
    });
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ─── SESSION STATUS ────────────────────────────────────────────────────────
app.get('/sessionStatus/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  const session = userSessions[sessionId];
  const stats = sessionStats[sessionId] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  res.json({
    exists: true,
    connected: !!activeSockets[sessionId],
    messaging: session.messaging && !stopFlags[sessionId]?.stopped,
    sent: stats.sent,
    failed: stats.failed,
    lastMessage: stats.lastMessage,
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || 0,
  });
});

// ─── STOP ──────────────────────────────────────────────────────────────────
app.post('/stop', async (req, res) => {
  const { sessionId } = req.body;
  if (!sessionId) return res.status(400).json({ success: false, message: 'Missing sessionId' });
  if (!userSessions[sessionId]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    if (stopFlags[sessionId]?.interval) {
      stopFlags[sessionId].stopped = true;
      clearInterval(stopFlags[sessionId].interval);
    }
    delete stopFlags[sessionId];
    delete messageQueues[sessionId];
    delete sessionStats[sessionId];

    if (activeSockets[sessionId]) {
      try {
        await activeSockets[sessionId].logout();
      } catch (e) {}
      delete activeSockets[sessionId];
    }

    const sessionPath = `./session/${sessionId}`;
    removeDir(sessionPath);
    delete userSessions[sessionId];
    saveSessions();

    console.log(chalk.red(`✅ Stopped & logged out: ${sessionId}`));
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

// ─── HEALTH CHECK ──────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── START SERVER ──────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 CORS enabled for all origins`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

  await restoreSessions();
});
