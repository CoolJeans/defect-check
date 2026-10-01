const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const server = http.createServer(app);

// 대용량 등록 한도 확장 (50MB)
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Socket.io 생성
const io = new Server(server, {
  maxHttpBufferSize: 1e8,
  cors: { origin: '*' }
});

// 1. SQLite DB 초기화
const db = new sqlite3.Database('./defects.db', (err) => {
  if (err) console.error('DB 연결 실패:', err);
  else console.log('📦 SQLite DB (defects.db) 연결 성공');
});

db.serialize(() => {
  db.run('PRAGMA journal_mode = WAL');
  db.run('PRAGMA synchronous = NORMAL');
  db.run(`
    CREATE TABLE IF NOT EXISTS defects (
      id TEXT PRIMARY KEY,
      matched INTEGER DEFAULT 0,
      worker TEXT DEFAULT '-',
      matched_at TEXT DEFAULT '-',
      method TEXT DEFAULT '-'
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_defects_matched ON defects (matched)');
});

function getStatsAndSample(callback) {
  db.get('SELECT COUNT(*) as total, SUM(matched) as completed FROM defects', [], (err, stat) => {
    const total = stat ? stat.total : 0;
    const completed = (stat && stat.completed) ? stat.completed : 0;
    callback(total, completed);
  });
}

// 2. 대용량 일괄 등록 API (트랜잭션 고속 커밋)
app.post('/api/upload-bulk', (req, res) => {
  const idList = req.body.list;
  if (!idList || !Array.isArray(idList)) {
    return res.status(400).json({ success: false, message: '리스트 형식이 올바르지 않습니다.' });
  }

  const startTime = Date.now();
  db.serialize(() => {
    db.run('BEGIN TRANSACTION');
    const stmt = db.prepare('INSERT OR IGNORE INTO defects (id, matched, worker, matched_at, method) VALUES (?, 0, "-", "-", "-")');

    for (let i = 0; i < idList.length; i++) {
      const cleanId = String(idList[i]).trim();
      if (cleanId) stmt.run(cleanId);
    }

    stmt.finalize(() => {
      db.run('COMMIT', (err) => {
        if (err) return res.status(500).json({ success: false, error: err.message });
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);

        getStatsAndSample((total, completed) => {
          io.emit('stats_updated', { total, completed });
          io.emit('broadcast_log', {
            time: new Date().toLocaleTimeString('ko-KR'),
            message: `📥 대용량 ID ${idList.length.toLocaleString()}건 등록 완료! (총 ${total.toLocaleString()}건, ${elapsed}초)`,
            type: 'info'
          });
          res.json({ success: true, count: idList.length, elapsed });
        });
      });
    });
  });
});

// 3. 페이징 및 검색 API
app.get('/api/defects', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 100;
  const offset = (page - 1) * limit;
  const search = req.query.search ? `%${req.query.search.trim()}%` : '%';
  const filter = req.query.filter || 'all';

  let whereClause = 'WHERE id LIKE ?';
  if (filter === 'waiting') whereClause += ' AND matched = 0';
  else if (filter === 'completed') whereClause += ' AND matched = 1';

  db.get(`SELECT COUNT(*) as count FROM defects ${whereClause}`, [search], (err, countRow) => {
    const totalFiltered = countRow ? countRow.count : 0;
    db.all(
      `SELECT * FROM defects ${whereClause} ORDER BY matched DESC, id ASC LIMIT ? OFFSET ?`,
      [search, limit, offset],
      (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ total: totalFiltered, page, limit, items: rows });
      }
    );
  });
});

// 4. Socket.io 실시간 이벤트
io.on('connection', (socket) => {
  getStatsAndSample((total, completed) => {
    socket.emit('stats_updated', { total, completed });
  });

  socket.on('match_defect', ({ targetId, worker, method }) => {
    const cleanId = String(targetId).trim();
    const now = new Date().toLocaleTimeString('ko-KR');

    db.get('SELECT * FROM defects WHERE id = ?', [cleanId], (err, row) => {
      if (err || !row) {
        socket.emit('match_result', { status: 'NOT_FOUND', id: cleanId });
        return;
      }
      if (row.matched === 1) {
        socket.emit('match_result', {
          status: 'ALREADY_MATCHED',
          id: cleanId,
          worker: row.worker,
          matchedAt: row.matched_at
        });
        return;
      }

      db.run(
        'UPDATE defects SET matched = 1, worker = ?, matched_at = ?, method = ? WHERE id = ?',
        [worker, now, method, cleanId],
        function (updateErr) {
          if (updateErr) return;

          socket.emit('match_result', {
            status: 'SUCCESS',
            id: cleanId,
            worker: worker,
            matchedAt: now,
            method: method
          });

          getStatsAndSample((total, completed) => {
            io.emit('stats_updated', { total, completed });
            io.emit('refresh_table');
            io.emit('broadcast_log', {
              time: now,
              message: `🎯 [${cleanId}] 선별 완료! (작업자: ${worker}, 방식: ${method})`,
              type: 'success'
            });
          });
        }
      );
    });
  });

  socket.on('clear_all', () => {
    db.run('DELETE FROM defects', () => {
      io.emit('stats_updated', { total: 0, completed: 0 });
      io.emit('refresh_table');
      io.emit('broadcast_log', {
        time: new Date().toLocaleTimeString('ko-KR'),
        message: '🗑️ 전체 불량 데이터가 초기화되었습니다.',
        type: 'warn'
      });
    });
  });
});

// Render 클라우드 환경 변수 PORT 바인딩 (기본값 3000)
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Render 클라우드 서버 가동 완료 (Port: ${PORT})`);
});
