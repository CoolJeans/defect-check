const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();

const app = express();
const server = http.createServer(app);

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

const io = new Server(server, {
  maxHttpBufferSize: 1e8,
  cors: { origin: '*' }
});

const DB_FILE = './defects.db';
const db = new sqlite3.Database(DB_FILE, (err) => {
  if (err) console.error('DB 연결 실패:', err);
  else console.log('📦 SQLite DB (defects.db) 연결 성공');
});

db.serialize(() => {
  db.run('PRAGMA journal_mode = WAL');
  db.run('PRAGMA synchronous = NORMAL');

  // 1. 불량 대상 관리 테이블
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

  // 2. [신규] 전체 스캔 이력 테이블 (정상/불량/중복 모두 기록)
  db.run(`
    CREATE TABLE IF NOT EXISTS scan_history (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      scanned_id TEXT,
      result_status TEXT,
      worker TEXT,
      scanned_at TEXT,
      method TEXT
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_history_time ON scan_history (seq DESC)');
});

function getStats(callback) {
  db.get('SELECT COUNT(*) as total, SUM(matched) as completed FROM defects', [], (err, stat) => {
    const total = stat ? stat.total : 0;
    const completed = (stat && stat.completed) ? stat.completed : 0;
    callback(total, completed);
  });
}

// -------------------------------------------------------------
// 📥 엑셀 및 DB 파일 다운로드 API
// -------------------------------------------------------------
// A. SQLite DB 원본 파일 다운로드
app.get('/api/download-db', (req, res) => {
  const filePath = path.resolve(DB_FILE);
  if (fs.existsSync(filePath)) {
    const dateStr = new Date().toISOString().slice(0, 10);
    res.download(filePath, `defects_backup_${dateStr}.db`);
  } else {
    res.status(404).send('DB 파일이 없습니다.');
  }
});

// B. 불량 선별 결과 목록 CSV 다운로드
app.get('/api/download-defects-csv', (req, res) => {
  db.all('SELECT id, matched, worker, matched_at, method FROM defects ORDER BY matched DESC, id ASC', [], (err, rows) => {
    if (err) return res.status(500).send('조회 오류');
    let csv = '\uFEFF제품 ID,선별 상태,작업자,선별 시각,입력 방식\n';
    rows.forEach(r => {
      const st = r.matched === 1 ? '선별 완료' : '미선별(대기)';
      csv += `"${r.id}","${st}","${r.worker}","${r.matched_at}","${r.method}"\n`;
    });
    const dateStr = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="defect_targets_${dateStr}.csv"`);
    res.send(csv);
  });
});

// C. 전체 스캔 이력 로그 CSV 다운로드
app.get('/api/download-history-csv', (req, res) => {
  db.all('SELECT seq, scanned_id, result_status, worker, scanned_at, method FROM scan_history ORDER BY seq DESC', [], (err, rows) => {
    if (err) return res.status(500).send('조회 오류');
    let csv = '\uFEFF순번,스캔 제품 ID,판정 결과,작업자,스캔 시각,입력 방식\n';
    rows.forEach(r => {
      csv += `"${r.seq}","${r.scanned_id}","${r.result_status}","${r.worker}","${r.scanned_at}","${r.method}"\n`;
    });
    const dateStr = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="scan_history_${dateStr}.csv"`);
    res.send(csv);
  });
});

// -------------------------------------------------------------
// 📋 페이징 API
// -------------------------------------------------------------
// 1. 불량 대상 리스트 페이징 조회
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

// 2. 스캔 이력 페이징 조회
app.get('/api/history', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 50;
  const offset = (page - 1) * limit;

  db.get('SELECT COUNT(*) as count FROM scan_history', [], (err, cRow) => {
    const total = cRow ? cRow.count : 0;
    db.all(
      'SELECT * FROM scan_history ORDER BY seq DESC LIMIT ? OFFSET ?',
      [limit, offset],
      (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ total, page, limit, items: rows });
      }
    );
  });
});

// 대용량 ID 일괄 등록
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
        getStats((total, completed) => {
          io.emit('stats_updated', { total, completed });
          io.emit('refresh_table');
          res.json({ success: true, count: idList.length, elapsed });
        });
      });
    });
  });
});

// -------------------------------------------------------------
// ⚡ 실시간 WebSocket 스캔 및 이력 저장 처리
// -------------------------------------------------------------
io.on('connection', (socket) => {
  getStats((total, completed) => {
    socket.emit('stats_updated', { total, completed });
  });

  socket.on('match_defect', ({ targetId, worker, method }) => {
    const cleanId = String(targetId).trim();
    const now = new Date().toLocaleTimeString('ko-KR');

    db.get('SELECT * FROM defects WHERE id = ?', [cleanId], (err, row) => {
      let resultStatus = '정상 (미등록)';
      let emitStatus = 'NOT_FOUND';

      if (!row) {
        // 불량 대상 목록에 없는 제품
        resultStatus = '정상 (미대상)';
        emitStatus = 'NOT_FOUND';
      } else if (row.matched === 1) {
        // 이미 선별 완료된 제품
        resultStatus = '중복 (기완료)';
        emitStatus = 'ALREADY_MATCHED';
      } else {
        // 불량 선별 성공!
        resultStatus = '불량 선별 완료';
        emitStatus = 'SUCCESS';
      }

      // 1. 모든 스캔 이력을 scan_history 테이블에 영구 저장
      db.run(
        'INSERT INTO scan_history (scanned_id, result_status, worker, scanned_at, method) VALUES (?, ?, ?, ?, ?)',
        [cleanId, resultStatus, worker, now, method]
      );

      // 2. 불량품인 경우 defects 테이블 상태 업데이트
      if (emitStatus === 'SUCCESS') {
        db.run(
          'UPDATE defects SET matched = 1, worker = ?, matched_at = ?, method = ? WHERE id = ?',
          [worker, now, method, cleanId],
          function () {
            getStats((total, completed) => {
              io.emit('stats_updated', { total, completed });
              io.emit('refresh_table');
            });
          }
        );
      }

      // 3. 스캐너 작업자에게 즉시 결과 통보 (시각적 HUD 피드백용)
      socket.emit('match_result', {
        status: emitStatus,
        id: cleanId,
        worker: (row && row.matched === 1) ? row.worker : worker,
        matchedAt: (row && row.matched === 1) ? row.matched_at : now
      });

      // 4. 모든 작업자에게 실시간 이력 갱신 브로드캐스트
      io.emit('refresh_history');
    });
  });

  socket.on('clear_all', () => {
    db.run('DELETE FROM defects', () => {
      db.run('DELETE FROM scan_history', () => {
        io.emit('stats_updated', { total: 0, completed: 0 });
        io.emit('refresh_table');
        io.emit('refresh_history');
      });
    });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 불량 선별 시스템 서버 시작 (Port: ${PORT})`);
});
