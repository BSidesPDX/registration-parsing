/**
 * Hackboat 2026 — QR Check-In System
 *
 * SETUP:
 * 1. Open your "Hackboat 2026 Registration and Shirts" spreadsheet
 * 2. Extensions → Apps Script → paste this file
 * 3. Add a sheet tab called "Volunteers" with volunteer emails in column A
 * 4. Add two columns to your registration sheet if missing:
 *    - "CheckedIn" (TRUE/FALSE)
 *    - "CheckedInBy" (volunteer email)
 * 5. Deploy → New Deployment → Web App
 *    - Execute as: Me
 *    - Who has access: Anyone with Google Account
 * 6. Copy the deployment URL
 *
 * SHEET COLUMNS EXPECTED:
 * A: Date | B: Product Quantity | C: Tier | D: Name | E: Email
 * F: Shirt Size | G: Special | H: Checked In (TRUE/FALSE)
 * I: Check-In Time | J: Checked In By
 */

// ── CONFIG ──────────────────────────────────────────────────────────────────

const CONFIG = {
  REGISTRATION_SHEET: 'Attendees',
  VOLUNTEER_SHEET: 'Volunteers',
  COL: {
    NAME: 4,           // D
    EMAIL: 5,          // E
    SHIRT_SIZE: 6,     // F
    SPECIAL: 7,        // G
    CHECKED_IN: 8,     // H
    CHECKED_IN_TIME: 9,// I
    CHECKED_IN_BY: 10  // J
  }
};

// ── WEB HANDLERS ────────────────────────────────────────────────────────────

function doGet(e) {
  // Serve the scanner UI
  return HtmlService.createHtmlOutput(getScannerHtml())
    .setTitle('Hackboat Check-In')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function doPost(e) {
  // Should not be called directly — all client calls go through google.script.run
  return ContentService.createTextOutput('Use the scanner UI');
}

// ── SERVER FUNCTIONS (called from client via google.script.run) ─────────────

/**
 * Verify the current user is an authorized volunteer.
 * Called once when the scanner page loads.
 */
function verifyVolunteer() {
  const user = Session.getActiveUser().getEmail().toLowerCase();
  if (!user) {
    return { authorized: false, error: 'Could not determine your Google account.' };
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const volSheet = ss.getSheetByName(CONFIG.VOLUNTEER_SHEET);

  if (!volSheet) {
    return { authorized: false, error: 'Volunteers sheet not found. Add a sheet tab called "Volunteers" with emails in column A.' };
  }

  const volunteers = volSheet.getDataRange().getValues()
    .flat()
    .map(v => v.toString().toLowerCase().trim())
    .filter(v => v.length > 0);

  if (!volunteers.includes(user)) {
    return { authorized: false, error: `${user} is not on the volunteer list.` };
  }

  return { authorized: true, email: user };
}

/**
 * Look up an attendee by name||email from the QR payload.
 * Does NOT check them in — just returns their info for confirmation.
 */
function lookupAttendee(qrPayload) {
  const parts = qrPayload.split('||');
  if (parts.length !== 2) {
    return { found: false, error: 'Invalid QR code format.' };
  }

  const [qrName, qrEmail] = parts.map(s => s.trim());
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.REGISTRATION_SHEET);
  const data = sheet.getDataRange().getValues();

  for (let i = 1; i < data.length; i++) {
    const rowEmail = (data[i][CONFIG.COL.EMAIL - 1] || '').toString().trim().toLowerCase();
    if (rowEmail === qrEmail.toLowerCase()) {
      return {
        found: true,
        row: i + 1,
        name: data[i][CONFIG.COL.NAME - 1],
        email: data[i][CONFIG.COL.EMAIL - 1],
        shirtSize: data[i][CONFIG.COL.SHIRT_SIZE - 1],
        special: data[i][CONFIG.COL.SPECIAL - 1],
        tier: data[i][2], // Column C
        alreadyCheckedIn: data[i][CONFIG.COL.CHECKED_IN - 1] === true || data[i][CONFIG.COL.CHECKED_IN - 1] === 'TRUE',
        checkedInBy: data[i][CONFIG.COL.CHECKED_IN_BY - 1] || '',
        checkedInTime: data[i][CONFIG.COL.CHECKED_IN_TIME - 1]
          ? new Date(data[i][CONFIG.COL.CHECKED_IN_TIME - 1]).toLocaleString()
          : ''
      };
    }
  }

  return { found: false, error: `No registration found for ${qrEmail}` };
}

/**
 * Mark an attendee as checked in. Requires row number from prior lookup.
 */
function checkInAttendee(row) {
  const user = Session.getActiveUser().getEmail();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.REGISTRATION_SHEET);

  // Double-check not already checked in
  const currentVal = sheet.getRange(row, CONFIG.COL.CHECKED_IN).getValue();
  if (currentVal === true || currentVal === 'TRUE') {
    return { success: false, error: 'Already checked in.' };
  }

  sheet.getRange(row, CONFIG.COL.CHECKED_IN).setValue('TRUE');
  sheet.getRange(row, CONFIG.COL.CHECKED_IN_TIME).setValue(new Date());
  sheet.getRange(row, CONFIG.COL.CHECKED_IN_BY).setValue(user);

  return { success: true };
}

/**
 * Get live stats for the dashboard.
 */
function getStats() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(CONFIG.REGISTRATION_SHEET);
  const data = sheet.getDataRange().getValues();

  let total = 0;
  let checkedIn = 0;

  for (let i = 1; i < data.length; i++) {
    if (data[i][CONFIG.COL.NAME - 1]) {
      total++;
      if (data[i][CONFIG.COL.CHECKED_IN - 1] === true || data[i][CONFIG.COL.CHECKED_IN - 1] === 'TRUE') checkedIn++;
    }
  }

  return { total, checkedIn, remaining: total - checkedIn };
}


// ── SCANNER HTML ────────────────────────────────────────────────────────────

function getScannerHtml() {
  return `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<title>Hackboat Check-In</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@400;600;700&display=swap');

  * { margin: 0; padding: 0; box-sizing: border-box; }

  :root {
    --bg: #0a0e17;
    --surface: #111827;
    --surface-2: #1a2235;
    --border: #2a3650;
    --text: #e2e8f0;
    --text-dim: #64748b;
    --accent: #06b6d4;
    --accent-glow: rgba(6, 182, 212, 0.3);
    --green: #10b981;
    --green-glow: rgba(16, 185, 129, 0.2);
    --yellow: #f59e0b;
    --red: #ef4444;
    --red-glow: rgba(239, 68, 68, 0.15);
  }

  body {
    font-family: 'Space Grotesk', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
    min-height: 100dvh;
    overflow-x: hidden;
  }

  .scanline {
    position: fixed;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--accent), transparent);
    animation: scanline 4s ease-in-out infinite;
    z-index: 100;
    opacity: 0.6;
  }

  @keyframes scanline {
    0%, 100% { transform: translateY(0); opacity: 0; }
    10% { opacity: 0.6; }
    90% { opacity: 0.6; }
    50% { transform: translateY(50vh); }
  }

  .app {
    max-width: 480px;
    margin: 0 auto;
    padding: 20px 16px;
    min-height: 100vh;
    min-height: 100dvh;
  }

  header {
    text-align: center;
    padding: 16px 0 20px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
  }

  header h1 {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    color: var(--accent);
    text-shadow: 0 0 30px var(--accent-glow);
  }

  header .subtitle {
    font-size: 0.75rem;
    color: var(--text-dim);
    margin-top: 4px;
    font-family: 'JetBrains Mono', monospace;
  }

  /* Stats bar */
  .stats {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 8px;
    margin-bottom: 20px;
  }

  .stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px;
    text-align: center;
  }

  .stat-card .num {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.5rem;
    font-weight: 700;
  }

  .stat-card .label {
    font-size: 0.65rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 2px;
  }

  .stat-card.checked .num { color: var(--green); }
  .stat-card.remaining .num { color: var(--yellow); }
  .stat-card.total .num { color: var(--accent); }

  /* Auth state */
  .auth-msg {
    text-align: center;
    padding: 40px 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.85rem;
    color: var(--text-dim);
  }

  .auth-msg.error {
    color: var(--red);
    background: var(--red-glow);
    border-radius: 8px;
    border: 1px solid rgba(239, 68, 68, 0.3);
  }

  /* Scanner area */
  .scanner-area {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    overflow: hidden;
    margin-bottom: 20px;
  }

  #reader {
    width: 100%;
  }

  #reader video {
    border-radius: 12px 12px 0 0;
  }

  /* html5-qrcode overrides */
  #reader img[alt="Info icon"] { display: none !important; }
  #reader div:has(> img[alt="Info icon"]) { display: none !important; }
  #reader__dashboard { padding: 10px !important; }
  #reader__dashboard_section_csr button {
    background: var(--accent) !important;
    color: var(--bg) !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 10px 20px !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    cursor: pointer !important;
  }
  #reader__dashboard_section_csr select {
    background: var(--surface-2) !important;
    color: var(--text) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    padding: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
  }
  #reader__dashboard span, #reader__dashboard a {
    color: var(--text-dim) !important;
    font-size: 0.75rem !important;
  }
  #reader__scan_region { background: var(--bg) !important; }

  /* Result card */
  .result-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 16px;
    animation: slideUp 0.3s ease-out;
  }

  @keyframes slideUp {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .result-card.success {
    border-color: var(--green);
    box-shadow: 0 0 20px var(--green-glow), inset 0 1px 0 rgba(16, 185, 129, 0.1);
  }

  .result-card.warning {
    border-color: var(--yellow);
    box-shadow: 0 0 20px rgba(245, 158, 11, 0.15);
  }

  .result-card.error {
    border-color: var(--red);
    box-shadow: 0 0 20px var(--red-glow);
  }

  .result-status {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .result-status .icon {
    font-size: 1.1rem;
  }

  .result-card.success .result-status { color: var(--green); }
  .result-card.warning .result-status { color: var(--yellow); }
  .result-card.error .result-status { color: var(--red); }

  .attendee-name {
    font-size: 1.5rem;
    font-weight: 700;
    margin-bottom: 12px;
    line-height: 1.2;
  }

  .detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
  }

  .detail {
    background: var(--surface-2);
    border-radius: 6px;
    padding: 10px;
  }

  .detail .detail-label {
    font-size: 0.6rem;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 4px;
    font-family: 'JetBrains Mono', monospace;
  }

  .detail .detail-value {
    font-size: 1rem;
    font-weight: 600;
  }

  .detail.shirt .detail-value {
    font-size: 1.4rem;
    color: var(--accent);
  }

  .detail.special .detail-value {
    color: var(--yellow);
  }

  .detail.full-width {
    grid-column: 1 / -1;
  }

  .btn-checkin {
    display: block;
    width: 100%;
    margin-top: 16px;
    padding: 14px;
    background: var(--green);
    color: #fff;
    border: none;
    border-radius: 8px;
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1rem;
    font-weight: 700;
    cursor: pointer;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    transition: all 0.15s;
  }

  .btn-checkin:active {
    transform: scale(0.97);
  }

  .btn-checkin:disabled {
    background: var(--border);
    color: var(--text-dim);
    cursor: not-allowed;
  }

  .btn-dismiss {
    display: block;
    width: 100%;
    margin-top: 8px;
    padding: 10px;
    background: transparent;
    color: var(--text-dim);
    border: 1px solid var(--border);
    border-radius: 8px;
    font-family: 'Space Grotesk', sans-serif;
    font-size: 0.85rem;
    cursor: pointer;
    transition: all 0.15s;
  }

  .btn-dismiss:active {
    background: var(--surface-2);
  }

  .volunteer-id {
    text-align: center;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    color: var(--text-dim);
    padding-top: 16px;
    border-top: 1px solid var(--border);
    margin-top: 16px;
  }
</style>
</head>
<body>

<div class="scanline"></div>

<div class="app">
  <header>
    <h1>⚓ HACKBOAT 2026</h1>
    <div class="subtitle">check-in terminal</div>
  </header>

  <div class="stats" id="stats">
    <div class="stat-card checked">
      <div class="num" id="stat-checked">—</div>
      <div class="label">Checked In</div>
    </div>
    <div class="stat-card remaining">
      <div class="num" id="stat-remaining">—</div>
      <div class="label">Remaining</div>
    </div>
    <div class="stat-card total">
      <div class="num" id="stat-total">—</div>
      <div class="label">Total</div>
    </div>
  </div>

  <div id="auth-area">
    <div class="auth-msg">Verifying volunteer access...</div>
  </div>

  <div id="scanner-area" class="scanner-area" style="display:none;">
    <div id="reader"></div>
  </div>

  <div id="result-area"></div>

  <div class="volunteer-id" id="volunteer-id"></div>
</div>

<!-- html5-qrcode library -->
<script src="https://unpkg.com/html5-qrcode@2.3.8/html5-qrcode.min.js"><\/script>

<script>
  let scanner = null;
  let isProcessing = false;
  let currentLookup = null;

  // ── Init ──────────────────────────────────────────────────────────────

  document.addEventListener('DOMContentLoaded', () => {
    google.script.run
      .withSuccessHandler(onAuthResult)
      .withFailureHandler(onAuthError)
      .verifyVolunteer();

    refreshStats();
  });

  function onAuthResult(result) {
    const el = document.getElementById('auth-area');
    if (result.authorized) {
      el.style.display = 'none';
      document.getElementById('volunteer-id').textContent = 'logged in as ' + result.email;
      document.getElementById('scanner-area').style.display = 'block';
      startScanner();
    } else {
      el.innerHTML = '<div class="auth-msg error">' + escapeHtml(result.error) + '</div>';
    }
  }

  function onAuthError(err) {
    document.getElementById('auth-area').innerHTML =
      '<div class="auth-msg error">Auth failed: ' + escapeHtml(err.message || err) + '</div>';
  }

  // ── Stats ─────────────────────────────────────────────────────────────

  function refreshStats() {
    google.script.run
      .withSuccessHandler(s => {
        document.getElementById('stat-checked').textContent = s.checkedIn;
        document.getElementById('stat-remaining').textContent = s.remaining;
        document.getElementById('stat-total').textContent = s.total;
      })
      .getStats();
  }

  // ── Scanner ───────────────────────────────────────────────────────────

  function startScanner() {
    scanner = new Html5Qrcode('reader');
    scanner.start(
      { facingMode: 'environment' },
      { fps: 10, qrbox: { width: 250, height: 250 } },
      onScanSuccess,
      () => {} // ignore scan failures
    ).catch(err => {
      document.getElementById('scanner-area').innerHTML =
        '<div class="auth-msg error">Camera error: ' + escapeHtml(err) + '</div>';
    });
  }

  function onScanSuccess(decoded) {
    if (isProcessing) return;
    isProcessing = true;

    // Vibrate if supported
    if (navigator.vibrate) navigator.vibrate(100);

    // Pause scanner while we process
    scanner.pause(true);

    showResult('loading', 'Looking up...', '<div class="auth-msg">Searching registration...</div>');

    google.script.run
      .withSuccessHandler(onLookupResult)
      .withFailureHandler(err => {
        showResult('error', 'Lookup Failed', '<p>' + escapeHtml(err.message || err) + '</p>');
        resumeAfterDelay(3000);
      })
      .lookupAttendee(decoded);
  }

  function onLookupResult(result) {
    if (!result.found) {
      showResult('error', 'Not Found', '<p>' + escapeHtml(result.error) + '</p>');
      resumeAfterDelay(3000);
      return;
    }

    currentLookup = result;

    if (result.alreadyCheckedIn) {
      showResult('warning', 'Already Checked In', buildAttendeeCard(result, true));
      return;
    }

    showResult('success', 'Attendee Found', buildAttendeeCard(result, false));
  }

  function buildAttendeeCard(r, alreadyIn) {
    let html = '<div class="attendee-name">' + escapeHtml(r.name) + '</div>';
    html += '<div class="detail-grid">';
    html += detailBox('T-Shirt', r.shirtSize || '—', 'shirt');
    html += detailBox('Tier', r.tier || '—', '');
    if (r.special) {
      html += detailBox('Special', r.special, 'special full-width');
    }
    if (alreadyIn) {
      html += detailBox('Checked in by', r.checkedInBy, 'full-width');
      html += detailBox('At', r.checkedInTime, 'full-width');
    }
    html += '</div>';

    if (!alreadyIn) {
      html += '<button class="btn-checkin" onclick="doCheckIn()">✓ Check In</button>';
    }
    html += '<button class="btn-dismiss" onclick="dismissResult()">Scan Next</button>';

    return html;
  }

  function detailBox(label, value, extraClass) {
    return '<div class="detail ' + (extraClass || '') + '">'
      + '<div class="detail-label">' + escapeHtml(label) + '</div>'
      + '<div class="detail-value">' + escapeHtml(value || '—') + '</div>'
      + '</div>';
  }

  function doCheckIn() {
    if (!currentLookup) return;
    const btn = document.querySelector('.btn-checkin');
    btn.disabled = true;
    btn.textContent = 'Checking in...';

    google.script.run
      .withSuccessHandler(res => {
        if (res.success) {
          showResult('success', 'Checked In ✓', buildCheckedInConfirmation(currentLookup));
          refreshStats();
        } else {
          showResult('warning', 'Already Checked In', '<p>' + escapeHtml(res.error) + '</p>');
        }
      })
      .withFailureHandler(err => {
        showResult('error', 'Check-In Failed', '<p>' + escapeHtml(err.message || err) + '</p>');
      })
      .checkInAttendee(currentLookup.row);
  }

  function buildCheckedInConfirmation(r) {
    let html = '<div class="attendee-name">' + escapeHtml(r.name) + '</div>';
    html += '<div class="detail-grid">';
    html += detailBox('T-Shirt', r.shirtSize || '—', 'shirt');
    html += detailBox('Tier', r.tier || '—', '');
    if (r.special) {
      html += detailBox('Special', r.special, 'special full-width');
    }
    html += '</div>';
    html += '<button class="btn-dismiss" onclick="dismissResult()" style="margin-top:16px">Scan Next</button>';
    return html;
  }

  function dismissResult() {
    document.getElementById('result-area').innerHTML = '';
    currentLookup = null;
    isProcessing = false;
    try { scanner.resume(); } catch(e) {}
  }

  function resumeAfterDelay(ms) {
    setTimeout(() => {
      document.getElementById('result-area').innerHTML = '';
      currentLookup = null;
      isProcessing = false;
      try { scanner.resume(); } catch(e) {}
    }, ms);
  }

  // ── Helpers ───────────────────────────────────────────────────────────

  function showResult(type, status, bodyHtml) {
    const icons = { success: '✓', warning: '⚠', error: '✕', loading: '⟳' };
    document.getElementById('result-area').innerHTML =
      '<div class="result-card ' + type + '">'
      + '<div class="result-status"><span class="icon">' + (icons[type] || '') + '</span> ' + escapeHtml(status) + '</div>'
      + bodyHtml
      + '</div>';
  }

  function escapeHtml(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.appendChild(document.createTextNode(str.toString()));
    return d.innerHTML;
  }
<\/script>

</body>
</html>`;
}
