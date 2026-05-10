// ==UserScript==
// @name         OPM EHRI Auto-Downloader
// @namespace    impactmap.opm
// @version      1.0
// @description  Clicks TXT downloads on data.opm.gov one at a time with spacing. Pairs with process_local_downloads.py which processes + uploads + deletes files as they arrive, so disk usage stays bounded.
// @match        https://data.opm.gov/explore-data/data/data-downloads*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

(function () {
    'use strict';

    // Knobs — tweak in this block.
    const SPACING_MS = 10 * 60 * 1000;   // 10 minutes between downloads
    const MAX_FILES = 50;                // safety cap per run
    const POST_CLICK_WAIT_MS = 1500;     // time for the TXT dropdown to render
    const PAGE_LOAD_WAIT_MS = 3000;      // time for next page of cards to settle

    // Persistent state across page reloads (so a refresh doesn't lose progress).
    const STATE_KEY = 'opm-auto-state';
    function loadState() {
        try { return JSON.parse(localStorage.getItem(STATE_KEY)) || {}; }
        catch { return {}; }
    }
    function saveState(s) { localStorage.setItem(STATE_KEY, JSON.stringify(s)); }

    let state = loadState();
    state.downloaded = state.downloaded || 0;
    state.attempted = state.attempted || [];   // aria-label values we've clicked
    saveState(state);

    let running = false;

    function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

    function log(msg) {
        const stamp = new Date().toISOString().replace('T', ' ').slice(0, 19);
        console.log(`[OPM-auto ${stamp}] ${msg}`);
    }

    function getDownloadButtons() {
        return Array.from(document.querySelectorAll('button[aria-label^="Download options for"]'));
    }

    function getNextPageButton() {
        return document.querySelector('button[aria-label="Go to next page"]');
    }

    async function clickDownloadOn(btn) {
        const label = btn.getAttribute('aria-label');
        log(`(${state.downloaded + 1}/${MAX_FILES}) Clicking: ${label}`);

        btn.click();
        await sleep(POST_CLICK_WAIT_MS);

        let txt = document.querySelector('[aria-label*="TXT"]');
        if (!txt) {
            // The dropdown sometimes takes a beat longer to render.
            await sleep(2000);
            txt = document.querySelector('[aria-label*="TXT"]');
        }
        if (!txt) {
            log(`WARN: TXT option not found for ${label} — skipping`);
            document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));
            return false;
        }

        txt.click();
        await sleep(500);
        document.body.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' }));

        state.downloaded += 1;
        state.attempted.push(label);
        saveState(state);
        return true;
    }

    async function run() {
        if (running) { log('Already running'); return; }
        running = true;
        log(`Starting. Spacing=${SPACING_MS / 60000}min, MaxFiles=${MAX_FILES}. State: ${state.downloaded} already downloaded.`);

        while (running && state.downloaded < MAX_FILES) {
            const buttons = getDownloadButtons();
            // Pick the first button whose aria-label we haven't attempted yet.
            const target = buttons.find(b => !state.attempted.includes(b.getAttribute('aria-label')));

            if (!target) {
                // Nothing new on this page — try the next page.
                const next = getNextPageButton();
                if (!next || next.disabled || next.getAttribute('aria-disabled') === 'true') {
                    log('No more cards on any page. Done.');
                    break;
                }
                log('Advancing to next page');
                next.click();
                await sleep(PAGE_LOAD_WAIT_MS);
                continue;
            }

            const ok = await clickDownloadOn(target);
            if (!ok) {
                // skip past a stubborn card by marking it attempted
                state.attempted.push(target.getAttribute('aria-label'));
                saveState(state);
                await sleep(2000);
                continue;
            }

            log(`Sleeping ${SPACING_MS / 1000}s before next`);
            await sleep(SPACING_MS);
        }

        running = false;
        log(`DONE. Downloaded ${state.downloaded} files this session.`);
    }

    function stop() {
        running = false;
        log('Stop requested — will halt after current sleep finishes.');
    }

    function reset() {
        state = { downloaded: 0, attempted: [] };
        saveState(state);
        log('State reset.');
    }

    // Expose to the page console.
    window.OPM_AUTO_START = run;
    window.OPM_AUTO_STOP = stop;
    window.OPM_AUTO_RESET = reset;
    window.OPM_AUTO_STATE = () => ({ ...state });

    log('Loaded. Filter the UI to the data type you want (e.g. Employment), then run OPM_AUTO_START() in the console.');
})();
