/**
 * ¿Cómo Votó? - Interactive Frontend
 * ====================================
 * Features:
 *   - Search legislators by name, bloc, province
 *   - Filter by chamber, coalition, year, law name
 *   - Waffle/grid visualization grouped by law
 *   - Notable laws infographic with legislator photo
 *   - Alignment charts (line + bar)
 *   - Vote history table with pagination
 *   - Copy image / Share to Twitter
 */

// ===========================================================================
//  GLOBALS
// ===========================================================================

let legislatorsData = [];
let currentDetail = null;
let chartAlignment = null;
let chartYearly = null;
let currentVotesPage = 1;
const VOTES_PER_PAGE = 25;

const DATA_PATH = "data";

// Notable laws — these are the ones we show on the homepage infographic
const NOTABLE_LAW_KEYWORDS = [
    "Ley Bases",
    "Paquete Fiscal",
    "RIGI",
    "DNU 70/2023",
    "Reforma Laboral",
    "Financiamiento Universitario",
    "Movilidad Jubilatoria",
    "Privatizaciones",
    "Boleta Unica",
    "Ficha Limpia",
    "IVE / Aborto",
    "Presupuesto",
    "Impuesto a las Ganancias",
];

// ===========================================================================
//  INITIALIZATION
// ===========================================================================

document.addEventListener("DOMContentLoaded", () => {
    loadGlobalData();
    setupEventListeners();
});

async function loadGlobalData() {
    try {
        const [statsResp, legsResp] = await Promise.all([
            fetch(`${DATA_PATH}/stats.json`),
            fetch(`${DATA_PATH}/legislators.json`),
        ]);

        if (statsResp.ok) {
            const stats = await statsResp.json();
            renderStats(stats);
        }

        if (legsResp.ok) {
            legislatorsData = await legsResp.json();
            console.log(`Loaded ${legislatorsData.length} legislators`);
        } else {
            legislatorsData = [];
        }
    } catch (err) {
        console.error("Error loading data:", err);
    }
}

function renderStats(stats) {
    setText("stat-legislators", stats.total_legislators || 0);
    setText(
        "stat-votaciones",
        (stats.total_votaciones_diputados || 0) + (stats.total_votaciones_senadores || 0)
    );
    setText("stat-years", (stats.years_covered || []).length);
    if (stats.last_updated) {
        const d = new Date(stats.last_updated);
        setText("stat-updated", d.toLocaleDateString("es-AR"));
    }
}

// ===========================================================================
//  EVENT LISTENERS
// ===========================================================================

function setupEventListeners() {
    const searchInput = document.getElementById("search-input");
    const clearBtn = document.getElementById("clear-search");
    const filterChamber = document.getElementById("filter-chamber");
    const filterCoalition = document.getElementById("filter-coalition");
    const backBtn = document.getElementById("back-btn");

    searchInput.addEventListener("input", debounce(onSearchInput, 150));
    searchInput.addEventListener("focus", onSearchInput);
    clearBtn.addEventListener("click", () => {
        searchInput.value = "";
        hideSearchResults();
        searchInput.focus();
    });
    filterChamber.addEventListener("change", onSearchInput);
    filterCoalition.addEventListener("change", onSearchInput);
    backBtn.addEventListener("click", showSearchView);

    document.addEventListener("click", (e) => {
        const searchBox = document.querySelector(".search-box");
        if (!searchBox.contains(e.target)) {
            hideSearchResults();
        }
        // Also close notable dropdown
        const notableRow = document.querySelector(".notable-search-row");
        if (notableRow && !notableRow.contains(e.target)) {
            document.getElementById("notable-search-results").classList.add("hidden");
        }
    });

    // Vote table filters
    document.getElementById("votes-year-filter").addEventListener("change", () => { currentVotesPage = 1; renderVotesTable(); });
    document.getElementById("votes-type-filter").addEventListener("change", () => { currentVotesPage = 1; renderVotesTable(); });
    document.getElementById("votes-law-filter").addEventListener("input", debounce(() => { currentVotesPage = 1; renderVotesTable(); }, 200));

    // Waffle filters
    document.getElementById("waffle-year-filter").addEventListener("change", renderWaffle);
    document.getElementById("waffle-law-filter").addEventListener("input", debounce(renderWaffle, 200));

    // Share buttons (legislator detail waffle)
    document.getElementById("btn-copy-image").addEventListener("click", copyWaffleImage);
    document.getElementById("btn-share-tw").addEventListener("click", shareTwitter);

    // Notable laws section
    const notableSearch = document.getElementById("notable-search");
    notableSearch.addEventListener("input", debounce(onNotableSearchInput, 150));
    notableSearch.addEventListener("focus", onNotableSearchInput);

    document.getElementById("btn-copy-notable").addEventListener("click", () => copyCardImage("notable-card", "btn-copy-notable"));
    document.getElementById("btn-share-notable-tw").addEventListener("click", shareTwitterNotable);
}

// ===========================================================================
//  SEARCH
// ===========================================================================

function onSearchInput() {
    const query = document.getElementById("search-input").value.trim().toLowerCase();
    const chamber = document.getElementById("filter-chamber").value;
    const coalition = document.getElementById("filter-coalition").value;

    if (!query && !chamber && !coalition) {
        hideSearchResults();
        return;
    }

    let results = legislatorsData;

    if (chamber) {
        results = results.filter((l) => l.c.includes(chamber));
    }
    if (coalition) {
        results = results.filter((l) => l.co === coalition);
    }
    if (query) {
        const terms = query.split(/\s+/);
        results = results.filter((l) => {
            const searchable = `${l.n} ${l.b} ${l.p}`.toLowerCase();
            return terms.every((t) => searchable.includes(t));
        });
    }

    results.sort((a, b) => (b.tv || 0) - (a.tv || 0));
    results = results.slice(0, 50);

    renderSearchResults(results);
}

function renderSearchResults(results) {
    const container = document.getElementById("search-results");
    container.classList.remove("hidden");

    if (results.length === 0) {
        container.innerHTML = `<div class="search-result-item" style="justify-content:center; cursor:default; color: var(--color-text-secondary);">No se encontraron resultados</div>`;
        return;
    }

    container.innerHTML = results
        .map(
            (l) => `
        <div class="search-result-item" data-key="${l.k}">
            <div class="search-result-name">${highlightMatch(l.n)}</div>
            <div class="search-result-meta">
                ${chamberBadges(l.c)}
                <span class="badge badge-${l.co.toLowerCase()}">${l.co}</span>
                <span class="badge" style="background:#f1f5f9">${l.p || ""}</span>
            </div>
        </div>`
        )
        .join("");

    container.querySelectorAll(".search-result-item[data-key]").forEach((el) => {
        el.addEventListener("click", () => loadLegislatorDetail(el.dataset.key));
    });
}

function chamberBadges(chamberStr) {
    if (!chamberStr) return "";
    const parts = chamberStr.split("+");
    return parts.map((c) => {
        const label = c === "diputados" ? "Dip." : "Sen.";
        return `<span class="badge badge-${c}">${label}</span>`;
    }).join("");
}

function highlightMatch(name) {
    const query = document.getElementById("search-input").value.trim();
    if (!query) return escapeHtml(name);
    const regex = new RegExp(`(${escapeRegex(query)})`, "gi");
    return escapeHtml(name).replace(regex, "<strong>$1</strong>");
}

function hideSearchResults() {
    document.getElementById("search-results").classList.add("hidden");
}

// ===========================================================================
//  NOTABLE LAWS SECTION (homepage)
// ===========================================================================

function onNotableSearchInput() {
    const query = document.getElementById("notable-search").value.trim().toLowerCase();
    const dropdown = document.getElementById("notable-search-results");

    if (!query || query.length < 2) {
        dropdown.classList.add("hidden");
        return;
    }

    const terms = query.split(/\s+/);
    let results = legislatorsData.filter((l) => {
        const searchable = `${l.n} ${l.b} ${l.p}`.toLowerCase();
        return terms.every((t) => searchable.includes(t));
    });
    results.sort((a, b) => (b.tv || 0) - (a.tv || 0));
    results = results.slice(0, 20);

    if (results.length === 0) {
        dropdown.innerHTML = `<div class="notable-dropdown-item" style="cursor:default; color:var(--color-text-secondary); justify-content:center;">Sin resultados</div>`;
        dropdown.classList.remove("hidden");
        return;
    }

    dropdown.innerHTML = results.map((l) => {
        const photoHtml = l.ph
            ? `<img src="${escapeAttr(l.ph)}" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
            : "";
        return `
        <div class="notable-dropdown-item" data-key="${l.k}">
            ${photoHtml}<span class="no-photo" ${l.ph ? 'style="display:none"' : ""}>👤</span>
            <span>${escapeHtml(l.n)} <small style="color:var(--color-text-secondary)">${l.co} · ${l.p || ""}</small></span>
        </div>`;
    }).join("");

    dropdown.classList.remove("hidden");

    dropdown.querySelectorAll(".notable-dropdown-item[data-key]").forEach((el) => {
        el.addEventListener("click", () => {
            dropdown.classList.add("hidden");
            document.getElementById("notable-search").value = "";
            loadNotableCard(el.dataset.key);
        });
    });
}

async function loadNotableCard(nameKey) {
    const safeKey = nameKey.replace(/[^A-Z0-9_]/g, "_").substring(0, 80);
    const url = `${DATA_PATH}/legislators/${safeKey}.json`;

    try {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        renderNotableCard(data);
    } catch (err) {
        console.error("Error loading notable card:", err);
    }
}

function renderNotableCard(data) {
    const wrapper = document.getElementById("notable-card-wrapper");
    wrapper.classList.remove("hidden");

    const left = document.getElementById("notable-card-left");
    const right = document.getElementById("notable-card-right");

    // Left: photo + name + chamber
    const chambers = data.chambers || [data.chamber];
    const chamberLabel = chambers.length > 1 ? "HCD + HCS" : (chambers[0] === "diputados" ? "HCD" : "HCS");

    const photoHtml = data.photo
        ? `<img class="notable-photo" src="${escapeAttr(data.photo)}" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
        : "";

    left.innerHTML = `
        ${photoHtml}
        <div class="no-photo-large" ${data.photo ? 'style="display:none"' : ""}>👤</div>
        <div class="notable-name">${escapeHtml(data.name)}</div>
        <div class="notable-chamber">${chamberLabel}</div>
    `;

    // Right: waffle grids for notable laws
    const laws = data.laws || [];

    // Find laws matching notable keywords
    const notableLaws = [];
    for (const keyword of NOTABLE_LAW_KEYWORDS) {
        const kw = keyword.toLowerCase();
        const match = laws.find((l) => l.name && l.name.toLowerCase().includes(kw));
        if (match) {
            notableLaws.push({ ...match, displayName: keyword });
        }
    }

    if (notableLaws.length === 0) {
        right.innerHTML = `<div class="notable-no-data">Este legislador no tiene votos registrados en las leyes destacadas.</div>`;
    } else {
        right.innerHTML = notableLaws.map((law) => {
            const tiles = law.votes.map((vote) => {
                const cls = `waffle-tile tile-${vote.v}`;
                const icon = voteIcon(vote.v);
                const tooltip = vote.al ? `${vote.al}: ${formatVoteShort(vote.v)}` : formatVoteShort(vote.v);
                return `<div class="${cls}" title="${escapeAttr(tooltip)}">${icon}</div>`;
            }).join("");

            return `
            <div class="notable-law-block">
                <div class="notable-law-title">${escapeHtml(law.displayName)}</div>
                <div class="notable-law-tiles">${tiles}</div>
            </div>`;
        }).join("");
    }

    // Scroll into view
    wrapper.scrollIntoView({ behavior: "smooth", block: "start" });
}

function shareTwitterNotable() {
    const nameEl = document.querySelector("#notable-card-left .notable-name");
    const name = nameEl ? nameEl.textContent : "un legislador";
    const text = `Mirá cómo votó ${name} las leyes más importantes en el Congreso Argentino 🗳️\n\n¿Cómo Votó? - comovoto.ar`;
    const url = encodeURIComponent(window.location.href);
    const tweetUrl = `https://twitter.com/intent/tweet?text=${encodeURIComponent(text)}&url=${url}`;
    window.open(tweetUrl, "_blank", "width=600,height=400");
}

// ===========================================================================
//  LEGISLATOR DETAIL
// ===========================================================================

async function loadLegislatorDetail(nameKey) {
    hideSearchResults();

    const detailSection = document.getElementById("legislator-detail");
    detailSection.classList.remove("hidden");
    document.querySelector(".search-section").classList.add("hidden");
    document.getElementById("stats-bar").classList.add("hidden");
    document.getElementById("notable-laws-section").classList.add("hidden");

    const safeKey = nameKey.replace(/[^A-Z0-9_]/g, "_").substring(0, 80);
    const url = `${DATA_PATH}/legislators/${safeKey}.json`;

    try {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        currentDetail = await resp.json();
        renderLegislatorDetail(currentDetail);
    } catch (err) {
        console.error("Error loading legislator:", err);
        document.getElementById("leg-name").textContent = "Error al cargar datos";
    }

    window.scrollTo({ top: 0, behavior: "smooth" });
}

function renderLegislatorDetail(data) {
    // Header with photo
    document.getElementById("leg-name").textContent = data.name;

    // Photo
    const photoEl = document.getElementById("leg-photo");
    if (data.photo) {
        photoEl.src = data.photo;
        photoEl.alt = data.name;
        photoEl.style.display = "block";
        photoEl.onerror = () => { photoEl.style.display = "none"; };
    } else {
        photoEl.style.display = "none";
    }

    const chamberBadge = document.getElementById("leg-chamber");
    const chambers = data.chambers || [data.chamber];
    if (chambers.length > 1) {
        chamberBadge.textContent = "Dip. + Sen.";
        chamberBadge.className = "leg-chamber badge badge-both";
    } else {
        chamberBadge.textContent = chambers[0] === "diputados" ? "Diputado/a" : "Senador/a";
        chamberBadge.className = `leg-chamber badge badge-${chambers[0]}`;
    }

    const blocBadge = document.getElementById("leg-bloc");
    blocBadge.textContent = data.bloc;
    blocBadge.className = `leg-bloc badge badge-${data.coalition.toLowerCase()}`;

    document.getElementById("leg-province").textContent = data.province;

    // Alignment summary cards
    const alignSummary = document.getElementById("leg-alignment-summary");
    alignSummary.innerHTML = "";

    const coalitions = [
        { key: "PJ", label: "PJ / UxP", cls: "alignment-pj" },
        { key: "PRO", label: "PRO / JxC", cls: "alignment-pro" },
        { key: "LLA", label: "LLA", cls: "alignment-lla" },
    ];

    for (const c of coalitions) {
        const val = data.alignment[c.key];
        const card = document.createElement("div");
        card.className = `alignment-card ${c.cls}`;
        card.innerHTML = `
            <div class="alignment-label">${c.label}</div>
            <div class="alignment-value">${val !== null ? val + "%" : "N/A"}</div>
        `;
        alignSummary.appendChild(card);
    }

    // Waffle card header
    document.getElementById("waffle-card-name").textContent = data.name;
    const chamberLabel = chambers.length > 1 ? "HCD + HCS" : (chambers[0] === "diputados" ? "HCD" : "HCS");
    document.getElementById("waffle-card-meta").innerHTML = `
        <span class="badge badge-${chambers[0]}">${chamberLabel}</span>
        <span class="badge badge-${data.coalition.toLowerCase()}">${data.bloc}</span>
    `;

    // Populate waffle year filter
    const waffleYearFilter = document.getElementById("waffle-year-filter");
    waffleYearFilter.innerHTML = '<option value="">Todos</option>';
    const years = Object.keys(data.yearly_stats).sort();
    for (const y of years) {
        waffleYearFilter.innerHTML += `<option value="${y}">${y}</option>`;
    }

    // Reset waffle law filter
    document.getElementById("waffle-law-filter").value = "";

    // Render waffle
    renderWaffle();

    // Charts
    renderAlignmentChart(data);
    renderYearlyChart(data);

    // Populate votes year filter
    const yearFilter = document.getElementById("votes-year-filter");
    yearFilter.innerHTML = '<option value="">Todos los años</option>';
    for (const y of years) {
        yearFilter.innerHTML += `<option value="${y}">${y}</option>`;
    }
    document.getElementById("votes-type-filter").value = "";
    document.getElementById("votes-law-filter").value = "";

    currentVotesPage = 1;
    renderVotesTable();
}

function showSearchView() {
    document.getElementById("legislator-detail").classList.add("hidden");
    document.querySelector(".search-section").classList.remove("hidden");
    document.getElementById("stats-bar").classList.remove("hidden");
    document.getElementById("notable-laws-section").classList.remove("hidden");

    if (chartAlignment) { chartAlignment.destroy(); chartAlignment = null; }
    if (chartYearly) { chartYearly.destroy(); chartYearly = null; }
    currentDetail = null;
}

// ===========================================================================
//  WAFFLE VISUALIZATION
// ===========================================================================

function renderWaffle() {
    if (!currentDetail) return;

    const yearFilter = document.getElementById("waffle-year-filter").value;
    const lawFilter = document.getElementById("waffle-law-filter").value.trim().toLowerCase();

    let laws = currentDetail.laws || [];

    // Apply filters
    if (yearFilter) {
        laws = laws.filter((l) => String(l.year) === yearFilter);
    }
    if (lawFilter) {
        laws = laws.filter((l) => l.name.toLowerCase().includes(lawFilter));
    }

    const body = document.getElementById("waffle-card-body");

    if (laws.length === 0) {
        body.innerHTML = '<div class="waffle-empty">No hay votaciones para los filtros seleccionados</div>';
        return;
    }

    // Render law rows
    let html = "";
    for (const law of laws) {
        const tiles = law.votes.map((vote) => {
            const cls = `waffle-tile tile-${vote.v}`;
            const icon = voteIcon(vote.v);
            const tooltip = vote.al ? `${vote.al}: ${formatVoteShort(vote.v)}` : formatVoteShort(vote.v);
            return `<div class="${cls}" title="${escapeAttr(tooltip)}">${icon}</div>`;
        }).join("");

        const displayName = escapeHtml(truncate(law.name, 60));
        const yearLabel = law.year ? `<span class="waffle-law-year">${law.year}</span>` : "";

        html += `
        <div class="waffle-law-row">
            <div class="waffle-law-label">
                <span class="waffle-law-name">${displayName}</span>
                ${yearLabel}
            </div>
            <div class="waffle-tiles">${tiles}</div>
        </div>`;
    }

    body.innerHTML = html;
}

function voteIcon(vote) {
    switch (vote) {
        case "AFIRMATIVO": return "✓";
        case "NEGATIVO": return "✗";
        case "ABSTENCION": return "○";
        case "AUSENTE": return "—";
        case "PRESIDENTE": return "⚑";
        default: return "?";
    }
}

// ===========================================================================
//  SHARE / EXPORT
// ===========================================================================

async function copyCardImage(cardId, btnId) {
    const card = document.getElementById(cardId);
    const btn = document.getElementById(btnId);
    const originalText = btn.innerHTML;

    try {
        btn.innerHTML = "⏳ Generando...";
        btn.disabled = true;

        const canvas = await html2canvas(card, {
            backgroundColor: "#ffffff",
            scale: 2,
            useCORS: true,
            logging: false,
        });

        canvas.toBlob(async (blob) => {
            try {
                await navigator.clipboard.write([
                    new ClipboardItem({ "image/png": blob }),
                ]);
                btn.innerHTML = "✓ Copiado!";
                setTimeout(() => { btn.innerHTML = originalText; btn.disabled = false; }, 2000);
            } catch (e) {
                // Fallback: download the image
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = `como_voto_${cardId}.png`;
                a.click();
                URL.revokeObjectURL(url);
                btn.innerHTML = "✓ Descargado!";
                setTimeout(() => { btn.innerHTML = originalText; btn.disabled = false; }, 2000);
            }
        }, "image/png");
    } catch (err) {
        console.error("Error generating image:", err);
        btn.innerHTML = "Error :(";
        setTimeout(() => { btn.innerHTML = originalText; btn.disabled = false; }, 2000);
    }
}

async function copyWaffleImage() {
    await copyCardImage("waffle-card", "btn-copy-image");
}

function shareTwitter() {
    if (!currentDetail) return;
    const name = currentDetail.name;
    const text = `Mirá cómo votó ${name} en el Congreso Argentino 🗳️\n\n¿Cómo Votó? - comovoto.ar`;
    const url = encodeURIComponent(window.location.href);
    const tweetUrl = `https://twitter.com/intent/tweet?text=${encodeURIComponent(text)}&url=${url}`;
    window.open(tweetUrl, "_blank", "width=600,height=400");
}

// ===========================================================================
//  CHARTS
// ===========================================================================

function renderAlignmentChart(data) {
    if (chartAlignment) chartAlignment.destroy();

    const ctx = document.getElementById("chart-alignment").getContext("2d");
    const years = Object.keys(data.yearly_alignment).sort();

    if (years.length === 0) {
        ctx.font = "14px Inter";
        ctx.fillStyle = "#6b7280";
        ctx.textAlign = "center";
        ctx.fillText("Sin datos de alineamiento", ctx.canvas.width / 2, ctx.canvas.height / 2);
        return;
    }

    const pjData = years.map((y) => data.yearly_alignment[y]?.PJ ?? null);
    const proData = years.map((y) => data.yearly_alignment[y]?.PRO ?? null);
    const llaData = years.map((y) => data.yearly_alignment[y]?.LLA ?? null);

    chartAlignment = new Chart(ctx, {
        type: "line",
        data: {
            labels: years,
            datasets: [
                {
                    label: "PJ / Unión por la Patria",
                    data: pjData,
                    borderColor: "#1e88e5",
                    backgroundColor: "rgba(30, 136, 229, 0.08)",
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                },
                {
                    label: "PRO / JxC / UCR",
                    data: proData,
                    borderColor: "#f9a825",
                    backgroundColor: "rgba(249, 168, 37, 0.08)",
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                },
                {
                    label: "La Libertad Avanza",
                    data: llaData,
                    borderColor: "#7b1fa2",
                    backgroundColor: "rgba(123, 31, 162, 0.08)",
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    tension: 0.3,
                    fill: false,
                    spanGaps: true,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: "bottom",
                    labels: { usePointStyle: true, padding: 15, font: { size: 12 } },
                },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.y !== null ? ctx.parsed.y + "%" : "N/A"}`,
                    },
                },
            },
            scales: {
                y: {
                    min: 0,
                    max: 100,
                    ticks: {
                        callback: (v) => v + "%",
                        font: { size: 11 },
                    },
                    grid: { color: "rgba(0,0,0,0.05)" },
                },
                x: {
                    ticks: { font: { size: 11 } },
                    grid: { display: false },
                },
            },
            interaction: { mode: "index", intersect: false },
        },
    });
}

function renderYearlyChart(data) {
    if (chartYearly) chartYearly.destroy();

    const ctx = document.getElementById("chart-yearly").getContext("2d");
    const years = Object.keys(data.yearly_stats).sort();

    if (years.length === 0) {
        ctx.font = "14px Inter";
        ctx.fillStyle = "#6b7280";
        ctx.textAlign = "center";
        ctx.fillText("Sin datos", ctx.canvas.width / 2, ctx.canvas.height / 2);
        return;
    }

    chartYearly = new Chart(ctx, {
        type: "bar",
        data: {
            labels: years,
            datasets: [
                {
                    label: "Afirmativo",
                    data: years.map((y) => data.yearly_stats[y]?.AFIRMATIVO || 0),
                    backgroundColor: "#22c55e",
                },
                {
                    label: "Negativo",
                    data: years.map((y) => data.yearly_stats[y]?.NEGATIVO || 0),
                    backgroundColor: "#ef4444",
                },
                {
                    label: "Abstención",
                    data: years.map((y) => data.yearly_stats[y]?.ABSTENCION || 0),
                    backgroundColor: "#f59e0b",
                },
                {
                    label: "Ausente",
                    data: years.map((y) => data.yearly_stats[y]?.AUSENTE || 0),
                    backgroundColor: "#94a3b8",
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: "bottom",
                    labels: { usePointStyle: true, padding: 15, font: { size: 12 } },
                },
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { font: { size: 11 } },
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    ticks: { font: { size: 11 } },
                    grid: { color: "rgba(0,0,0,0.05)" },
                },
            },
        },
    });
}

// ===========================================================================
//  VOTES TABLE
// ===========================================================================

function renderVotesTable() {
    if (!currentDetail) return;

    const yearFilter = document.getElementById("votes-year-filter").value;
    const typeFilter = document.getElementById("votes-type-filter").value;
    const lawFilter = document.getElementById("votes-law-filter").value.trim().toLowerCase();

    let votes = currentDetail.votes || [];

    if (yearFilter) {
        votes = votes.filter((v) => String(v.yr) === yearFilter);
    }
    if (typeFilter) {
        votes = votes.filter((v) => v.v === typeFilter);
    }
    if (lawFilter) {
        votes = votes.filter((v) => {
            const searchable = `${v.ln || ""} ${v.t || ""}`.toLowerCase();
            return searchable.includes(lawFilter);
        });
    }

    // Sort by date descending
    votes.sort((a, b) => {
        const da = parseArgDate(a.d);
        const db = parseArgDate(b.d);
        return db - da;
    });

    // Pagination
    const totalPages = Math.max(1, Math.ceil(votes.length / VOTES_PER_PAGE));
    if (currentVotesPage > totalPages) currentVotesPage = totalPages;
    const start = (currentVotesPage - 1) * VOTES_PER_PAGE;
    const pageVotes = votes.slice(start, start + VOTES_PER_PAGE);

    const tbody = document.getElementById("votes-tbody");
    tbody.innerHTML = pageVotes
        .map(
            (v) => `
        <tr>
            <td style="white-space:nowrap">${escapeHtml(v.d || "")}</td>
            <td>
                <div class="vote-title">${escapeHtml(v.ln || v.t || "")}</div>
                ${v.al ? `<div class="vote-article">${escapeHtml(v.al)}</div>` : ""}
            </td>
            <td><span class="vote-chip vote-${v.v}">${formatVote(v.v)}</span></td>
            <td><span class="vote-chip vote-${v.pj}">${formatVote(v.pj)}</span></td>
            <td><span class="vote-chip vote-${v.pro}">${formatVote(v.pro)}</span></td>
            <td><span class="vote-chip vote-${v.lla}">${formatVote(v.lla)}</span></td>
        </tr>`
        )
        .join("");

    renderPagination(totalPages, votes.length);
}

function renderPagination(totalPages, totalItems) {
    const container = document.getElementById("votes-pagination");
    if (totalPages <= 1) {
        container.innerHTML = `<span style="font-size:0.8rem;color:var(--color-text-secondary)">${totalItems} votaciones</span>`;
        return;
    }

    let html = "";

    if (currentVotesPage > 1) {
        html += `<button data-page="${currentVotesPage - 1}">← Ant.</button>`;
    }

    for (let p = 1; p <= totalPages; p++) {
        if (p === 1 || p === totalPages || Math.abs(p - currentVotesPage) <= 2) {
            html += `<button data-page="${p}" class="${p === currentVotesPage ? "active" : ""}">${p}</button>`;
        } else if (Math.abs(p - currentVotesPage) === 3) {
            html += `<span style="padding:0.4rem;color:var(--color-text-secondary)">…</span>`;
        }
    }

    if (currentVotesPage < totalPages) {
        html += `<button data-page="${currentVotesPage + 1}">Sig. →</button>`;
    }

    html += `<span style="font-size:0.75rem;color:var(--color-text-secondary);margin-left:0.5rem">${totalItems} votaciones</span>`;

    container.innerHTML = html;

    container.querySelectorAll("button[data-page]").forEach((btn) => {
        btn.addEventListener("click", () => {
            currentVotesPage = parseInt(btn.dataset.page);
            renderVotesTable();
            document.querySelector(".votes-section").scrollIntoView({ behavior: "smooth" });
        });
    });
}

// ===========================================================================
//  UTILITIES
// ===========================================================================

function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function escapeHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function escapeAttr(str) {
    return str.replace(/"/g, "&quot;").replace(/'/g, "&#39;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function truncate(str, maxLen) {
    if (!str) return "";
    return str.length > maxLen ? str.substring(0, maxLen - 1) + "…" : str;
}

function debounce(fn, ms) {
    let timeout;
    return function (...args) {
        clearTimeout(timeout);
        timeout = setTimeout(() => fn.apply(this, args), ms);
    };
}

function formatVote(v) {
    const map = {
        AFIRMATIVO: "✓ Afirm.",
        NEGATIVO: "✗ Neg.",
        ABSTENCION: "○ Abst.",
        AUSENTE: "— Aus.",
        PRESIDENTE: "⚑ Pres.",
        "N/A": "—",
    };
    return map[v] || v || "—";
}

function formatVoteShort(v) {
    const map = {
        AFIRMATIVO: "Afirmativo",
        NEGATIVO: "Negativo",
        ABSTENCION: "Abstención",
        AUSENTE: "Ausente",
        PRESIDENTE: "Presidente",
    };
    return map[v] || v || "";
}

function parseArgDate(dateStr) {
    if (!dateStr) return 0;
    const match = dateStr.match(/(\d{2})\/(\d{2})\/(\d{4})/);
    if (match) {
        return new Date(
            parseInt(match[3]),
            parseInt(match[2]) - 1,
            parseInt(match[1])
        ).getTime();
    }
    return new Date(dateStr).getTime() || 0;
}
