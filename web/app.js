(function () {
"use strict";

// ---------- state ----------

const state = {
  view: "nation",       // 'nation' | 'state'
  stateFips: null,      // 2-digit
  countyFips: null,     // 5-digit, selected for the detail inset
  monthIndex: 0,
  colorMode: "national", // 'national' | 'state' - state view only; nation view is always national
};

let data = null;        // { months, counties, zhvi, income }
let rawTopo = null;     // raw TopoJSON topology (needed for mesh())
let nationGeo = null;   // GeoJSON FeatureCollection, all counties
let stateGeo = null;    // GeoJSON FeatureCollection, all states
let byFips = new Map(); // fips -> { county, index, geo } - only counties Zillow covers
let countyNameByFips = new Map(); // fips -> {name, state} - every county in the map, including ones Zillow doesn't cover
let statesByFips = new Map(); // 2-digit fips -> { abbr, geo }
let colorScale = null;         // fixed national color scale - the main map always uses this
let stateColorScaleCache = new Map(); // stateFips -> its own color scale, built on first use
let countyPathSel = null; // d3 selection of the currently-drawn county <path> elements
let colorModeStateFips = null; // which state colorMode currently applies to (reset on state change)

// ---------- boot ----------

Promise.all([
  fetch("data/dataset.json").then((r) => r.json()),
  fetch("data/counties-10m.json").then((r) => r.json()),
]).then(([ds, topo]) => {
  data = ds;
  rawTopo = topo;
  nationGeo = topojson.feature(topo, topo.objects.counties);
  stateGeo = topojson.feature(topo, topo.objects.states);

  data.counties.forEach((c, i) => byFips.set(c.fips, { county: c, index: i }));
  nationGeo.features.forEach((f) => {
    const entry = byFips.get(f.id);
    if (entry) entry.geo = f;
  });
  stateGeo.features.forEach((f) => {
    // topojson state ids are 2-digit FIPS; grab an abbr from any county in it
    const anyCounty = data.counties.find((c) => c.fips.slice(0, 2) === f.id);
    const fullName = f.properties && f.properties.name ? f.properties.name : f.id;
    statesByFips.set(f.id, { abbr: anyCounty ? anyCounty.state : f.id, name: fullName, geo: f });
  });
  // The map (TopoJSON) has every US county; Zillow's dataset - and so
  // byFips - only has the ~3,071 it reports ZHVI for. A handful of small
  // counties (e.g. Jackson County, CO) are on the map but have no Zillow
  // data at all, so byFips has nothing for them. This fallback name/state
  // lookup covers every county on the map, so those still display
  // sensibly instead of silently failing wherever byFips is checked.
  nationGeo.features.forEach((f) => {
    if (!countyNameByFips.has(f.id)) {
      const stateAbbr = statesByFips.get(f.id.slice(0, 2));
      countyNameByFips.set(f.id, {
        name: f.properties && f.properties.name ? f.properties.name : f.id,
        state: stateAbbr ? stateAbbr.abbr : f.id.slice(0, 2),
      });
    }
  });

  colorScale = buildColorScale();

  const slider = document.getElementById("timeline-slider");
  slider.max = data.months.length - 1;
  // Default to the most recent month with *any* income coverage, not
  // Zillow's own latest month - FRED's annual income data lags Zillow's
  // monthly data by a year or more, so the very latest months would
  // otherwise render as an all-gray "no data" map on first load.
  state.monthIndex = latestMonthWithIncomeData();
  slider.value = state.monthIndex;

  renderLegendSwatch();
  renderTimelineScale();
  wireControls();
  applyHash(false);
  render();
});

// ---------- data helpers ----------

function ratioFor(fips, monthIndex) {
  const entry = byFips.get(fips);
  if (!entry) return null;
  const zhvi = data.zhvi[entry.index][monthIndex];
  if (zhvi == null) return null;
  const income = data.income[entry.index][monthIndex];
  if (income == null) return null;
  return zhvi / income;
}

function latestMonthWithIncomeData() {
  for (let mi = data.months.length - 1; mi >= 0; mi--) {
    const hasAny = data.income.some((row) => row[mi] != null);
    if (hasAny) return mi;
  }
  return data.months.length - 1;
}

// Samples ratios across whatever counties `fipsFilter` selects (2nd/98th
// percentile of the sample becomes the scale's domain) so a few outlier
// counties don't wash out the color range for everyone else.
function sampleRatios(fipsFilter) {
  const counties = data.counties.filter(fipsFilter);
  const step = Math.max(1, Math.floor(counties.length / 400)); // subsample if there are many
  const samples = [];
  for (let ci = 0; ci < counties.length; ci += step) {
    const fips = counties[ci].fips;
    for (let mi = 0; mi < data.months.length; mi += 6) {
      const r = ratioFor(fips, mi);
      if (r != null && isFinite(r)) samples.push(r);
    }
  }
  return samples;
}

function scaleFromSamples(samples, interpolator) {
  samples = samples.slice().sort((a, b) => a - b);
  const lo = samples.length ? samples[Math.floor(samples.length * 0.02)] : 0;
  const hi = samples.length ? samples[Math.floor(samples.length * 0.98)] : 1;
  return d3.scaleSequential(interpolator).domain([lo, hi]).clamp(true);
}

function buildColorScale() {
  return scaleFromSamples(sampleRatios(() => true), d3.interpolateBlues);
}

// A state's own counties can have far less ratio spread than the whole
// country, so the fixed national scale can leave a low-variation state
// (e.g. Kansas) looking nearly uniform. This builds (and caches) a scale
// from just that state's own counties, in a visually distinct color
// scheme so it's never mistaken for the national one.
function getStateColorScale(stateFips) {
  if (!stateColorScaleCache.has(stateFips)) {
    const samples = sampleRatios((c) => c.fips.slice(0, 2) === stateFips);
    stateColorScaleCache.set(stateFips, scaleFromSamples(samples, d3.interpolateOranges));
  }
  return stateColorScaleCache.get(stateFips);
}

// The scale actually used to color the map right now: the state view's own
// scale only when the user has toggled it on for the current state; the
// national map, and state view by default, always use the fixed national
// scale (per requirement - only the state-view toggle can opt into the
// state-relative scale).
function activeColorScale() {
  if (state.view === "state" && state.colorMode === "state") {
    return getStateColorScale(state.stateFips);
  }
  return colorScale;
}

function renderLegendSwatch() {
  const scale = activeColorScale();
  const [lo, hi] = scale.domain();

  const svg = d3.select("#legend-swatch");
  svg.selectAll("*").remove();
  // The <svg> itself stretches via CSS (width: 100%), but a shape drawn
  // inside it needs its own width in the same terms - a hardcoded pixel
  // width here would just leave the rest of the (now wider) box blank.
  const defs = svg.append("defs");
  const gradId = "legend-gradient";
  const grad = defs.append("linearGradient").attr("id", gradId);
  const stops = 10;
  for (let i = 0; i <= stops; i++) {
    const t = i / stops;
    grad
      .append("stop")
      .attr("offset", `${t * 100}%`)
      .attr("stop-color", scale(lo + t * (hi - lo)));
  }
  svg.append("rect").attr("width", "100%").attr("height", 14).attr("fill", `url(#${gradId})`);
  document.getElementById("legend-min").textContent = lo.toFixed(1) + "x";
  document.getElementById("legend-max").textContent = hi.toFixed(1) + "x";

  const isStateScale = state.view === "state" && state.colorMode === "state";
  document.getElementById("legend-title").textContent =
    "Price / Income ratio" + (isStateScale ? " — this state" : "");

  const legendEl = document.getElementById("map-legend");
  legendEl.classList.toggle("clickable", state.view === "state");

  const toggleHint = document.getElementById("legend-toggle-hint");
  toggleHint.hidden = state.view !== "state";
  if (state.view === "state") {
    toggleHint.textContent = isStateScale
      ? "Colored by this state's own range — click for national scale"
      : "Colored by the national range — click for this state's own scale";
  }
}

function toggleColorMode() {
  if (state.view !== "state") return;
  state.colorMode = state.colorMode === "state" ? "national" : "state";
  renderLegendSwatch();
  updateMapColors();
}

// ---------- routing (hash-based, safe for static S3 hosting) ----------

function currentHash() {
  let h = "#/nation/" + state.monthIndex;
  if (state.view === "state") {
    h = "#/state/" + state.stateFips + "/" + state.monthIndex;
    if (state.countyFips) h += "/county/" + state.countyFips;
  }
  return h;
}

function pushHash() {
  const h = currentHash();
  if (location.hash !== h) location.hash = h;
}

function applyHash(fromEvent) {
  const parts = location.hash.replace(/^#\//, "").split("/");
  if (parts[0] === "state" && parts[1]) {
    state.view = "state";
    state.stateFips = parts[1];
    if (parts[2] != null && !isNaN(+parts[2])) state.monthIndex = +parts[2];
    if (parts[3] === "county" && parts[4]) state.countyFips = parts[4];
    else state.countyFips = null;
  } else {
    state.view = "nation";
    state.stateFips = null;
    state.countyFips = null;
    if (parts[1] != null && !isNaN(+parts[1])) state.monthIndex = +parts[1];
  }
  if (fromEvent) render();
}

window.addEventListener("hashchange", () => applyHash(true));

// ---------- controls ----------

function wireControls() {
  const slider = document.getElementById("timeline-slider");
  slider.addEventListener("input", () => {
    state.monthIndex = +slider.value;
    onTimelineChange();
  });

  document.getElementById("play-toggle").addEventListener("click", togglePlay);

  document.getElementById("county-search").addEventListener("input", (e) => {
    renderCountyList(e.target.value.trim().toLowerCase());
  });

  document.getElementById("map-legend").addEventListener("click", toggleColorMode);

  wireCollapsibleLegends();
}

// Lets the floating legend/overview/sources boxes over the map be
// collapsed down to just their title - they can crowd a small screen.
// Remembered per box (by element id) in localStorage so a collapsed box
// stays collapsed across reloads.
function wireCollapsibleLegends() {
  document.querySelectorAll(".collapsible").forEach((box) => {
    const toggle = box.querySelector(".legend-collapse-toggle");
    if (!toggle) return;

    let collapsed = false;
    try {
      collapsed = localStorage.getItem("legendCollapsed:" + box.id) === "1";
    } catch (e) {
      // localStorage unavailable (private browsing etc.) - just default open
    }
    setLegendCollapsed(box, toggle, collapsed);

    toggle.addEventListener("click", (event) => {
      // #map-legend itself has a click handler (color-scale toggle) -
      // don't let that also fire when the user meant to collapse it.
      event.stopPropagation();
      const next = !box.classList.contains("collapsed");
      setLegendCollapsed(box, toggle, next);
      try {
        localStorage.setItem("legendCollapsed:" + box.id, next ? "1" : "0");
      } catch (e) {
        // ignore - collapse still works for this session, just won't persist
      }
    });
  });
}

function setLegendCollapsed(box, toggle, collapsed) {
  box.classList.toggle("collapsed", collapsed);
  toggle.textContent = collapsed ? "+" : "−";
  toggle.setAttribute("aria-label", collapsed ? "Expand" : "Collapse");
}

// Shared by the slider's own input handler and the play-loop timer: keeps
// the map, county list (its no-data graying is month-dependent), and the
// right-hand panel(s) - the state summary is always visible in state view,
// plus the county detail marker if a county is also selected - in sync
// with the current month.
//
// `fast` (set only by the animated play loop) skips per-element tooltip
// text and the list/summary panel rebuilds - real DOM work that isn't the
// point of focus while the map is animating, and that otherwise competes
// with the browser's paint budget for the map itself. Those panels catch
// up on the next non-fast update (manual scrub, or play being paused).
function onTimelineChange(fast) {
  updateTimelineLabel();
  updateMapColors(fast);
  if (state.view === "nation") {
    renderNationalSummary();
  } else {
    const searchTerm = document.getElementById("county-search").value.trim().toLowerCase();
    renderCountyList(searchTerm);
    renderStateSummary();
    if (state.countyFips) renderDetailMarker();
  }
  // Skip URL churn while animating; the final month gets pushed once play
  // pauses (see togglePlay).
  if (!fast) pushHash();
}

const PLAY_INTERVAL_MS = 100;
const PLAY_END_PAUSE_MS = 5000;
let playing = false;
let playLastAdvance = 0;
let playHoldUntil = 0; // 0 = not holding; else a timestamp - reaching the last month pauses here before recycling to the start

// Paced by requestAnimationFrame, not setTimeout/setInterval on a fixed
// clock. Measured that even a "cheap" (~10ms JS) color update can be
// followed by a much more expensive browser paint/composite of ~3,000+
// SVG paths - work our own JS timing never sees, since it happens after
// the script yields. A fixed-delay timer has no way to know the browser
// is still busy painting the previous frame, so it keeps requesting new
// state anyway; the browser then has to coalesce/skip paints to catch up,
// which is what showed up as "slow, then jumps 2-3 months at once".
// rAF only calls back once the browser is actually ready to paint a new
// frame, so we can never get further ahead than real paint capacity - at
// worst playback runs slower than PLAY_INTERVAL_MS, which reads as
// smooth-but-slower rather than bursty.
function playFrame(now) {
  if (!playing) return;

  const atEnd = state.monthIndex === data.months.length - 1;
  if (atEnd) {
    if (playHoldUntil === 0) playHoldUntil = now + PLAY_END_PAUSE_MS; // just arrived - start the pause
    if (now < playHoldUntil) {
      requestAnimationFrame(playFrame);
      return;
    }
  }

  if (now - playLastAdvance >= PLAY_INTERVAL_MS) {
    playLastAdvance = now;
    playHoldUntil = 0;
    let next = state.monthIndex + 1;
    if (next > data.months.length - 1) next = 0;
    state.monthIndex = next;
    document.getElementById("timeline-slider").value = next;
    onTimelineChange(true);
  }
  requestAnimationFrame(playFrame);
}

function togglePlay() {
  const btn = document.getElementById("play-toggle");
  playing = !playing;
  btn.textContent = playing ? "⏸" : "▶";
  if (playing) {
    playLastAdvance = 0;
    if (state.monthIndex === data.months.length - 1) {
      // Starting play from the end should jump straight to the start,
      // not sit through the end-of-range pause first.
      state.monthIndex = 0;
      document.getElementById("timeline-slider").value = 0;
      onTimelineChange(true);
    }
    requestAnimationFrame(playFrame);
  } else {
    playHoldUntil = 0; // restart the full end-of-range pause if resumed later
    // Play skipped tooltips/list/summary updates each tick - catch them
    // up now that we've landed on a final month.
    onTimelineChange(false);
  }
}

function updateTimelineLabel() {
  const [y, m] = data.months[state.monthIndex].split("-");
  const label = new Date(+y, +m - 1, 1).toLocaleDateString(undefined, {
    month: "short",
    year: "numeric",
  });
  document.getElementById("timeline-label").textContent = label;
}

// ---------- render orchestration ----------

function render() {
  document.getElementById("timeline-slider").value = state.monthIndex;
  updateTimelineLabel();
  renderBreadcrumb();

  // The state color-scale toggle is per-state, transient UI state - reset
  // to the national scale whenever landing on a different state (or
  // leaving state view) so it never silently carries over and confuses a
  // freshly-opened state.
  if (state.stateFips !== colorModeStateFips) {
    state.colorMode = "national";
    colorModeStateFips = state.stateFips;
  }

  const listPanel = document.getElementById("county-list-panel");
  const detailPanel = document.getElementById("detail-panel");
  const countyDetailEl = document.getElementById("county-detail");

  if (state.view === "nation") {
    listPanel.hidden = true;
    detailPanel.hidden = true;
    document.getElementById("crumb-title").textContent = "United States";
    document.getElementById("national-summary").hidden = false;
    document.getElementById("nation-hint").hidden = false;
    renderMap();
    renderNationalSummary();
  } else {
    document.getElementById("national-summary").hidden = true;
    document.getElementById("nation-hint").hidden = true;
    listPanel.hidden = false;
    detailPanel.hidden = false;
    document.getElementById("crumb-title").textContent = stateFullName(state.stateFips);
    renderCountyList("");
    renderMap();

    // The state summary stays visible even with a county selected, per
    // request - the divider and hint text just delineate the two sections.
    const hasCounty = !!state.countyFips;
    document.getElementById("detail-divider").hidden = !hasCounty;
    document.getElementById("state-summary-hint").hidden = hasCounty;
    countyDetailEl.hidden = !hasCounty;
    if (hasCounty) renderDetail();
    renderStateSummary();
  }
}

function stateName(fips) {
  const s = statesByFips.get(fips);
  return s ? s.abbr : fips;
}

function stateFullName(fips) {
  const s = statesByFips.get(fips);
  return s ? s.name : fips;
}

function renderBreadcrumb() {
  const el = document.getElementById("breadcrumb");
  el.innerHTML = "";
  const nationLink = document.createElement("a");
  nationLink.textContent = "United States";
  nationLink.addEventListener("click", () => {
    state.view = "nation";
    state.stateFips = null;
    state.countyFips = null;
    pushHash();
    render();
  });
  el.appendChild(nationLink);

  if (state.view === "state") {
    el.appendChild(document.createTextNode(" › "));
    const stateSpan = document.createElement("a");
    stateSpan.textContent = stateName(state.stateFips);
    stateSpan.addEventListener("click", () => {
      state.countyFips = null;
      pushHash();
      render();
    });
    el.appendChild(stateSpan);

    if (state.countyFips) {
      el.appendChild(document.createTextNode(" › "));
      const span = document.createElement("span");
      span.textContent = countyDisplayInfo(state.countyFips).name;
      el.appendChild(span);
    }
  }
}

// ---------- map ----------

// True if this feature set has points on both sides of the antimeridian
// (longitude both > 150 and < -150) - a specific enough heuristic that no
// other US state's geometry can trigger it, only Alaska's.
function crossesAntimeridian(features) {
  let hasFarEast = false;
  let hasFarWest = false;
  const check = (node) => {
    if (typeof node[0] === "number") {
      if (node[0] > 150) hasFarEast = true;
      else if (node[0] < -150) hasFarWest = true;
    } else {
      for (const child of node) check(child);
    }
  };
  for (const f of features) {
    check(f.geometry.coordinates);
    if (hasFarEast && hasFarWest) return true;
  }
  return false;
}

// d3 projections normalize/wrap longitude as part of the standard
// spherical projection math, so manually shifting raw coordinates outside
// -180..180 gets silently wrapped right back - it does NOT fix the
// antimeridian crossing. The actual fix is to rotate the projection so its
// own reference meridian (where it "cuts" the sphere) lands away from the
// real landmass, then project the ORIGINAL coordinates as normal - fitSize
// computes its bounding box from the already-projected (and thus
// already-rotated) points, so this works with no coordinate mutation at
// all. This estimates a good rotation center: shift far-east points (e.g.
// Aleutians read as +172) down by 360deg just long enough to average them
// together with the rest of Alaska's (already-negative) longitudes into
// one contiguous number line, purely to compute that center.
function antimeridianRotationCenter(features) {
  let sum = 0;
  let count = 0;
  const visit = (node) => {
    if (typeof node[0] === "number") {
      sum += node[0] > 90 ? node[0] - 360 : node[0];
      count += 1;
    } else {
      for (const child of node) visit(child);
    }
  };
  for (const f of features) visit(f.geometry.coordinates);
  return count ? sum / count : 0;
}

// Rebuilds the map geometry from scratch: projection, path generation for
// every county polygon, click handlers, state border. This is the
// expensive part (measured ~270-300ms at national scope for 3,231
// counties) - only call it when the view actually changes (nation <-> a
// state, or which state), never on every timeline tick. Recoloring for a
// month change is handled separately by updateMapColors(), which just
// touches existing elements' fill/class - no geometry recomputation.
function renderMap() {
  const svg = d3.select("#map-svg");
  svg.selectAll("*").remove();
  const bounds = svg.node().getBoundingClientRect();
  const width = bounds.width || 800;
  const height = bounds.height || 600;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  if (state.view === "nation") {
    const projection = d3.geoAlbersUsa().fitSize([width, height], nationGeo);
    const path = d3.geoPath(projection);

    countyPathSel = svg
      .append("g")
      .selectAll("path")
      .data(nationGeo.features)
      .join("path")
      .attr("d", path)
      .on("click", (event, d) => selectCounty(d.id));
    countyPathSel.append("title");

    svg
      .append("path")
      .datum(topojson.mesh(rawTopo, rawTopo.objects.states, (a, b) => a !== b))
      .attr("class", "state-border")
      .attr("d", path);
  } else {
    const stateFeature = stateGeo.features.find((f) => f.id === state.stateFips);
    const countyFeatures = nationGeo.features.filter((f) => f.id.slice(0, 2) === state.stateFips);

    const projection = d3.geoMercator();
    // Alaska's Aleutian Islands cross the antimeridian (180th meridian) -
    // the raw longitudes span from ~-179 to ~+179 (the far-west Aleutians
    // read as small positive numbers, everything else as large negative
    // ones), so fitSize's default bounding box spans almost the entire
    // globe and Alaska renders as a tiny sliver instead of filling the
    // panel. (The national view doesn't have this problem because
    // d3.geoAlbersUsa() special-cases Alaska into a fixed inset; plain
    // geoMercator has no such handling.) Fixed by rotating the projection's
    // own reference meridian away from the real landmass, rather than
    // mutating coordinates - the latter doesn't work because d3 projections
    // normalize/wrap longitude as part of the standard spherical math, so a
    // manually-shifted coordinate outside -180..180 just gets wrapped
    // straight back.
    if (crossesAntimeridian(countyFeatures)) {
      projection.rotate([-antimeridianRotationCenter(countyFeatures), 0]);
    }
    const featureCollection = { type: "FeatureCollection", features: countyFeatures };
    projection.fitSize([width, height], featureCollection);
    const path = d3.geoPath(projection);

    countyPathSel = svg
      .append("g")
      .selectAll("path")
      .data(countyFeatures)
      .join("path")
      .attr("d", path)
      .on("click", (event, d) => selectCounty(d.id));
    countyPathSel.append("title");

    if (stateFeature) {
      svg
        .append("path")
        .datum(stateFeature)
        .attr("class", "state-border")
        .attr("d", path);
    }
  }

  renderLegendSwatch();
  updateMapColors();
}

// Cheap per-tick update: fill color, no-data/selected classes, and tooltip
// text on the already-built path selection. No geometry recomputation.
function updateMapColors(fast) {
  if (!countyPathSel) return;
  const scale = activeColorScale();
  countyPathSel
    .attr("class", (d) => {
      const noData = ratioFor(d.id, state.monthIndex) == null ? " no-data" : "";
      const selected = d.id === state.countyFips ? " selected" : "";
      return "county-shape" + noData + selected;
    })
    .attr("fill", (d) => {
      const r = ratioFor(d.id, state.monthIndex);
      return r == null ? null : scale(r);
    });
  // Skipped during animated play: a per-element string rebuild + text-node
  // mutation for every county, every frame, purely for an off-screen
  // tooltip nobody can read while the map is animating anyway.
  if (!fast) {
    countyPathSel.select("title").text((d) => countyTooltip(d.id));
  }
}

// {name, state} for any county on the map, whether or not Zillow covers it.
function countyDisplayInfo(fips) {
  const entry = byFips.get(fips);
  if (entry) return { name: entry.county.name, state: entry.county.state };
  return countyNameByFips.get(fips) || { name: fips, state: "" };
}

function countyTooltip(fips) {
  const { name, state: st } = countyDisplayInfo(fips);
  const label = `${name}, ${st}`;
  const r = ratioFor(fips, state.monthIndex);
  return r == null ? `${label}\nNo data available` : `${label}\n${r.toFixed(2)}x price/income`;
}

function selectCounty(fips) {
  state.countyFips = fips;
  // Derive from the FIPS itself (first 2 digits = state), not from
  // byFips - a county Zillow doesn't cover (e.g. Jackson County, CO)
  // still has a real state and should still navigate there correctly.
  state.stateFips = fips.slice(0, 2);
  state.view = "state";
  pushHash();
  render();
}

// ---------- county list ----------

function renderCountyList(filterText) {
  const ul = document.getElementById("county-list");
  ul.innerHTML = "";
  const counties = data.counties
    .filter((c) => c.fips.slice(0, 2) === state.stateFips)
    .filter((c) => !filterText || c.name.toLowerCase().includes(filterText))
    .sort((a, b) => a.name.localeCompare(b.name));

  counties.forEach((c) => {
    const li = document.createElement("li");
    li.textContent = c.name;
    const hasData = ratioFor(c.fips, state.monthIndex) != null;
    if (!hasData) li.classList.add("no-data");
    if (c.fips === state.countyFips) li.classList.add("selected");
    li.addEventListener("click", () => selectCounty(c.fips));
    ul.appendChild(li);
  });
}

// ---------- state summary (shown in the right panel when no county is selected) ----------

function countyStatsRows(filterFn) {
  return data.counties.filter(filterFn).map((c) => {
    const entry = byFips.get(c.fips);
    const zhvi = data.zhvi[entry.index][state.monthIndex];
    const income = data.income[entry.index][state.monthIndex];
    const ratio = zhvi != null && income != null ? zhvi / income : null;
    return { fips: c.fips, name: c.name, state: c.state, zhvi, income, ratio };
  });
}

// Shared by the state and national summaries: renders the coverage /
// highest-lowest home-value / income / ratio stat cards for whatever set
// of county rows is passed in.
function renderSummaryStatCards(el, rows, avgLabel, totalCount) {
  el.innerHTML = "";

  const withZhvi = rows.filter((r) => r.zhvi != null);
  const withIncome = rows.filter((r) => r.income != null);
  const withRatio = rows.filter((r) => r.ratio != null);

  addStatCard(el, "Coverage this month", [
    { label: "Counties with a ratio", value: `${withRatio.length} / ${totalCount}`, county: null },
  ]);

  if (withZhvi.length) {
    const avgZhvi = withZhvi.reduce((sum, r) => sum + r.zhvi, 0) / withZhvi.length;
    addStatCard(el, "Home value (ZHVI)", [
      { label: "Highest", value: fmtDollar(maxBy(withZhvi, "zhvi").zhvi), county: maxBy(withZhvi, "zhvi") },
      { label: "Lowest", value: fmtDollar(minBy(withZhvi, "zhvi").zhvi), county: minBy(withZhvi, "zhvi") },
      { label: avgLabel, value: fmtDollar(avgZhvi), county: null },
    ]);
  }

  if (withIncome.length) {
    const avgIncome = withIncome.reduce((sum, r) => sum + r.income, 0) / withIncome.length;
    addStatCard(el, "Per-capita income", [
      { label: "Highest", value: fmtDollar(maxBy(withIncome, "income").income), county: maxBy(withIncome, "income") },
      { label: "Lowest", value: fmtDollar(minBy(withIncome, "income").income), county: minBy(withIncome, "income") },
      { label: avgLabel, value: fmtDollar(avgIncome), county: null },
    ]);
  }

  if (withRatio.length) {
    const avgRatio = withRatio.reduce((sum, r) => sum + r.ratio, 0) / withRatio.length;
    addStatCard(el, "Price / income ratio", [
      { label: "Highest", value: maxBy(withRatio, "ratio").ratio.toFixed(2) + "x", county: maxBy(withRatio, "ratio") },
      { label: "Lowest", value: minBy(withRatio, "ratio").ratio.toFixed(2) + "x", county: minBy(withRatio, "ratio") },
      { label: avgLabel, value: avgRatio.toFixed(2) + "x", county: null },
    ]);
  }
}

function renderStateSummary() {
  document.getElementById("state-summary-title").textContent = stateName(state.stateFips) + " overview";
  const rows = countyStatsRows((c) => c.fips.slice(0, 2) === state.stateFips);
  // Total counties in this state per the map (every real county), not per
  // Zillow's list - Zillow doesn't cover every county (e.g. Jackson County,
  // CO), so using data.counties.length as the denominator understated the
  // true gap and could read as "100% coverage" when it wasn't.
  const totalCount = nationGeo.features.filter((f) => f.id.slice(0, 2) === state.stateFips).length;
  renderSummaryStatCards(document.getElementById("state-summary-stats"), rows, "State average", totalCount);
}

function renderNationalSummary() {
  document.getElementById("national-summary-title").textContent = "United States overview";
  const rows = countyStatsRows(() => true);
  renderSummaryStatCards(document.getElementById("national-summary-stats"), rows, "National average", nationGeo.features.length);
}

function maxBy(arr, key) {
  return arr.reduce((a, b) => (b[key] > a[key] ? b : a));
}

function minBy(arr, key) {
  return arr.reduce((a, b) => (b[key] < a[key] ? b : a));
}

function fmtDollar(v) {
  return "$" + d3.format(",.0f")(v);
}

function addStatCard(container, title, items) {
  const card = document.createElement("div");
  card.className = "stat-card";
  const heading = document.createElement("div");
  heading.className = "stat-card-title";
  heading.textContent = title;
  card.appendChild(heading);

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "stat-row";

    const label = document.createElement("span");
    label.className = "stat-label";
    label.textContent = item.label;
    row.appendChild(label);

    const countyName = document.createElement("span");
    countyName.className = "stat-county";
    countyName.textContent = item.county ? item.county.name : "";
    row.appendChild(countyName);

    const value = document.createElement("span");
    value.className = "stat-value";
    value.textContent = item.value;
    row.appendChild(value);

    if (item.county) {
      row.addEventListener("click", () => selectCounty(item.county.fips));
    }
    card.appendChild(row);
  });

  container.appendChild(card);
}

// ---------- detail inset (dual-line chart: zhvi + income, real values) ----------

const PRICE_COLOR = "#2b6cb0";
const INCOME_COLOR = "#d69e2e";
const RATIO_COLOR = "#2c7a7b";

// x scale shared by all three mini-charts (same domain/range for all three,
// since they're drawn at the same width with the same margins) - stashed so
// renderDetailMarker() can move each chart's marker line without redoing
// the whole layout on every month change.
let detailXScale = null;

function renderDetail() {
  const { name, state: st } = countyDisplayInfo(state.countyFips);
  document.getElementById("detail-title").textContent = `${name}, ${st}`;

  const entry = byFips.get(state.countyFips);
  const noDataEl = document.getElementById("detail-no-data");
  const chartsEl = document.getElementById("detail-charts");
  const legendEl = document.getElementById("detail-legend");
  if (!entry) {
    // Zillow doesn't cover every county (e.g. Jackson County, CO) - say so
    // plainly instead of leaving the charts blank or showing a stale
    // previous county's data.
    noDataEl.hidden = false;
    chartsEl.hidden = true;
    detailXScale = null;
    legendEl.hidden = true;
    return;
  }
  noDataEl.hidden = true;
  chartsEl.hidden = false;
  legendEl.hidden = false;

  const months = data.months;
  const zhviSeries = months.map((m, i) => data.zhvi[entry.index][i]);
  const incomeSeries = months.map((m, i) => data.income[entry.index][i]);
  const ratioSeries = months.map((m, i) =>
    zhviSeries[i] != null && incomeSeries[i] != null ? zhviSeries[i] / incomeSeries[i] : null
  );

  // Years extrapolated from a QCEW growth rate rather than a real published
  // FRED figure - flagged so the charts don't present an estimate as if it
  // were identical to real data (see build_income.py / web_data_build.py).
  // The ratio inherits the same flag wherever it depends on an estimated
  // income figure.
  const estimatedYears = new Set(data.incomeEstimatedYears[state.countyFips] || []);
  const isEstimated = (i) => estimatedYears.has(+months[i].slice(0, 4));
  let lastRealIndex = -1;
  for (let i = 0; i < months.length; i++) {
    if (incomeSeries[i] != null && !isEstimated(i)) lastRealIndex = i;
  }

  // All three charts share this width/margin, so one x scale (stashed in
  // detailXScale) positions every chart's marker line identically.
  const sampleSvg = document.getElementById("detail-svg-price");
  const width = sampleSvg.getBoundingClientRect().width || 340;
  const margin = { top: 8, right: 14, bottom: 4, left: 54 };
  const x = d3.scaleLinear().domain([0, months.length - 1]).range([margin.left, width - margin.right]);
  detailXScale = x;

  drawMiniChart("detail-svg-price", {
    width,
    margin,
    x,
    months,
    showXAxis: false,
    yFormat: (d) => "$" + d3.format(".2s")(d),
    series: [{ values: zhviSeries, color: PRICE_COLOR, dash: false }],
  });

  drawMiniChart("detail-svg-income", {
    width,
    margin,
    x,
    months,
    showXAxis: false,
    yFormat: (d) => "$" + d3.format(".2s")(d),
    series: [
      { values: incomeSeries, color: INCOME_COLOR, dash: true, defined: (i) => incomeSeries[i] != null && !isEstimated(i) },
      {
        values: incomeSeries,
        color: INCOME_COLOR,
        dash: true,
        faint: true,
        // Overlaps the real line's last point so the estimated tail
        // connects visually instead of leaving a gap.
        defined: (i) => incomeSeries[i] != null && (isEstimated(i) || i === lastRealIndex),
      },
    ],
  });

  drawMiniChart("detail-svg-ratio", {
    width,
    // Only margin.left/right need to match the other charts (that's what
    // keeps the shared x scale aligned) - bottom can be taller here to fit
    // this chart's tick labels, since it's the only one showing them.
    margin: { ...margin, bottom: 20 },
    x,
    months,
    showXAxis: true,
    yFormat: (d) => d3.format(".1f")(d) + "x",
    series: [
      { values: ratioSeries, color: RATIO_COLOR, dash: false, defined: (i) => ratioSeries[i] != null && !isEstimated(i) },
      {
        values: ratioSeries,
        color: RATIO_COLOR,
        dash: false,
        faint: true,
        defined: (i) => ratioSeries[i] != null && (isEstimated(i) || i === lastRealIndex),
      },
    ],
  });

  renderDetailMarker();
  renderDetailLegend(estimatedYears.size > 0);
}

// Draws one of the three stacked mini-charts: a single y-axis, one or more
// line series (each independently defined, for the real/estimated split),
// and a marker line for the currently-selected month. Pulled out into a
// helper because the three charts are otherwise identical apart from which
// series they plot - only the y-axis format and line styling differ.
function drawMiniChart(svgId, { width, margin, x, months, showXAxis, yFormat, series }) {
  const svg = d3.select("#" + svgId);
  svg.selectAll("*").remove();
  const bounds = svg.node().getBoundingClientRect();
  const height = bounds.height || 120;
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const allValues = series.flatMap((s) => s.values.filter((v) => v != null));
  const maxValue = d3.max(allValues) || 1;
  const y = d3.scaleLinear().domain([0, maxValue * 1.05]).range([height - margin.bottom, margin.top]);

  const g = svg.append("g");

  if (showXAxis) {
    g.append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(
        d3
          .axisBottom(x)
          .ticks(6)
          .tickFormat((i) => (months[i] ? months[i].slice(0, 4) : ""))
      );
  }

  g.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(y).ticks(4).tickFormat(yFormat));

  series.forEach((s) => {
    const line = d3
      .line()
      .defined((d, i) => (s.defined ? s.defined(i) : s.values[i] != null))
      .x((d, i) => x(i))
      .y((d, i) => y(s.values[i]));
    g.append("path")
      .datum(months)
      .attr("fill", "none")
      .attr("stroke", s.color)
      .attr("stroke-width", 1.6)
      .attr("stroke-opacity", s.faint ? 0.55 : 1)
      .attr("stroke-dasharray", s.dash ? (s.faint ? "1,2" : "4,2") : null)
      .attr("d", line);
  });

  g.append("line")
    .attr("class", "detail-marker")
    .attr("y1", margin.top)
    .attr("y2", height - margin.bottom)
    .attr("stroke", "#999")
    .attr("stroke-width", 1)
    .attr("stroke-dasharray", "2,2");
}

function renderDetailMarker() {
  if (!detailXScale || state.view !== "state" || !state.countyFips) return;
  const xPos = detailXScale(state.monthIndex);
  d3.selectAll(".detail-marker").attr("x1", xPos).attr("x2", xPos);
}

function renderDetailLegend(hasEstimatedYears) {
  const el = document.getElementById("detail-legend");
  el.innerHTML = "";
  const rows = [
    { color: PRICE_COLOR, dash: false, label: "Home value (ZHVI, $)" },
    { color: INCOME_COLOR, dash: true, label: "Per-capita income ($/yr)" },
    { color: RATIO_COLOR, dash: false, label: "Price / income ratio" },
  ];
  if (hasEstimatedYears) {
    rows.push({
      color: INCOME_COLOR,
      dash: true,
      faint: true,
      label: "Income & ratio, estimated (no published income figure yet)",
    });
  }
  rows.forEach((r) => {
    const row = document.createElement("div");
    row.className = "swatch-row";
    const swatch = document.createElement("span");
    swatch.className = "swatch";
    swatch.style.background = r.color;
    if (r.dash) swatch.style.borderTop = "2px dashed " + r.color;
    if (r.faint) swatch.style.opacity = "0.55";
    row.appendChild(swatch);
    const label = document.createElement("span");
    label.textContent = r.label;
    row.appendChild(label);
    el.appendChild(row);
  });
}

window.addEventListener("resize", () => {
  if (data) {
    render();
    renderTimelineScale();
  }
});

// Small year-tick scale drawn under the timeline slider, so there's a
// sense of "where am I" without having to read the single month/year
// label. Approximate alignment with the native <input type=range>'s
// actual track (which insets slightly for the thumb radius at each end) -
// close enough for a lightweight reference scale, not pixel-exact.
function renderTimelineScale() {
  const svg = d3.select("#timeline-scale");
  svg.selectAll("*").remove();
  const bounds = svg.node().getBoundingClientRect();
  const width = bounds.width || 300;
  svg.attr("viewBox", `0 0 ${width} 14`);

  const x = d3.scaleLinear().domain([0, data.months.length - 1]).range([6, width - 6]);

  // First month-index of each calendar year present in the data.
  const yearStarts = [];
  let lastYear = null;
  data.months.forEach((m, i) => {
    const year = m.slice(0, 4);
    if (year !== lastYear) {
      yearStarts.push({ year, index: i });
      lastYear = year;
    }
  });

  // Thin out to roughly one tick per ~55px so labels don't overlap.
  const maxTicks = Math.max(2, Math.floor(width / 55));
  const step = Math.max(1, Math.ceil(yearStarts.length / maxTicks));
  const ticks = yearStarts.filter((_, i) => i % step === 0);

  const g = svg.append("g");
  ticks.forEach((t) => {
    const tx = x(t.index);
    const anchor = tx < 15 ? "start" : tx > width - 15 ? "end" : "middle";
    g.append("line").attr("x1", tx).attr("x2", tx).attr("y1", 0).attr("y2", 4);
    g.append("text").attr("x", tx).attr("y", 13).attr("text-anchor", anchor).text(t.year);
  });
}
})();
