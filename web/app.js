(function () {
"use strict";

// ---------- state ----------

const state = {
  view: "nation",       // 'nation' | 'state'
  stateFips: null,      // 2-digit
  countyFips: null,     // 5-digit, selected for the detail inset
  monthIndex: 0,
};

let data = null;        // { months, counties, zhvi, income }
let rawTopo = null;     // raw TopoJSON topology (needed for mesh())
let nationGeo = null;   // GeoJSON FeatureCollection, all counties
let stateGeo = null;    // GeoJSON FeatureCollection, all states
let byFips = new Map(); // fips -> { county, index, geo }
let statesByFips = new Map(); // 2-digit fips -> { abbr, geo }
let colorScale = null;
let playTimer = null;

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
    statesByFips.set(f.id, { abbr: anyCounty ? anyCounty.state : f.id, geo: f });
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

  buildLegendSwatch();
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
  const year = data.months[monthIndex].slice(0, 4);
  const incomeByYear = data.income[fips];
  const income = incomeByYear ? incomeByYear[year] : null;
  if (income == null) return null;
  return zhvi / income;
}

function latestMonthWithIncomeData() {
  for (let mi = data.months.length - 1; mi >= 0; mi--) {
    const year = data.months[mi].slice(0, 4);
    const hasAny = data.counties.some((c) => {
      const byYear = data.income[c.fips];
      return byYear && byYear[year] != null;
    });
    if (hasAny) return mi;
  }
  return data.months.length - 1;
}

function buildColorScale() {
  // Sample ratios across all counties/months to pick a robust domain
  // (2nd/98th percentile) rather than let a few outlier counties wash
  // out the color range for everyone else.
  const samples = [];
  const step = Math.max(1, Math.floor(data.counties.length / 400)); // subsample counties
  for (let ci = 0; ci < data.counties.length; ci += step) {
    const fips = data.counties[ci].fips;
    for (let mi = 0; mi < data.months.length; mi += 6) {
      const r = ratioFor(fips, mi);
      if (r != null && isFinite(r)) samples.push(r);
    }
  }
  samples.sort((a, b) => a - b);
  const lo = samples[Math.floor(samples.length * 0.02)];
  const hi = samples[Math.floor(samples.length * 0.98)];
  return d3.scaleSequential(d3.interpolateBlues).domain([lo, hi]).clamp(true);
}

function buildLegendSwatch() {
  const svg = d3.select("#legend-swatch");
  const width = 140;
  svg.attr("width", width);
  const defs = svg.append("defs");
  const gradId = "legend-gradient";
  const grad = defs.append("linearGradient").attr("id", gradId);
  const stops = 10;
  for (let i = 0; i <= stops; i++) {
    const t = i / stops;
    const [lo, hi] = colorScale.domain();
    grad
      .append("stop")
      .attr("offset", `${t * 100}%`)
      .attr("stop-color", colorScale(lo + t * (hi - lo)));
  }
  svg.append("rect").attr("width", width).attr("height", 14).attr("fill", `url(#${gradId})`);
  const [lo, hi] = colorScale.domain();
  document.getElementById("legend-min").textContent = lo.toFixed(1) + "x";
  document.getElementById("legend-max").textContent = hi.toFixed(1) + "x";
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
    updateTimelineLabel();
    renderMap();
    renderDetailMarker();
    pushHash();
  });

  document.getElementById("play-toggle").addEventListener("click", togglePlay);

  document.getElementById("county-search").addEventListener("input", (e) => {
    renderCountyList(e.target.value.trim().toLowerCase());
  });
}

function togglePlay() {
  const btn = document.getElementById("play-toggle");
  if (playTimer) {
    clearInterval(playTimer);
    playTimer = null;
    btn.textContent = "▶";
    return;
  }
  btn.textContent = "⏸";
  playTimer = setInterval(() => {
    const slider = document.getElementById("timeline-slider");
    let next = state.monthIndex + 1;
    if (next > data.months.length - 1) next = 0;
    state.monthIndex = next;
    slider.value = next;
    updateTimelineLabel();
    renderMap();
    renderDetailMarker();
    pushHash();
  }, 200);
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

  const listPanel = document.getElementById("county-list-panel");
  const detailPanel = document.getElementById("detail-panel");

  if (state.view === "nation") {
    listPanel.hidden = true;
    detailPanel.hidden = true;
    document.getElementById("crumb-title").textContent = "United States";
    renderMap();
  } else {
    listPanel.hidden = false;
    document.getElementById("crumb-title").textContent = stateName(state.stateFips);
    renderCountyList("");
    renderMap();
    if (state.countyFips) {
      detailPanel.hidden = false;
      renderDetail();
    } else {
      detailPanel.hidden = true;
    }
  }
}

function stateName(fips) {
  const s = statesByFips.get(fips);
  return s ? s.abbr : fips;
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
      const entry = byFips.get(state.countyFips);
      const span = document.createElement("span");
      span.textContent = entry ? entry.county.name : state.countyFips;
      el.appendChild(span);
    }
  }
}

// ---------- map ----------

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

    svg
      .append("g")
      .selectAll("path")
      .data(nationGeo.features)
      .join("path")
      .attr("class", (d) => "county-shape" + (ratioFor(d.id, state.monthIndex) == null ? " no-data" : ""))
      .attr("d", path)
      .attr("fill", (d) => {
        const r = ratioFor(d.id, state.monthIndex);
        return r == null ? null : colorScale(r);
      })
      .append("title")
      .text((d) => countyTooltip(d.id));

    svg
      .select("g")
      .selectAll("path.county-shape")
      .on("click", (event, d) => {
        state.view = "state";
        state.stateFips = d.id.slice(0, 2);
        state.countyFips = null;
        pushHash();
        render();
      });

    svg
      .append("path")
      .datum(topojson.mesh(rawTopo, rawTopo.objects.states, (a, b) => a !== b))
      .attr("class", "state-border")
      .attr("d", path);
  } else {
    const stateFeature = stateGeo.features.find((f) => f.id === state.stateFips);
    const countyFeatures = nationGeo.features.filter((f) => f.id.slice(0, 2) === state.stateFips);
    const projection = d3.geoMercator();
    const featureCollection = { type: "FeatureCollection", features: countyFeatures };
    projection.fitSize([width, height], featureCollection);
    const path = d3.geoPath(projection);

    svg
      .append("g")
      .selectAll("path")
      .data(countyFeatures)
      .join("path")
      .attr("class", (d) => {
        const selected = d.id === state.countyFips ? " selected" : "";
        return "county-shape" + (ratioFor(d.id, state.monthIndex) == null ? " no-data" : "") + selected;
      })
      .attr("d", path)
      .attr("fill", (d) => {
        const r = ratioFor(d.id, state.monthIndex);
        return r == null ? null : colorScale(r);
      })
      .attr("stroke", (d) => (d.id === state.countyFips ? "#1f1f1f" : null))
      .attr("stroke-width", (d) => (d.id === state.countyFips ? 1.5 : null))
      .on("click", (event, d) => selectCounty(d.id))
      .append("title")
      .text((d) => countyTooltip(d.id));

    if (stateFeature) {
      svg
        .append("path")
        .datum(stateFeature)
        .attr("class", "state-border")
        .attr("d", path);
    }
  }
}

function countyTooltip(fips) {
  const entry = byFips.get(fips);
  if (!entry) return fips;
  const r = ratioFor(fips, state.monthIndex);
  const label = `${entry.county.name}, ${entry.county.state}`;
  return r == null ? `${label}\nNo income data available` : `${label}\n${r.toFixed(2)}x price/income`;
}

function selectCounty(fips) {
  state.countyFips = fips;
  const entry = byFips.get(fips);
  if (entry) state.stateFips = fips.slice(0, 2);
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

// ---------- detail inset (dual-line chart: zhvi + income, real values) ----------

function renderDetail() {
  const entry = byFips.get(state.countyFips);
  if (!entry) return;
  document.getElementById("detail-title").textContent = `${entry.county.name}, ${entry.county.state}`;

  const svg = d3.select("#detail-svg");
  svg.selectAll("*").remove();
  const bounds = svg.node().getBoundingClientRect();
  const width = bounds.width || 340;
  const height = bounds.height || 260;
  const margin = { top: 10, right: 46, bottom: 24, left: 54 };
  svg.attr("viewBox", `0 0 ${width} ${height}`);

  const months = data.months;
  const zhviSeries = months.map((m, i) => data.zhvi[entry.index][i]);
  const incomeSeries = months.map((m) => {
    const year = m.slice(0, 4);
    const byYear = data.income[state.countyFips];
    return byYear ? (byYear[year] != null ? byYear[year] : null) : null;
  });

  // Years extrapolated from a QCEW growth rate rather than a real published
  // FRED figure - flagged so the chart doesn't present an estimate as if it
  // were identical to real data (see build_income.py / web_data_build.py).
  const estimatedYears = new Set(data.incomeEstimatedYears[state.countyFips] || []);
  const isEstimated = (i) => estimatedYears.has(+months[i].slice(0, 4));
  let lastRealIndex = -1;
  for (let i = 0; i < months.length; i++) {
    if (incomeSeries[i] != null && !isEstimated(i)) lastRealIndex = i;
  }

  const x = d3
    .scaleLinear()
    .domain([0, months.length - 1])
    .range([margin.left, width - margin.right]);

  const zhviExtent = d3.extent(zhviSeries.filter((v) => v != null));
  const incomeExtent = d3.extent(incomeSeries.filter((v) => v != null));

  const yPrice = d3
    .scaleLinear()
    .domain([0, (zhviExtent[1] || 1) * 1.05])
    .range([height - margin.bottom, margin.top]);

  const yIncome = d3
    .scaleLinear()
    .domain([0, (incomeExtent[1] || 1) * 1.05])
    .range([height - margin.bottom, margin.top]);

  const g = svg.append("g");

  g.append("g")
    .attr("transform", `translate(0,${height - margin.bottom})`)
    .call(
      d3
        .axisBottom(x)
        .ticks(6)
        .tickFormat((i) => (months[i] ? months[i].slice(0, 4) : ""))
    );

  g.append("g")
    .attr("transform", `translate(${margin.left},0)`)
    .call(d3.axisLeft(yPrice).ticks(5).tickFormat((d) => "$" + d3.format(".2s")(d)));

  g.append("g")
    .attr("transform", `translate(${width - margin.right},0)`)
    .call(d3.axisRight(yIncome).ticks(5).tickFormat((d) => "$" + d3.format(".2s")(d)));

  const priceLine = d3
    .line()
    .defined((d, i) => zhviSeries[i] != null)
    .x((d, i) => x(i))
    .y((d, i) => yPrice(zhviSeries[i]));

  const incomeLineReal = d3
    .line()
    .defined((d, i) => incomeSeries[i] != null && !isEstimated(i))
    .x((d, i) => x(i))
    .y((d, i) => yIncome(incomeSeries[i]));

  // Overlaps the real line's last point so the estimated tail connects
  // visually instead of leaving a gap.
  const incomeLineEstimated = d3
    .line()
    .defined((d, i) => incomeSeries[i] != null && (isEstimated(i) || i === lastRealIndex))
    .x((d, i) => x(i))
    .y((d, i) => yIncome(incomeSeries[i]));

  g.append("path")
    .datum(months)
    .attr("fill", "none")
    .attr("stroke", "#2b6cb0")
    .attr("stroke-width", 1.6)
    .attr("d", priceLine);

  g.append("path")
    .datum(months)
    .attr("fill", "none")
    .attr("stroke", "#d69e2e")
    .attr("stroke-width", 1.6)
    .attr("stroke-dasharray", "4,2")
    .attr("d", incomeLineReal);

  g.append("path")
    .datum(months)
    .attr("fill", "none")
    .attr("stroke", "#d69e2e")
    .attr("stroke-width", 1.6)
    .attr("stroke-opacity", 0.55)
    .attr("stroke-dasharray", "1,2")
    .attr("d", incomeLineEstimated);

  g.append("line")
    .attr("id", "detail-marker")
    .attr("y1", margin.top)
    .attr("y2", height - margin.bottom)
    .attr("stroke", "#999")
    .attr("stroke-width", 1)
    .attr("stroke-dasharray", "2,2");

  svg.node()._x = x; // stash scale for the marker updater

  renderDetailMarker();
  renderDetailLegend(estimatedYears.size > 0);
}

function renderDetailMarker() {
  const svg = document.getElementById("detail-svg");
  if (!svg || !svg._x || state.view !== "state" || !state.countyFips) return;
  const x = svg._x(state.monthIndex);
  const marker = d3.select("#detail-marker");
  if (!marker.empty()) marker.attr("x1", x).attr("x2", x);
}

function renderDetailLegend(hasEstimatedYears) {
  const el = document.getElementById("detail-legend");
  el.innerHTML = "";
  const rows = [
    { color: "#2b6cb0", dash: false, label: "Home value (ZHVI, $)" },
    { color: "#d69e2e", dash: true, label: "Per-capita income ($/yr)" },
  ];
  if (hasEstimatedYears) {
    rows.push({
      color: "#d69e2e",
      dash: true,
      faint: true,
      label: "Income, estimated (no published figure yet)",
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
  if (data) render();
});
})();
