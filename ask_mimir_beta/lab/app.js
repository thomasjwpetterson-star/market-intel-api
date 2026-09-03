const state = {
  messages: [],
  busy: false,
  tier: localStorage.getItem("askMimirTestTier") || "public",
  subject: localStorage.getItem("askMimirTestSubject") || crypto.randomUUID(),
  entitlement: null,
};
localStorage.setItem("askMimirTestSubject", state.subject);

const emptyState = document.getElementById("emptyState");
const messagesEl = document.getElementById("messages");
const form = document.getElementById("askForm");
const question = document.getElementById("question");
const sendButton = document.getElementById("sendButton");
const thinking = document.getElementById("thinking");
const thinkingTitle = document.getElementById("thinkingTitle");
const thinkingDetail = document.getElementById("thinkingDetail");
const thinkingElapsed = document.getElementById("thinkingElapsed");
const evidenceList = document.getElementById("evidenceList");
const evidenceCount = document.getElementById("evidenceCount");
const releaseLabel = document.getElementById("releaseLabel");
const allowanceStatus = document.getElementById("allowanceStatus");
const testTierControl = document.getElementById("testTierControl");
const testTier = document.getElementById("testTier");
let thinkingTimer = null;

function startThinking(promptText = "") {
  const started = Date.now();
  thinking.hidden = false;
  thinkingTitle.textContent = "Submitting the question";
  thinkingDetail.textContent = "Waiting for the evidence workflow to begin";
  const update = () => {
    const elapsed = Math.floor((Date.now() - started) / 1000);
    thinkingElapsed.textContent = `${elapsed}s`;
  };
  update();
  thinkingTimer = window.setInterval(update, 1000);
}

function updateThinking(stage, detail) {
  thinkingTitle.textContent = stage || "Working through the evidence";
  thinkingDetail.textContent = detail || "Checking the records supporting the answer";
}

function betaHeaders() {
  return {
    "Content-Type": "application/json",
    "X-Ask-Mimir-Tier": state.tier,
    "X-Ask-Mimir-Subject": state.subject,
  };
}

function updateAllowance(access) {
  if (!access) return;
  state.entitlement = access;
  allowanceStatus.textContent = `${access.display_name} · ${access.queries_remaining_today} of ${access.queries_per_utc_day} queries left today`;
}

function stopThinking() {
  if (thinkingTimer) window.clearInterval(thinkingTimer);
  thinkingTimer = null;
  thinking.hidden = true;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderInline(value) {
  return escapeHtml(value)
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(
      /\[([^\]]+)\]\(((?:https?:\/\/|\/)[^)]+)\)/g,
      '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>',
    );
}

function parseMarkdownTableRow(line) {
  return line.trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map((cell) => cell.trim());
}

function tableHtml(headers, rows) {
  return `<div class="answer-table-scroll"><table><thead><tr>${headers.map((header) => `<th>${renderInline(header)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${headers.map((_, index) => `<td>${renderInline(row[index] || "")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

function renderAnswerTable(headers, rows, label = "Evidence summary") {
  const previewCount = 10;
  const preview = rows.slice(0, previewCount);
  const remaining = rows.slice(previewCount);
  const expanded = remaining.length
    ? `<details class="answer-table-more"><summary>Show ${remaining.length} more rows</summary>${tableHtml(headers, remaining)}</details>`
    : "";
  return `<section class="answer-table-shell"><div class="answer-table-label">${escapeHtml(label)} · ${rows.length} rows</div>${tableHtml(headers, preview)}${expanded}</section>`;
}

function renderText(value) {
  const lines = String(value).split("\n");
  let inList = false;
  const output = [];
  for (let index = 0; index < lines.length; index += 1) {
    const raw = lines[index];
    if (
      raw.trim().startsWith("|")
      && lines[index + 1]?.trim().startsWith("|")
      && /^\|?\s*:?-{3,}/.test(lines[index + 1].trim())
    ) {
      if (inList) { output.push("</ul>"); inList = false; }
      const headers = parseMarkdownTableRow(raw);
      index += 2;
      const rows = [];
      while (index < lines.length && lines[index].trim().startsWith("|")) {
        rows.push(parseMarkdownTableRow(lines[index]));
        index += 1;
      }
      index -= 1;
      output.push(renderAnswerTable(headers, rows));
      continue;
    }
    const line = renderInline(raw);
    if (/^#{1,3}\s/.test(line)) {
      if (inList) { output.push("</ul>"); inList = false; }
      output.push(`<h3>${line.replace(/^#{1,3}\s/, "")}</h3>`);
    } else if (/^[-*]\s/.test(line)) {
      if (!inList) { output.push("<ul>"); inList = true; }
      output.push(`<li>${line.replace(/^[-*]\s/, "")}</li>`);
    } else if (line.trim()) {
      if (inList) { output.push("</ul>"); inList = false; }
      output.push(`<p>${line}</p>`);
    }
  }
  if (inList) output.push("</ul>");
  return output.join("");
}

function dashboardUrl(view, key, value) {
  const url = new URL("https://www.mimiradvisors.org/dashboard");
  url.searchParams.set("view", view);
  url.searchParams.set(key, String(value));
  url.searchParams.set("source", "ask-mimir");
  return url.toString();
}

function dashboardLink(label, view, key, value, className = "") {
  return `<a ${className ? `class="${className}" ` : ""}href="${escapeHtml(dashboardUrl(view, key, value))}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
}

function dashboardMarkdownLink(label, view, key, value) {
  return `[${label}](${dashboardUrl(view, key, value)})`;
}

function platformSiteIndexHtml(trace, artifacts = {}) {
  const platformCall = (trace || []).find(
    (entry) => entry.tool === "get_platform_supply_chain" && entry.result,
  );
  const rows = artifacts.supplier_site_index
    || platformCall?.result?.supplier_site_summary
    || [];
  if (!rows.length) return "";

  const headers = [
    "Company",
    "Contracting site / CAGE",
    "Observed place(s) of performance",
    "Supplier route",
    "Capability",
    "Evidence example",
    "Notes",
  ];
  const tableRows = rows.map((row) => {
    const cage = row.cage || "Not shown";
    const site = [row.contracting_site_city, row.contracting_site_state]
      .filter(Boolean)
      .join(", ") || "Location not shown";
    const places = (row.observed_place_of_performance_locations || []).join("; ") || "Not reported";
    const capability = (row.capability || []).join("; ") || "Capability not established";
    const evidence = (row.evidence_examples || []).slice(0, 2).map(
      (award) => dashboardMarkdownLink(award, "AWARDS", "award", award),
    ).join("; ") || "See evidence drawer";
    return [
      dashboardMarkdownLink(row.supplier || cage, "COMPANY", "cage", cage),
      `${site} / ${dashboardMarkdownLink(`CAGE ${cage}`, "COMPANY", "cage", cage)}`,
      places,
      row.supplier_tier || "Reported supplier",
      capability,
      evidence,
      row.notes || "",
    ];
  });
  return renderAnswerTable(headers, tableRows, "Supplier site index");
}

function addMessage(role, content, trace = [], artifacts = {}) {
  emptyState.hidden = true;
  messagesEl.classList.add("active");
  const article = document.createElement("article");
  article.className = `message ${role}`;
  const siteIndex = role === "assistant" ? platformSiteIndexHtml(trace, artifacts) : "";
  article.innerHTML = `<div class="message-label">${role === "user" ? "YOU" : "ASK MIMIR"}</div><div class="message-body">${siteIndex}${renderText(content)}</div>`;
  messagesEl.appendChild(article);
  window.scrollTo({ top: document.body.scrollHeight, behavior: "smooth" });
  return article;
}

function addFeedbackControls(article, payload) {
  if (!payload?.response_id || payload.response_id.endsWith("disambiguation")) return;
  const feedback = document.createElement("div");
  feedback.className = "answer-feedback";
  feedback.innerHTML = `
    <span>Was this useful?</span>
    <button type="button" data-rating="accurate">Accurate</button>
    <button type="button" data-rating="incomplete">Incomplete</button>
    <button type="button" data-rating="wrong_entity">Wrong entity</button>
    <button type="button" data-rating="unsupported">Evidence issue</button>`;
  feedback.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-rating]");
    if (!button) return;
    const rating = button.dataset.rating;
    const reason = rating === "accurate"
      ? null
      : window.prompt("What should we review? A short note is enough.") || null;
    const response = await fetch("/api/feedback", {
      method: "POST",
      headers: betaHeaders(),
      body: JSON.stringify({
        response_id: payload.response_id,
        request_id: payload.request_id,
        rating,
        reason,
      }),
    });
    if (response.ok) {
      feedback.className = "answer-feedback recorded";
      feedback.textContent = "Feedback recorded. Thank you.";
    }
  });
  article.appendChild(feedback);
}

function compactResult(entry) {
  if (entry.error) return entry;
  const result = entry.result || {};
  if (entry.tool === "search_metric_scopes") {
    return { release_id: result.release_id, matches: result.matches };
  }
  if (entry.tool === "get_metric_evidence") {
    return {
      release_id: result.release_id,
      scope_type: result.scope_type,
      scope_id: result.scope_id,
      measure_type: result.measure_type,
      fiscal_year: result.fiscal_year,
      total_records: result.total_records,
      records_shown: result.records?.length || 0,
      records: result.records,
    };
  }
  return result;
}

function formatMoney(value) {
  if (value === null || value === undefined) return "Value not stated";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function candidateEvidenceCard(candidate, horizonLabel) {
  const details = document.createElement("details");
  details.className = "evidence-card";
  const title = escapeHtml(candidate.title || candidate.program || "Opportunity evidence");
  const route = (candidate.route_to_market || []).join(" · ");
  details.innerHTML = `
    <summary>
      <span class="tool-name">${title}</span>
      <span class="tool-meta">${escapeHtml(horizonLabel)}${route ? ` · ${escapeHtml(route)}` : ""}</span>
    </summary>`;

  const body = document.createElement("div");
  body.className = "evidence-detail";
  const overview = document.createElement("section");
  overview.className = "evidence-section";
  overview.innerHTML = `
    <h4>Assessment</h4>
    <div class="evidence-row">${escapeHtml(candidate.commercial_utility || "No interpretation supplied.")}</div>
    <span class="evidence-tag">${escapeHtml(candidate.fit_band || "existing position")}</span>
    <span class="evidence-tag">${escapeHtml(candidate.scoring_primitives?.evidence_quality || "evidence not scored")}</span>`;
  body.appendChild(overview);

  const awards = candidate.prime_award_evidence || [];
  if (awards.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Prime award records</h4>${awards.map((award) => `
      <div class="evidence-row">
        <strong>${escapeHtml(award.contract_id || "Award")}</strong>
        ${escapeHtml(award.base_award_description || "No description")}<br />
        ${escapeHtml(award.recipient_name || award.recipient_cage || "Recipient not shown")} · ${formatMoney(award.net_prime_obligations_usd)} net observed obligations · latest ${escapeHtml(award.latest_action_date || "date unavailable")}
      </div>`).join("")}`;
    body.appendChild(section);
  }

  const events = candidate.event_evidence || [];
  if (events.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Demand and requirement signals</h4>${events.map((event) => {
      const source = event.source || {};
      const sourceTitle = escapeHtml(source.title || source.publisher || "Source document");
      const sourceLink = source.canonical_url
        ? `<a href="${escapeHtml(source.canonical_url)}" target="_blank" rel="noopener noreferrer">${sourceTitle}</a>`
        : sourceTitle;
      return `<div class="evidence-row"><strong>${sourceLink}</strong>${escapeHtml(event.fact || "")}${event.effective_period ? `<br />Effective period: ${escapeHtml(event.effective_period)}` : ""}</div>`;
    }).join("")}`;
    body.appendChild(section);
  }

  const relationships = candidate.analogous_subcontract_capability_slice?.records || [];
  if (relationships.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Reported subcontract relationships</h4>${relationships.map((row) => {
      const reports = (row.source_report_ids || []).map((id) => `<span class="evidence-tag">Report ${escapeHtml(id)}</span>`).join("");
      return `<div class="evidence-row">
        <strong>${escapeHtml(row.incumbent_name || row.incumbent_cage || "Supplier")}</strong>
        ${escapeHtml((row.prime_names || []).join(", "))} · ${formatMoney(row.mimir_modelled_subcontract_value_usd)} Mimir-modelled reported value · ${escapeHtml(row.latest_observed_date || "date unavailable")}<br />
        ${escapeHtml((row.matching_descriptions || []).join(" · "))}<br />${reports}
      </div>`;
    }).join("")}`;
    body.appendChild(section);
  }

  const budgets = candidate.budget_evidence || [];
  if (budgets.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Budget evidence</h4>${budgets.map((row) => `
      <div class="evidence-row">
        <strong>FY${escapeHtml(row.fiscal_year)} · ${escapeHtml(row.funding_status || "Status unavailable")}</strong>
        ${escapeHtml(row.budget_line_item_title || "Budget line")} · ${row.measure_type === "quantity" ? `${escapeHtml(row.quantity)} units` : formatMoney(row.amount_usd)}<br />
        ${escapeHtml(row.source_locator || "Source location unavailable")}
      </div>`).join("")}`;
    body.appendChild(section);
  }

  details.appendChild(body);
  return details;
}

function renderOpportunityEvidence(entry, answerText) {
  const result = entry.result || {};
  const horizonNames = {
    protect_and_expand: "Protect and expand",
    shape_before_solicitation: "Shape before solicitation",
    adjacent_whitespace: "Adjacent requirement",
  };
  Object.entries(result.decision_horizons || {}).forEach(([horizon, candidates]) => {
    (candidates || []).forEach((candidate) => {
      const citedInAnswer = String(answerText || "").toLowerCase().includes(
        String(candidate.title || "").toLowerCase()
      );
      if (horizon === "shape_before_solicitation" && !citedInAnswer) return;
      evidenceList.appendChild(candidateEvidenceCard(candidate, horizonNames[horizon] || horizon));
    });
  });
}

function platformSupplierEvidenceCard(supplier) {
  const details = document.createElement("details");
  details.className = "evidence-card";
  const roles = (supplier.component_roles || []).join(" · ");
  details.innerHTML = `
    <summary>
      <span class="tool-name">${escapeHtml(supplier.display_name || "Verified supplier")}</span>
      <span class="tool-meta">${escapeHtml(roles || "Platform content")}</span>
    </summary>`;

  const body = document.createElement("div");
  body.className = "evidence-detail";
  const overview = document.createElement("section");
  overview.className = "evidence-section";
  overview.innerHTML = `
    <h4>Verified content</h4>
    <div class="evidence-row">${escapeHtml(roles)}</div>
    <div class="evidence-row">${escapeHtml(supplier.site_attribution || "Site attribution not stated.")}</div>`;
  body.appendChild(overview);

  const sources = supplier.sources || [];
  if (sources.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Public source</h4>${sources.map((source) => `
      <div class="evidence-row">
        <strong><a href="${escapeHtml(source.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(source.title)}</a></strong>
        ${escapeHtml(source.publisher)}${source.publication_date ? ` · ${escapeHtml(source.publication_date)}` : ""}<br />
        ${escapeHtml(source.claim)}
      </div>`).join("")}`;
    body.appendChild(section);
  }

  const siteEvidence = [
    ...(supplier.prime_recipient_site_evidence || []),
    ...(supplier.reported_subaward_site_evidence || []),
  ];
  if (siteEvidence.length) {
    const section = document.createElement("section");
    section.className = "evidence-section";
    section.innerHTML = `<h4>Government-record drill-down</h4>${siteEvidence.map((site) => {
      const contracts = (site.sample_prime_contract_ids || []).map((id) => dashboardLink(`Contract ${id}`, "AWARDS", "award", id, "evidence-tag")).join("");
      const location = [site.city, site.state, site.country].filter(Boolean).join(", ");
      return `<div class="evidence-row">
        <strong>${site.cage ? dashboardLink(`${site.supplier_name || supplier.display_name} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage) : escapeHtml(site.supplier_name || supplier.display_name)}</strong>
        ${escapeHtml(location || "Location not shown")}<br />${contracts}
      </div>`;
    }).join("")}`;
    body.appendChild(section);
  }

  details.appendChild(body);
  return details;
}

function renderPlatformSupplyChainEvidence(entry) {
  const result = entry.result || {};
  if (state.entitlement?.can_download_evidence) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = `/api/evidence/platform-supply-chain/${encodeURIComponent(result.scope?.platform_id || "CH-53K")}.zip`;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }

  const platformPrimes = result.platform_prime_contractors || [];
  if (platformPrimes.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Platform prime contractor</span>
        <span class="tool-meta">Direct government award context</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${platformPrimes.map((site) => `<div class="evidence-row">
          <strong>${dashboardLink(`${site.supplier_name || site.cage} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage)}</strong>
          ${escapeHtml([site.city, site.state].filter(Boolean).join(", "))}<br />
          ${(site.sample_contract_ids || []).map((id) => dashboardLink(`Award ${id}`, "AWARDS", "award", id, "evidence-tag")).join("")}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  (result.component_verified_suppliers || []).forEach((supplier) => {
    evidenceList.appendChild(platformSupplierEvidenceCard(supplier));
  });

  const capabilities = result.capability_supported_first_tier_suppliers || [];
  if (capabilities.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Reported component and lower-tier evidence</span>
        <span class="tool-meta">Specific items, capability categories and customer routes</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${capabilities.map((row) => `<div class="evidence-row">
          <strong>${dashboardLink(`${row.supplier_name || row.cage} · CAGE ${row.cage}`, "COMPANY", "cage", row.cage)}</strong>
          ${escapeHtml(row.capability_description)}<br />
          Reported beneath ${escapeHtml((row.reported_prime_names || []).join(" / ") || "a mapped prime award")}<br />
          <span class="evidence-tag">${row.evidence_precision === "SPECIFIC_REPORTED_ITEM" ? "Specific reported item" : "Capability category"}</span>
          ${(row.sample_prime_contract_ids || []).map((id) => dashboardLink(`Contract ${id}`, "AWARDS", "award", id, "evidence-tag")).join("")}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  const reported = result.reported_first_tier_supplier_sites || [];
  if (reported.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Reported suppliers under mapped prime awards</span>
        <span class="tool-meta">Direct subaward relationship · not necessarily Tier 1 to Sikorsky</span>
      </summary>
      <div class="evidence-detail">
        <section class="evidence-section">
          <h4>Reported CH-53K relationships</h4>
          ${reported.map((site) => `
            <div class="evidence-row">
              <strong>${dashboardLink(`${site.supplier_name || site.cage} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage)}</strong>
              ${escapeHtml([site.city, site.state, site.country].filter(Boolean).join(", "))}<br />
              Reported beneath ${escapeHtml((site.reported_prime_names || []).filter(Boolean).join(" / ") || "a mapped prime award")}<br />
              ${formatMoney(site.mimir_modelled_subcontract_value_usd)} Mimir-modelled reported value · ${escapeHtml(site.selected_report_count || 0)} selected reports<br />
              ${(site.sample_prime_contract_ids || []).map((id) => dashboardLink(`Contract ${id}`, "AWARDS", "award", id, "evidence-tag")).join("")}
            </div>`).join("")}
        </section>
      </div>`;
    evidenceList.appendChild(details);
  }

  const familyItems = result.broader_ch53_family?.top_items || [];
  if (familyItems.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Wider CH-53 family evidence</span>
        <span class="tool-meta">NIIN and part evidence · not confirmed for CH-53K</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        <h4>Family-level DLA activity</h4>
        ${familyItems.slice(0, 12).map((item) => `<div class="evidence-row">
          <strong>${dashboardLink(`${item.description || item.nsn} · NIIN ${item.niin}`, "PARTS", "nsn", item.nsn || item.niin)}</strong>
          NSN ${escapeHtml(item.nsn || "not available")} · ${escapeHtml(item.platform_family)} family reference<br />
          ${(item.sample_part_numbers || []).slice(0, 4).map((part) => `<span class="evidence-tag">Part ${escapeHtml(part)}</span>`).join("")}
          <span class="evidence-tag">CH-53K fit unconfirmed</span>
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  const exclusions = result.known_configuration_exclusions || [];
  if (exclusions.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Known configuration exclusions</span>
        <span class="tool-meta">Retained for audit · excluded from CH-53K totals</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${exclusions.map((row) => `<div class="evidence-row"><strong>${escapeHtml(row.excluded_content)}</strong>${escapeHtml(row.reason)}</div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  const primes = result.other_direct_prime_recipients || [];
  if (primes.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">Other direct award recipients</span>
        <span class="tool-meta">Not automatically first-tier suppliers</span>
      </summary>
      <div class="evidence-detail">
        <section class="evidence-section">
          ${primes.map((site) => `
            <div class="evidence-row">
              <strong>${dashboardLink(`${site.supplier_name || site.cage} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage)}</strong>
              ${escapeHtml([site.city, site.state].filter(Boolean).join(", "))} · ${formatMoney(site.net_prime_obligations_usd)} net prime obligations<br />
              ${(site.sample_contract_ids || []).map((id) => dashboardLink(`Award ${id}`, "AWARDS", "award", id, "evidence-tag")).join("")}
            </div>`).join("")}
        </section>
      </div>`;
    evidenceList.appendChild(details);
  }
}

function renderCompanyEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }

  const scope = result.scope || {};
  const sites = result.identity?.sites || [];
  const details = document.createElement("details");
  details.className = "evidence-card";
  details.innerHTML = `
    <summary>
      <span class="tool-name">Company evidence scope</span>
      <span class="tool-meta">${escapeHtml(scope.scope_name || scope.scope_id || "Company")} · ${escapeHtml(scope.observation_window || "Current evidence release")}</span>
    </summary>
    <div class="evidence-detail"><section class="evidence-section">
      ${sites.map((site) => `<div class="evidence-row">
        <strong>${site.cage ? dashboardLink(`${site.vendor_name || site.cage} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage) : escapeHtml(site.vendor_name || "Company site")}</strong>
        ${escapeHtml([site.city, site.state].filter(Boolean).join(", ") || "Location not shown")}<br />
        ${escapeHtml(site.official_capability_summary || "See the evidence pack for supporting records.")}
        ${site.official_source_url ? `<br /><a href="${escapeHtml(site.official_source_url)}" target="_blank" rel="noopener noreferrer">Company source</a>` : ""}
      </div>`).join("") || '<div class="evidence-row">No resolved company site was returned.</div>'}
    </section></div>`;
  evidenceList.appendChild(details);

  const awards = result.top_awards || [];
  if (awards.length) {
    const awardDetails = document.createElement("details");
    awardDetails.className = "evidence-card";
    awardDetails.innerHTML = `
      <summary>
        <span class="tool-name">Prime award evidence</span>
        <span class="tool-meta">${awards.length} award records in this answer context</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${awards.slice(0, 12).map((award) => `<div class="evidence-row">
          <strong>${dashboardLink(award.contract_id || "Award", "AWARDS", "award", award.contract_id || award.award_key)}</strong>
          ${escapeHtml(award.base_award_description || "Description not reported")}<br />
          ${formatMoney(award.net_prime_obligations_usd)} prime obligations
          ${award.public_record_url ? ` · <a href="${escapeHtml(award.public_record_url)}" target="_blank" rel="noopener noreferrer">Public record</a>` : ""}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(awardDetails);
  }
}

function renderUniversalPlatformEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }

  const scope = result.scope || {};
  const summary = document.createElement("details");
  summary.className = "evidence-card";
  summary.open = true;
  summary.innerHTML = `
    <summary>
      <span class="tool-name">${escapeHtml(scope.display_name || "Platform evidence")}</span>
      <span class="tool-meta">FY${escapeHtml((scope.completed_fiscal_years || [])[0] || "")}–FY${escapeHtml((scope.completed_fiscal_years || []).slice(-1)[0] || "")}</span>
    </summary>
    <div class="evidence-detail"><section class="evidence-section">
      <div class="evidence-row"><strong>Evidence coverage</strong>
        ${escapeHtml(result.coverage?.direct_award_recipient_sites || 0)} direct award recipient sites ·
        ${escapeHtml(result.coverage?.reported_supplier_sites || 0)} reported supplier sites ·
        ${escapeHtml(result.coverage?.associated_niins || 0)} associated NIINs
      </div>
    </section></div>`;
  evidenceList.appendChild(summary);

  const suppliers = result.reported_supplier_sites || [];
  if (suppliers.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary><span class="tool-name">Reported supplier sites</span><span class="tool-meta">${suppliers.length} shown</span></summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${suppliers.slice(0, 18).map((site) => `<div class="evidence-row">
          <strong>${dashboardLink(`${site.supplier_name || site.cage} · CAGE ${site.cage}`, "COMPANY", "cage", site.cage)}</strong>
          ${escapeHtml([site.city, site.state].filter(Boolean).join(", "))}<br />
          ${formatMoney(site.mimir_modelled_reported_subcontract_value_usd)} Mimir-modelled reported subcontract value<br />
          ${(site.sample_prime_contract_ids || []).slice(0, 3).map((id) => dashboardLink(`Award ${id}`, "AWARDS", "award", id, "evidence-tag")).join("")}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  const awards = result.top_prime_awards || [];
  if (awards.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary><span class="tool-name">Prime award evidence</span><span class="tool-meta">${awards.length} shown</span></summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${awards.slice(0, 12).map((award) => `<div class="evidence-row">
          <strong>${dashboardLink(award.contract_id || "Award", "AWARDS", "award", award.contract_id || "")}</strong>
          ${escapeHtml(award.base_award_description || "Description not reported")}<br />
          ${formatMoney(award.net_prime_obligations_usd)} net prime obligations
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }
}

function renderItemEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }

  const identity = result.identity || {};
  const suppliers = result.supplier_summary || [];
  const contracts = result.contracts || [];
  const details = document.createElement("details");
  details.className = "evidence-card";
  details.innerHTML = `
    <summary>
      <span class="tool-name">Item evidence scope</span>
      <span class="tool-meta">NSN ${escapeHtml(identity.nsn || "not available")} · NIIN ${escapeHtml(identity.niin || "not available")}</span>
    </summary>
    <div class="evidence-detail"><section class="evidence-section">
      <div class="evidence-row">
        <strong>${escapeHtml(identity.description || "Item description unavailable")}</strong>
        ${escapeHtml(result.observation_window?.label || "Completed fiscal years")} · ${suppliers.length} supplier/reference sites · ${contracts.length} contract records
      </div>
    </section></div>`;
  evidenceList.appendChild(details);

  if (suppliers.length) {
    const supplierDetails = document.createElement("details");
    supplierDetails.className = "evidence-card";
    supplierDetails.innerHTML = `
      <summary>
        <span class="tool-name">Supplier and source evidence</span>
        <span class="tool-meta">Authorization and observed procurement kept separate</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${suppliers.slice(0, 16).map((supplier) => `<div class="evidence-row">
          <strong>${dashboardLink(`${supplier.vendor_name || supplier.cage} · CAGE ${supplier.cage}`, "COMPANY", "cage", supplier.cage)}</strong>
          ${escapeHtml([supplier.city, supplier.state].filter(Boolean).join(", ") || "Location not shown")}<br />
          ${supplier.is_active_authorized_source ? '<span class="evidence-tag">Active authorized source</span>' : ""}
          ${supplier.has_observed_dla_procurement ? `<span class="evidence-tag">Observed DLA procurement · ${formatMoney(supplier.net_dla_procurement_value_usd)}</span>` : ""}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(supplierDetails);
  }
}

function renderAwardOpportunityEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const identity = result.identity || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }

  const isContract = identity.record_type === "contract";
  const scope = document.createElement("details");
  scope.className = "evidence-card";
  const publicIdentifier = identity.contract_id || identity.solicitation_number || identity.opportunity_id;
  const title = identity.base_award_description || identity.title || "Public record";
  const sourceLink = identity.public_notice_url
    ? `<br /><a href="${escapeHtml(identity.public_notice_url)}" target="_blank" rel="noopener noreferrer">Open SAM.gov notice</a>`
    : "";
  scope.innerHTML = `
    <summary>
      <span class="tool-name">${escapeHtml(isContract ? "Contract evidence scope" : "Opportunity evidence scope")}</span>
      <span class="tool-meta">${escapeHtml(publicIdentifier || "Identifier unavailable")}</span>
    </summary>
    <div class="evidence-detail"><section class="evidence-section">
      <div class="evidence-row"><strong>${escapeHtml(title)}</strong>
        ${escapeHtml(identity.recipient_name || identity.sub_agency || identity.agency || "")}
        ${sourceLink}
      </div>
    </section></div>`;
  evidenceList.appendChild(scope);

  const suppliers = isContract
    ? result.reported_subaward_suppliers || []
    : result.likely_competitors || [];
  if (suppliers.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">${isContract ? "Reported supplier evidence" : "Historically relevant suppliers"}</span>
        <span class="tool-meta">${suppliers.length} records in the answer context</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${suppliers.slice(0, 16).map((supplier) => {
          const cage = supplier.supplier_cage || supplier.cage;
          const name = supplier.supplier_name || cage || "Supplier";
          const reasons = (supplier.relevance_reasons || []).join("; ");
          const contracts = (supplier.sample_contract_ids || []).slice(0, 3).map(
            (id) => dashboardLink(`Award ${id}`, "AWARDS", "award", id, "evidence-tag"),
          ).join("");
          return `<div class="evidence-row">
            <strong>${cage ? dashboardLink(`${name} · CAGE ${cage}`, "COMPANY", "cage", cage) : escapeHtml(name)}</strong>
            ${escapeHtml([supplier.city, supplier.state].filter(Boolean).join(", ") || "Location not shown")}<br />
            ${isContract ? `${formatMoney(supplier.mimir_modelled_reported_subcontract_value_usd)} Mimir-modelled reported subcontract value` : escapeHtml(reasons || "See comparable award evidence")}
            ${contracts ? `<br />${contracts}` : ""}
          </div>`;
        }).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }

  const awards = isContract ? result.action_history || [] : result.related_historical_awards || [];
  if (awards.length) {
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary>
        <span class="tool-name">${isContract ? "Contract action history" : "Related historical awards"}</span>
        <span class="tool-meta">${awards.length} records in the answer context</span>
      </summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${awards.slice(0, 16).map((award) => {
          const id = award.contract_id || identity.contract_id;
          const value = award.prime_obligation_usd ?? award.completed_year_prime_obligations_usd;
          return `<div class="evidence-row">
            <strong>${dashboardLink(id || "Award", "AWARDS", "award", id || "")}</strong>
            ${escapeHtml(award.action_description || award.base_award_description || "Description not reported")}<br />
            ${formatMoney(value)}${award.action_date ? ` · ${escapeHtml(award.action_date)}` : ""}
          </div>`;
        }).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  }
}

function renderCompetitivePositionEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }
  const lanes = [
    ["Reported supply-chain position", result.reported_supply_chain_position || [], "mimir_modelled_reported_subcontract_value_usd"],
    ["Observed DLA item procurement", result.dla_item_procurement_position || [], "attributed_dla_procurement_value_usd"],
    ["Direct power-related awards", result.direct_award_position || [], "net_prime_obligations_usd"],
  ];
  lanes.forEach(([label, rows, valueField]) => {
    if (!rows.length) return;
    const details = document.createElement("details");
    details.className = "evidence-card";
    details.innerHTML = `
      <summary><span class="tool-name">${escapeHtml(label)}</span><span class="tool-meta">Top ${Math.min(rows.length, 10)} CAGE sites</span></summary>
      <div class="evidence-detail"><section class="evidence-section">
        ${rows.slice(0, 10).map((row) => `<div class="evidence-row">
          <strong>${dashboardLink(`${row.rank}. ${row.supplier_name || row.cage} · CAGE ${row.cage}`, "COMPANY", "cage", row.cage)}</strong>
          ${escapeHtml([row.city, row.state].filter(Boolean).join(", ") || "Location not shown")} · ${escapeHtml((row.platforms || []).join(" · "))}<br />
          ${formatMoney(row[valueField])} in this evidence lane · score ${escapeHtml(row.observed_position_score)}<br />
          ${escapeHtml((row.evidence_descriptions || row.item_descriptions || []).slice(0, 3).join(" · "))}
        </div>`).join("")}
      </section></div>`;
    evidenceList.appendChild(details);
  });
}

function renderCompetitorDiscoveryEvidence(entry, artifacts = {}) {
  const result = entry.result || {};
  const pack = artifacts.evidence_pack || {};
  if (pack.download_url) {
    const download = document.createElement("a");
    download.className = "evidence-download";
    download.href = pack.download_url;
    download.textContent = "Download CSV evidence pack";
    evidenceList.appendChild(download);
  }
  const peers = result.observed_peers || [];
  if (!peers.length) return;
  const details = document.createElement("details");
  details.className = "evidence-card";
  details.innerHTML = `
    <summary><span class="tool-name">Observed peer evidence</span><span class="tool-meta">Top ${Math.min(peers.length, 15)} CAGE sites</span></summary>
    <div class="evidence-detail"><section class="evidence-section">
      ${peers.slice(0, 15).map((row) => `<div class="evidence-row">
        <strong>${dashboardLink(`${row.rank}. ${row.supplier_name} · CAGE ${row.cage}`, "COMPANY", "cage", row.cage)}</strong>
        ${escapeHtml([row.city, row.state].filter(Boolean).join(", ") || "Location not shown")} · score ${escapeHtml(row.observed_competitor_score)}<br />
        ${escapeHtml(`${row.shared_exact_niin_count} exact NIINs · ${row.shared_active_authorized_niin_count} active-authorized overlaps · ${row.shared_observed_procurement_niin_count} observed-procurement overlaps`)}<br />
        ${escapeHtml((row.overlapping_capability_groups || []).join(" · "))}
      </div>`).join("")}
    </section></div>`;
  evidenceList.appendChild(details);
}

function renderEvidence(trace, answerText = "", artifacts = {}) {
  evidenceList.innerHTML = "";
  evidenceCount.textContent = `${trace.length} ${trace.length === 1 ? "call" : "calls"}`;
  const evidencePack = artifacts.evidence_pack || {};
  if (evidencePack.locked) {
    const gate = document.createElement("section");
    gate.className = "evidence-pack-gate";
    gate.innerHTML = `
      <div>
        <strong>Download the supporting evidence</strong>
        <span>Professional includes the customer-ready CSV evidence pack used for this answer.</span>
      </div>
      <a href="${escapeHtml(evidencePack.upgrade_url || "https://www.mimiradvisors.org/dashboard?upgrade=professional")}">View Professional</a>`;
    evidenceList.appendChild(gate);
  }
  if (!trace.length) {
    evidenceList.insertAdjacentHTML("beforeend", '<div class="evidence-placeholder">No evidence tool was used. Treat this response as unsupported.</div>');
    return;
  }
  trace.forEach((entry, index) => {
    if (entry.tool === "get_company_opportunity_candidates" && entry.result) {
      renderOpportunityEvidence(entry, answerText);
      return;
    }
    if (entry.tool === "get_platform_supply_chain" && entry.result) {
      if (artifacts.platform_dossier) return;
      renderPlatformSupplyChainEvidence(entry);
      return;
    }
    if (entry.tool === "get_platform_context" && entry.result) {
      renderUniversalPlatformEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "get_company_context" && entry.result) {
      renderCompanyEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "get_item_context" && entry.result) {
      renderItemEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "get_award_opportunity_context" && entry.result) {
      renderAwardOpportunityEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "get_competitive_position" && entry.result) {
      renderCompetitivePositionEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "get_competitor_discovery" && entry.result) {
      renderCompetitorDiscoveryEvidence(entry, artifacts);
      return;
    }
    if (entry.tool === "search_award_opportunity_contexts" && entry.result && (artifacts.contract_dossier || artifacts.opportunity_dossier)) {
      return;
    }
    if (entry.tool === "search_item_contexts" && entry.result && artifacts.item_dossier) {
      return;
    }
    if (entry.tool === "search_platform_contexts" && entry.result && artifacts.platform_dossier) {
      return;
    }
    const details = document.createElement("details");
    details.className = "evidence-card";
    const args = Object.entries(entry.arguments || {}).map(([key, value]) => `${key}: ${value}`).join(" · ");
    details.innerHTML = `
      <summary>
        <span class="tool-name">${index + 1}. ${escapeHtml(entry.tool || "Tool error")}</span>
        <span class="tool-meta">${escapeHtml(args)}</span>
      </summary>
      <div class="evidence-detail"><pre>${escapeHtml(JSON.stringify(compactResult(entry), null, 2))}</pre></div>`;
    evidenceList.appendChild(details);
  });
  wireEvidenceDownloads();
}

function wireEvidenceDownloads() {
  evidenceList.querySelectorAll("a.evidence-download").forEach((link) => {
    if (link.dataset.wired === "true") return;
    link.dataset.wired = "true";
    link.addEventListener("click", async (event) => {
      event.preventDefault();
      const response = await fetch(link.href, { headers: betaHeaders() });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        window.alert(payload.detail || "The evidence pack could not be downloaded.");
        return;
      }
      const blob = await response.blob();
      const disposition = response.headers.get("content-disposition") || "";
      const match = disposition.match(/filename="?([^";]+)"?/i);
      const temporary = document.createElement("a");
      temporary.href = URL.createObjectURL(blob);
      temporary.download = match?.[1] || "mimir-evidence.zip";
      temporary.click();
      URL.revokeObjectURL(temporary.href);
    });
  });
}

async function submitQuestion(text) {
  const clean = text.trim();
  if (!clean || state.busy) return;
  state.busy = true;
  state.messages.push({ role: "user", content: clean });
  addMessage("user", clean);
  question.value = "";
  startThinking(clean);
  sendButton.disabled = true;
  try {
    const response = await fetch("/api/ask/jobs", {
      method: "POST",
      headers: betaHeaders(),
      body: JSON.stringify({ messages: state.messages.slice(-12) }),
    });
    const payload = await response.json();
    if (!response.ok) {
      const detail = typeof payload.detail === "object" ? payload.detail.message : payload.detail;
      if (payload.detail?.access) updateAllowance(payload.detail.access);
      throw new Error(detail || "Ask Mimir could not complete the request.");
    }
    updateAllowance(payload.access);
    let job = payload;
    while (!["completed", "failed"].includes(job.status)) {
      updateThinking(job.stage, job.detail);
      await new Promise((resolve) => window.setTimeout(resolve, 700));
      const statusResponse = await fetch(`/api/ask/jobs/${encodeURIComponent(job.request_id)}`, {
        headers: betaHeaders(),
      });
      job = await statusResponse.json();
      if (!statusResponse.ok) throw new Error(job.detail || "The Ask Mimir job could not be read.");
    }
    updateThinking(job.stage, job.detail);
    if (job.status === "failed") throw new Error(job.error || job.detail);
    const result = job.result;
    updateAllowance(result.access);
    state.messages.push({ role: "assistant", content: result.answer });
    const article = addMessage(
      "assistant",
      result.answer,
      result.tool_trace || [],
      result.answer_artifacts || {},
    );
    addFeedbackControls(article, result);
    renderEvidence(
      result.tool_trace || [],
      result.answer,
      result.answer_artifacts || {},
    );
    releaseLabel.textContent = "Evidence release current";
  } catch (error) {
    const message = `I could not complete that request. ${error.message}`;
    state.messages.push({ role: "assistant", content: message });
    addMessage("assistant", message);
  } finally {
    stopThinking();
    sendButton.disabled = false;
    state.busy = false;
    question.focus();
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  submitQuestion(question.value);
});

question.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    form.requestSubmit();
  }
});

document.querySelectorAll("[data-template]").forEach((button) => {
  button.addEventListener("click", () => {
    question.value = button.dataset.template || "";
    question.placeholder = button.dataset.placeholder || question.placeholder;
    question.focus();
    question.setSelectionRange(question.value.length, question.value.length);
  });
});

fetch("/api/health")
  .then((response) => response.json())
  .then((health) => {
    releaseLabel.textContent = `FY${health.analysis_fy} evidence release`;
    if (health.mock_mode) releaseLabel.textContent += " · local evidence mock";
    else if (!health.openai_configured) releaseLabel.textContent += " · API key required";
    else if (!health.external_evidence_allowed) releaseLabel.textContent += " · outbound evidence locked";
  })
  .catch(() => { releaseLabel.textContent = "Metric release unavailable"; });

fetch("/api/beta/policy", { headers: betaHeaders() })
  .then((response) => response.json())
  .then((policy) => {
    updateAllowance(policy.current_access);
    if (policy.test_identities_enabled) {
      testTierControl.hidden = false;
      testTier.value = state.tier;
    }
  })
  .catch(() => { allowanceStatus.textContent = "Allowance unavailable"; });

testTier.addEventListener("change", async () => {
  state.tier = testTier.value;
  localStorage.setItem("askMimirTestTier", state.tier);
  const response = await fetch("/api/beta/policy", { headers: betaHeaders() });
  if (response.ok) {
    const policy = await response.json();
    updateAllowance(policy.current_access);
  }
});
