/* main.js — CASDAM case study interactive layer */

(function () {
  "use strict";

  /* ─── Utility ────────────────────────────────────────────────────────── */

  function $(sel, ctx) {
    return (ctx || document).querySelector(sel);
  }
  function $$(sel, ctx) {
    return Array.from((ctx || document).querySelectorAll(sel));
  }

  /* ─── Hamburger nav ──────────────────────────────────────────────────── */

  const hamburger = $("#hamburger");
  const navMobile = $("#nav-mobile");

  if (hamburger && navMobile) {
    hamburger.addEventListener("click", function () {
      const open = navMobile.classList.toggle("open");
      hamburger.setAttribute("aria-expanded", open);
    });

    // Close on link click
    $$("a", navMobile).forEach(function (a) {
      a.addEventListener("click", function () {
        navMobile.classList.remove("open");
        hamburger.setAttribute("aria-expanded", "false");
      });
    });
  }

  /* ─── Nav active section tracking ───────────────────────────────────── */

  const sections = $$("section[id], div[id='hero']");
  const navLinks = $$(".nav-links a");

  if (sections.length && navLinks.length) {
    const sectionObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            const id = entry.target.id;
            navLinks.forEach(function (a) {
              a.classList.toggle(
                "active",
                a.getAttribute("href") === "#" + id
              );
            });
          }
        });
      },
      { rootMargin: "-40% 0px -55% 0px", threshold: 0 }
    );
    sections.forEach(function (s) {
      sectionObserver.observe(s);
    });
  }

  /* ─── Fade-up entrance animations ───────────────────────────────────── */

  const fadeEls = $$(".fade-up");
  if (fadeEls.length) {
    const fadeObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add("visible");
            fadeObserver.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.1 }
    );
    fadeEls.forEach(function (el) {
      fadeObserver.observe(el);
    });
  }

  /* ─── Expandable cards (data-expand) ────────────────────────────────── */

  $$("[data-expand]").forEach(function (card) {
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "button");
    card.setAttribute("aria-expanded", card.classList.contains("open") ? "true" : "false");

    function toggle() {
      const open = card.classList.toggle("open");
      card.setAttribute("aria-expanded", open);
    }

    card.addEventListener("click", toggle);
    card.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggle();
      }
    });
  });

  /* ─── Theorist cards (data-theorist) ────────────────────────────────── */

  $$("[data-theorist]").forEach(function (card) {
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "button");
    card.setAttribute("aria-expanded", "false");

    function toggle() {
      const open = card.classList.toggle("open");
      card.setAttribute("aria-expanded", open);
    }

    card.addEventListener("click", toggle);
    card.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggle();
      }
    });
  });

  /* ─── 7-layer framework diagram ─────────────────────────────────────── */

  const layerBars = $$(".layer-bar[data-layer]");
  const layerPanels = $$(".layer-panel[data-panel]");

  function activateLayer(num) {
    layerBars.forEach(function (bar) {
      bar.classList.toggle("active", bar.dataset.layer === num);
    });
    layerPanels.forEach(function (panel) {
      panel.classList.toggle("active", panel.dataset.panel === num);
    });
  }

  if (layerBars.length) {
    // Activate layer 1 by default
    activateLayer("1");

    layerBars.forEach(function (bar) {
      bar.setAttribute("tabindex", "0");
      bar.setAttribute("role", "tab");

      bar.addEventListener("click", function () {
        activateLayer(bar.dataset.layer);
      });
      bar.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          activateLayer(bar.dataset.layer);
        }
      });
    });
  }

  /* ─── Stage stepper ──────────────────────────────────────────────────── */

  const stageTabs = $$(".stage-tab[data-stage]");
  const stagePanels = $$(".stage-panel[data-panel]");

  function activateStage(num) {
    stageTabs.forEach(function (tab) {
      tab.classList.toggle("active", tab.dataset.stage === num);
      tab.setAttribute("aria-selected", tab.dataset.stage === num);
    });
    stagePanels.forEach(function (panel) {
      panel.classList.toggle("active", panel.dataset.panel === num);
    });
  }

  if (stageTabs.length) {
    activateStage("1");

    stageTabs.forEach(function (tab) {
      tab.setAttribute("tabindex", "0");
      tab.setAttribute("role", "tab");

      tab.addEventListener("click", function () {
        activateStage(tab.dataset.stage);
      });
      tab.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          activateStage(tab.dataset.stage);
        }
        // Arrow key navigation
        if (e.key === "ArrowRight" || e.key === "ArrowDown") {
          e.preventDefault();
          const cur = parseInt(tab.dataset.stage, 10);
          const next = cur < stageTabs.length ? cur + 1 : 1;
          activateStage(String(next));
          const nextTab = $(".stage-tab[data-stage='" + next + "']");
          if (nextTab) nextTab.focus();
        }
        if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
          e.preventDefault();
          const cur = parseInt(tab.dataset.stage, 10);
          const prev = cur > 1 ? cur - 1 : stageTabs.length;
          activateStage(String(prev));
          const prevTab = $(".stage-tab[data-stage='" + prev + "']");
          if (prevTab) prevTab.focus();
        }
      });
    });
  }

  /* ─── Number expand toggles (data-num-expand) ────────────────────────── */

  $$("[data-num-expand]").forEach(function (trigger) {
    trigger.setAttribute("tabindex", "0");
    trigger.setAttribute("role", "button");

    const targetId = trigger.dataset.numExpand;
    const body = (targetId ? $("#" + targetId) : null) || trigger.nextElementSibling;

    function toggle() {
      if (!body) return;
      const open = body.classList.toggle("open");
      trigger.setAttribute("aria-expanded", open);
    }

    trigger.addEventListener("click", toggle);
    trigger.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        toggle();
      }
    });
  });

  /* ─── Counting number animation ─────────────────────────────────────── */

  function easeOutCubic(t) {
    return 1 - Math.pow(1 - t, 3);
  }

  function animateCount(el) {
    const target = parseFloat(el.dataset.count);
    const prefix = el.dataset.prefix || "";
    const suffix = el.dataset.suffix || "";
    const decimals = parseInt(el.dataset.decimals || "0", 10);
    const duration = 1800;
    const start = performance.now();

    function format(val) {
      return prefix + val.toFixed(decimals) + suffix;
    }

    el.textContent = format(0);

    function tick(now) {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      const value = easeOutCubic(progress) * target;
      el.textContent = format(value);
      if (progress < 1) {
        requestAnimationFrame(tick);
      } else {
        el.textContent = format(target);
      }
    }

    requestAnimationFrame(tick);
  }

  const countEls = $$("[data-count]");
  if (countEls.length) {
    const countObserver = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            animateCount(entry.target);
            countObserver.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.5 }
    );
    countEls.forEach(function (el) {
      countObserver.observe(el);
    });
  }

  /* ─── Nested MVS diagram ─────────────────────────────────────────────── */

  const mvsContainer = $("#mvs-svg");
  if (mvsContainer) {
    buildMvsDiagram(mvsContainer);
  }

  function buildMvsDiagram(container) {
    const svg = container;
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", "Nested Minimum Viable System diagram: three governance tiers");

    const C = {
      teal:        "#1ABC9C",
      tealGlow:    "rgba(26,188,156,0.55)",
      tealFill:    "#0c1c17",
      tealFillHi:  "#0f2a1e",
      tealStroke:  "rgba(26,188,156,0.6)",
      amber:       "#F39C12",
      amberGlow:   "rgba(243,156,18,0.55)",
      amberFill:   "#17140a",
      amberFillHi: "#221c0d",
      amberStroke: "rgba(243,156,18,0.6)",
      bg2:         "#161B22",
      bg3:         "#1C2128",
      text:        "#E8EDF2",
      text2:       "#8B9DB0",
      text3:       "#4A5568",
    };

    // viewBox 540×340
    const W = 540, H = 340;

    // Tier geometry — three cleanly nested boxes
    const tierDefs = [
      { badge: "L5",   name: "Meta-Governance",  sub: "Monitors the governance layer itself",           x: 10,  y: 10,  w: 520, h: 320, rx: 14, stroke: C.tealStroke,  fill: C.tealFill,  fillHi: C.tealFillHi,  col: C.teal  },
      { badge: "ORCH", name: "Orchestrator",      sub: "Gate logic · degradation routing · FactList",    x: 42,  y: 52,  w: 456, h: 236, rx: 10, stroke: C.amberStroke, fill: C.amberFill, fillHi: C.amberFillHi, col: C.amber },
      { badge: "CORE", name: "Pipeline Stages",   sub: "S1 → S2 → S3 → S4 → S5  +  S6 RAG",            x: 76,  y: 98,  w: 388, h: 148, rx: 8,  stroke: C.tealStroke,  fill: C.tealFill,  fillHi: C.tealFillHi,  col: C.teal  },
    ];

    // SVG defs: arrowhead + drop-shadow filter
    const defs = svgEl("defs");

    const filter = svgEl("filter");
    filter.setAttribute("id", "glow");
    filter.setAttribute("x", "-30%"); filter.setAttribute("y", "-30%");
    filter.setAttribute("width", "160%"); filter.setAttribute("height", "160%");
    const feGaussian = svgEl("feGaussianBlur");
    feGaussian.setAttribute("stdDeviation", "3");
    feGaussian.setAttribute("result", "blur");
    const feMerge = svgEl("feMerge");
    ["blur", "SourceGraphic"].forEach(function(n) {
      const node = svgEl("feMergeNode");
      if (n === "blur") node.setAttribute("in", "blur");
      else node.setAttribute("in", "SourceGraphic");
      feMerge.appendChild(node);
    });
    filter.appendChild(feGaussian);
    filter.appendChild(feMerge);
    defs.appendChild(filter);

    const marker = svgEl("marker");
    marker.setAttribute("id", "arr"); marker.setAttribute("markerWidth", "7");
    marker.setAttribute("markerHeight", "7"); marker.setAttribute("refX", "6");
    marker.setAttribute("refY", "3.5"); marker.setAttribute("orient", "auto");
    const arrPath = svgEl("path");
    arrPath.setAttribute("d", "M0,0.5 L0,6.5 L6,3.5 Z");
    arrPath.setAttribute("fill", C.tealStroke);
    marker.appendChild(arrPath);
    defs.appendChild(marker);
    svg.appendChild(defs);

    // Build tier groups
    const groups = [];
    tierDefs.forEach(function(td, i) {
      const g = svgEl("g");
      g.style.opacity = "0";
      g.style.transition = "opacity 0.55s ease";
      g.style.cursor = "pointer";

      // Box
      const rect = svgEl("rect");
      rect.setAttribute("x", td.x); rect.setAttribute("y", td.y);
      rect.setAttribute("width", td.w); rect.setAttribute("height", td.h);
      rect.setAttribute("rx", td.rx);
      rect.setAttribute("fill", td.fill);
      rect.setAttribute("stroke", td.stroke);
      rect.setAttribute("stroke-width", "1.5");
      rect.style.transition = "fill 0.3s, stroke 0.3s, filter 0.3s";
      g.appendChild(rect);

      // Badge pill: colored rounded rect + abbrev text
      const badgeW = td.badge.length * 9 + 16;
      const badgeX = td.x + 14;
      const badgeY = td.y + 14;
      const badgePill = svgEl("rect");
      badgePill.setAttribute("x", badgeX); badgePill.setAttribute("y", badgeY);
      badgePill.setAttribute("width", badgeW); badgePill.setAttribute("height", 22);
      badgePill.setAttribute("rx", 4);
      badgePill.setAttribute("fill", td.col);
      badgePill.setAttribute("opacity", "0.15");
      g.appendChild(badgePill);

      const badgeTxt = svgEl("text");
      badgeTxt.setAttribute("x", badgeX + badgeW / 2);
      badgeTxt.setAttribute("y", badgeY + 15);
      badgeTxt.setAttribute("text-anchor", "middle");
      badgeTxt.setAttribute("fill", td.col);
      badgeTxt.setAttribute("font-family", "'JetBrains Mono', monospace");
      badgeTxt.setAttribute("font-size", "10");
      badgeTxt.setAttribute("font-weight", "700");
      badgeTxt.setAttribute("letter-spacing", "0.8");
      badgeTxt.textContent = td.badge;
      g.appendChild(badgeTxt);

      // Tier name
      const nameEl = svgEl("text");
      nameEl.setAttribute("x", badgeX + badgeW + 10);
      nameEl.setAttribute("y", badgeY + 15);
      nameEl.setAttribute("fill", td.col);
      nameEl.setAttribute("font-family", "'Inter', sans-serif");
      nameEl.setAttribute("font-size", "12");
      nameEl.setAttribute("font-weight", "700");
      nameEl.setAttribute("letter-spacing", "0.2");
      nameEl.textContent = td.name;
      g.appendChild(nameEl);

      // Sub-label (only on outer two — inner has stage nodes)
      if (i < 2) {
        const subEl = svgEl("text");
        subEl.setAttribute("x", td.x + 14);
        subEl.setAttribute("y", td.y + 52);
        subEl.setAttribute("fill", C.text3);
        subEl.setAttribute("font-family", "'Inter', sans-serif");
        subEl.setAttribute("font-size", "9.5");
        subEl.textContent = td.sub;
        g.appendChild(subEl);
      }

      svg.appendChild(g);
      groups.push({ g, rect, td });
    });

    // Stage nodes inside Pipeline Core (tier index 2)
    const core = tierDefs[2];
    const nodeY = core.y + core.h / 2 + 6;
    const stageGroup = svgEl("g");
    stageGroup.style.opacity = "0";
    stageGroup.style.transition = "opacity 0.5s ease";

    const stages = [
      { id: "S1", sub: "Ingest" },
      { id: "S2", sub: "Recon"  },
      { id: "S3", sub: "KPI"    },
      { id: "S4", sub: "Insight"},
      { id: "S5", sub: "Report" },
    ];
    const nodeR = 20;
    const totalStageW = core.w - 80;
    const stageSpacing = totalStageW / (stages.length - 1);
    const startX = core.x + 40;

    stages.forEach(function(s, i) {
      const cx = startX + i * stageSpacing;

      if (i < stages.length - 1) {
        const line = svgEl("line");
        line.setAttribute("x1", cx + nodeR); line.setAttribute("y1", nodeY);
        line.setAttribute("x2", cx + stageSpacing - nodeR); line.setAttribute("y2", nodeY);
        line.setAttribute("stroke", C.tealStroke); line.setAttribute("stroke-width", "1");
        line.setAttribute("marker-end", "url(#arr)");
        stageGroup.appendChild(line);
      }

      const circ = svgEl("circle");
      circ.setAttribute("cx", cx); circ.setAttribute("cy", nodeY);
      circ.setAttribute("r", nodeR);
      circ.setAttribute("fill", C.bg3);
      circ.setAttribute("stroke", C.teal); circ.setAttribute("stroke-width", "1.5");
      stageGroup.appendChild(circ);

      const lbl = svgEl("text");
      lbl.setAttribute("x", cx); lbl.setAttribute("y", nodeY - 4);
      lbl.setAttribute("text-anchor", "middle");
      lbl.setAttribute("fill", C.teal);
      lbl.setAttribute("font-family", "'JetBrains Mono', monospace");
      lbl.setAttribute("font-size", "9"); lbl.setAttribute("font-weight", "700");
      lbl.textContent = s.id;
      stageGroup.appendChild(lbl);

      const sub = svgEl("text");
      sub.setAttribute("x", cx); sub.setAttribute("y", nodeY + 9);
      sub.setAttribute("text-anchor", "middle");
      sub.setAttribute("fill", C.text2);
      sub.setAttribute("font-family", "'Inter', sans-serif");
      sub.setAttribute("font-size", "7.5");
      sub.textContent = s.sub;
      stageGroup.appendChild(sub);
    });

    // S6 RAG — dashed amber box above the line
    const s6cx = core.x + core.w - 30;
    const s6cy = core.y + 36;
    const s6box = svgEl("rect");
    s6box.setAttribute("x", s6cx - 22); s6box.setAttribute("y", s6cy - 16);
    s6box.setAttribute("width", 44); s6box.setAttribute("height", 32);
    s6box.setAttribute("rx", 5);
    s6box.setAttribute("fill", C.bg3);
    s6box.setAttribute("stroke", C.amber); s6box.setAttribute("stroke-width", "1.5");
    s6box.setAttribute("stroke-dasharray", "4,2.5");
    stageGroup.appendChild(s6box);

    const s6lbl = svgEl("text");
    s6lbl.setAttribute("x", s6cx); s6lbl.setAttribute("y", s6cy - 2);
    s6lbl.setAttribute("text-anchor", "middle");
    s6lbl.setAttribute("fill", C.amber);
    s6lbl.setAttribute("font-family", "'JetBrains Mono', monospace");
    s6lbl.setAttribute("font-size", "9"); s6lbl.setAttribute("font-weight", "700");
    s6lbl.textContent = "S6";
    stageGroup.appendChild(s6lbl);

    const s6sub = svgEl("text");
    s6sub.setAttribute("x", s6cx); s6sub.setAttribute("y", s6cy + 10);
    s6sub.setAttribute("text-anchor", "middle");
    s6sub.setAttribute("fill", C.text2);
    s6sub.setAttribute("font-family", "'Inter', sans-serif");
    s6sub.setAttribute("font-size", "7");
    s6sub.textContent = "RAG";
    stageGroup.appendChild(s6sub);

    svg.appendChild(stageGroup);

    // Scroll-triggered reveal
    const mvsObserver = new IntersectionObserver(function(entries) {
      if (entries[0].isIntersecting) {
        setTimeout(function() { groups[0].g.style.opacity = "1"; }, 0);
        setTimeout(function() { groups[1].g.style.opacity = "1"; }, 350);
        setTimeout(function() { groups[2].g.style.opacity = "1"; }, 700);
        setTimeout(function() { stageGroup.style.opacity = "1"; }, 1050);
        mvsObserver.disconnect();
      }
    }, { threshold: 0.25 });
    mvsObserver.observe(container);

    // Info panel content
    const tierInfo = [
      {
        badge: "L5", col: C.teal,
        name: "Meta-Governance Monitor",
        vsm: "Beer's System 4: Intelligence",
        role: "Watches the governance layer itself, not individual outputs. Operates on run logs across time, looking for patterns invisible to single-run monitoring.",
        mechanism: "Layer5Monitor reads the N most recent run logs and emits structured alerts on rising retry rates, declining claim acceptance, and verifier agreement lock. Bounded authority: it recommends, it does not act.",
        failure: "Silent model drift, configuration degradation, and agreement lock — where two models converge on a shared blind spot and score 100% agreement for consecutive runs.",
      },
      {
        badge: "ORCH", col: C.amber,
        name: "Orchestrator",
        vsm: "Beer's System 3: Control",
        role: "Sequences stage execution, enforces gate logic, and routes degradation signals. Coordinates the interfaces between stages. It does not govern stage internals.",
        mechanism: "asyncio pipeline with typed contracts at every boundary. Each stage returns VerifiedOutput or DegradationSignal. The orchestrator decides what advances — stages have no visibility into each other.",
        failure: "Agents bypassing governance boundaries, ungoverned state transitions, and data corruption propagating silently between stages.",
      },
      {
        badge: "CORE", col: C.teal,
        name: "Pipeline Stages",
        vsm: "Beer's System 1: Operations",
        role: "Five self-governing stages plus the Stage 6 RAG advisor. Each stage is its own viable system: bounded scope, internal verification, and a typed output contract.",
        mechanism: "S1 ingests and normalises. S2 reconciles across sources. S3 computes KPIs in Python. S4 generates and verifies insights. S5 compiles the governed report. S6 adds RAG-sourced supply chain commentary with citation verification.",
        failure: "Unbounded agent scope, schema violations crossing stage boundaries, and unverified claims reaching the final report.",
      },
    ];

    const infoDefault = document.getElementById("mvs-info-default");
    const infoDetail  = document.getElementById("mvs-info-detail");
    let activeIdx = -1;

    function activateTier(i) {
      if (activeIdx === i) return;
      activeIdx = i;
      const td = tierInfo[i];

      // Dim non-selected tiers, highlight selected
      groups.forEach(function(obj, oi) {
        obj.rect.setAttribute("fill", oi === i ? obj.td.fillHi : obj.td.fill);
        obj.rect.setAttribute("stroke", oi === i ? obj.td.col : obj.td.stroke);
        obj.rect.style.filter = oi === i ? "url(#glow)" : "none";
      });

      // Build info panel
      infoDetail.innerHTML =
        '<div class="mvs-detail-badge" style="color:' + td.col + ';border-color:' + td.col + '20">' +
          '<span class="mvs-detail-abbr" style="background:' + td.col + '22;color:' + td.col + '">' + td.badge + '</span>' +
          '<span class="mvs-detail-name">' + td.name + '</span>' +
        '</div>' +
        '<div class="mvs-detail-vsm">' + td.vsm + '</div>' +
        '<div class="mvs-detail-row">' +
          '<div class="mvs-detail-label">Role</div>' +
          '<p class="mvs-detail-text">' + td.role + '</p>' +
        '</div>' +
        '<div class="mvs-detail-row mvs-detail-row--teal">' +
          '<div class="mvs-detail-label" style="color:' + td.col + '">In CASDAM</div>' +
          '<p class="mvs-detail-text">' + td.mechanism + '</p>' +
        '</div>' +
        '<div class="mvs-detail-row mvs-detail-row--red">' +
          '<div class="mvs-detail-label mvs-detail-label--red">Failure Prevented</div>' +
          '<p class="mvs-detail-text">' + td.failure + '</p>' +
        '</div>';

      // Swap panels
      if (infoDefault.style.display !== "none") {
        infoDefault.style.opacity = "0";
        setTimeout(function() {
          infoDefault.style.display = "none";
          infoDetail.style.display = "block";
          requestAnimationFrame(function() {
            infoDetail.style.opacity = "1";
          });
        }, 220);
      } else {
        infoDetail.style.opacity = "0";
        setTimeout(function() {
          infoDetail.style.opacity = "1";
        }, 50);
      }
    }

    groups.forEach(function(obj, i) {
      obj.g.addEventListener("click", function() { activateTier(i); });
      obj.g.addEventListener("mouseenter", function() {
        if (activeIdx !== i) {
          obj.rect.setAttribute("fill", obj.td.fillHi);
        }
      });
      obj.g.addEventListener("mouseleave", function() {
        if (activeIdx !== i) {
          obj.rect.setAttribute("fill", obj.td.fill);
        }
      });
    });

  }

  function svgEl(tag) {
    return document.createElementNS("http://www.w3.org/2000/svg", tag);
  }

  /* ─── Smooth scroll for anchor links ────────────────────────────────── */

  $$('a[href^="#"]').forEach(function (a) {
    a.addEventListener("click", function (e) {
      const id = a.getAttribute("href").slice(1);
      const target = document.getElementById(id);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: "smooth", block: "start" });
        // Update URL without jump
        history.pushState(null, "", "#" + id);
      }
    });
  });

  /* ─── Output section: screenshot lightbox ───────────────────────────── */

  const screenshotImgs = $$(".screenshot-card img");
  screenshotImgs.forEach(function (img) {
    img.style.cursor = "zoom-in";
    img.setAttribute("tabindex", "0");
    img.setAttribute("role", "button");
    img.setAttribute("aria-label", "View full size: " + (img.alt || "screenshot"));

    function openLightbox() {
      const overlay = document.createElement("div");
      overlay.style.cssText =
        "position:fixed;inset:0;background:rgba(7,13,26,0.92);z-index:9999;" +
        "display:flex;align-items:center;justify-content:center;cursor:zoom-out;padding:24px;";

      const clone = document.createElement("img");
      clone.src = img.src;
      clone.alt = img.alt;
      clone.style.cssText =
        "max-width:100%;max-height:100%;object-fit:contain;" +
        "border-radius:4px;box-shadow:0 0 60px rgba(0,0,0,0.8);";

      overlay.appendChild(clone);
      document.body.appendChild(overlay);
      document.body.style.overflow = "hidden";

      function close() {
        overlay.remove();
        document.body.style.overflow = "";
      }

      overlay.addEventListener("click", close);
      document.addEventListener(
        "keydown",
        function esc(e) {
          if (e.key === "Escape") {
            close();
            document.removeEventListener("keydown", esc);
          }
        }
      );
    }

    img.addEventListener("click", openLightbox);
    img.addEventListener("keydown", function (e) {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openLightbox();
      }
    });
  });

  /* ─── Hero pipeline: trigger stroke animation on load ───────────────── */

  // The CSS animation handles most of this; ensure the hero SVG is visible
  const heroPipeline = $("#hero-pipeline");
  if (heroPipeline) {
    heroPipeline.classList.add("ready");
  }

})();
