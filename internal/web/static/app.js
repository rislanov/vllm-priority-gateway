(() => {
  const refresh = document.querySelector('[data-refresh]');
  if (refresh) refresh.addEventListener('click', () => window.location.reload());
  bindConfirmations(document);
  function bindConfirmations(root) {
    root.querySelectorAll('[data-confirm]').forEach((control) => {
      control.addEventListener('click', (event) => {
        if (!window.confirm(control.dataset.confirm)) event.preventDefault();
      });
    });
  }
  initKeyControls();
  window.addEventListener('pagehide', () => document.querySelector('#one-time-secret')?.remove());

  function initKeyControls() {
    const copy = document.querySelector('[data-copy]');
    if (copy) copy.addEventListener('click', async () => {
      const secret = document.querySelector('[data-secret]');
      const status = document.querySelector('[data-copy-status]');
      if (!secret || !secret.value || copy.disabled) return;
      copy.disabled = true;
      copy.textContent = 'Copy key';
      if (status) status.textContent = 'Copying…';
      let copied = false;
      try {
        if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
          await navigator.clipboard.writeText(secret.value);
          copied = true;
        }
      } catch {
        // A denied Clipboard API request can still allow copying selected text.
      }
      if (!copied) {
        secret.focus();
        secret.select();
        try {
          // Plain HTTP has no Clipboard API; use the browser's selected-text copy.
          copied = document.execCommand('copy');
        } catch {
          // Keep the complete key selected so the operator can copy it manually.
        }
      }
      copy.textContent = copied ? 'Copied' : 'Copy key';
      if (status) status.textContent = copied
        ? 'API key copied to clipboard.'
        : 'Automatic copying is unavailable. The key is selected; copy it manually.';
      copy.disabled = false;
    });
    const form = document.querySelector('[data-key-create]');
    if (form) {
      let submitting = false;
      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        if (submitting) return;
        submitting = true;
        const button = form.querySelector('[type="submit"]');
        const status = form.querySelector('[data-key-create-status]');
        button.disabled = true;
        status.textContent = 'Creating API key…';
        document.querySelector('#one-time-secret')?.remove();
        try {
          // Keep the page on a GET so refresh cannot resubmit a successful POST.
          // Never follow a redirect or retry an ambiguous creation automatically.
          const response = await fetch(form.getAttribute('action'), {
            method: 'POST', credentials: 'same-origin', cache: 'no-store', redirect: 'error',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            body: new URLSearchParams(new FormData(form)),
          });
          const result = new DOMParser().parseFromString(await response.text(), 'text/html');
          if (!response.ok) {
            status.textContent = result.querySelector('.notice.error')?.textContent
              || 'API key creation failed. Check the key list before trying again.';
            return;
          }
          const main = result.querySelector('main');
          const secret = main?.querySelector('[data-secret]');
          if (!secret || !/^llmgw_[A-Za-z0-9_-]{43}$/.test(secret.value)) throw new Error('Missing creation result');
          document.querySelector('main').replaceWith(main);
          document.querySelector('footer').replaceWith(result.querySelector('footer'));
          window.history.replaceState(null, '', form.getAttribute('action'));
          bindConfirmations(main);
          initKeyControls();
          main.querySelector('[data-secret]').focus();
        } catch {
          status.textContent = 'Unable to confirm API key creation. A key may have been created. Check the key list and revoke any key you cannot copy before trying again.';
        } finally {
          submitting = false;
          button.disabled = false;
        }
      });
      // Enable only after the fetch handler is installed; native submission
      // would leave a POST document that can be resubmitted on refresh.
      form.querySelector('[type="submit"]').disabled = false;
    }
  }
  if (document.body.hasAttribute('data-dashboard')) {
    const autoRefresh = () => {
      const focused = document.activeElement && document.activeElement !== document.body;
      if (document.visibilityState === 'visible' && !focused) {
        window.location.reload();
        return;
      }
      window.setTimeout(autoRefresh, 15000);
    };
    window.setTimeout(autoRefresh, 15000);
  }

  const analyticsTimeZone = initAnalyticsTime();

  function initAnalyticsTime() {
    const form = document.querySelector('.analytics-filter-grid');
    const time = window.LLMGWAnalyticsTime;
    if (!form || !time) return null;
    const changes = [];
    const bindings = [];
    let committed = false;
    function change(element, property, value) {
      if (!element) throw new Error('Missing analytics time control');
      changes.push({element, property, previous: element[property], value});
    }
    function bind(element, type, handler) {
      element.addEventListener(type, handler);
      bindings.push({element, type, handler});
    }
    try {
      const timeZone = new Intl.DateTimeFormat().resolvedOptions().timeZone;
      if (!timeZone) return null;
      const boundaries = ['from', 'to'].map((name) => {
        const input = form.querySelector(`#analytics-${name}`);
        const original = form.dataset[name === 'from' ? 'rangeFrom' : 'rangeTo'];
        const initial = time.toLocalInput(input.dataset.utcInput);
        const resolved = time.resolveBoundary(initial, initial, original);
        change(input, 'value', initial);
        const hint = form.querySelector(`[data-analytics-offset="${name}"]`);
        change(hint, 'textContent', resolved.offsetLabel);
        return {name, input, hint, initial, original};
      });
      document.querySelectorAll('[data-analytics-time]').forEach((element) => {
        change(element, 'textContent', time.formatTimestamp(element.getAttribute('datetime'), {timeZone, precision: 'millisecond'}));
      });
      document.querySelectorAll('[data-analytics-timezone]').forEach((element) => change(element, 'textContent', timeZone));
      change(form.querySelector('#analytics-range-hint'), 'textContent',
        'Times use your browser time zone. New or edited repeated times select the earlier occurrence; the effective UTC offset is shown beside each field. Times skipped by a clock change are invalid. Unchanged boundaries keep their exact precision. CSV exports use UTC.');
      const error = form.querySelector('[data-analytics-range-error]');
      if (!error) throw new Error('Missing analytics validation message');
      const resolve = (boundary) => time.resolveBoundary(boundary.input.value, boundary.initial, boundary.original);
      boundaries.forEach((boundary) => bind(boundary.input, 'input', () => {
        boundary.input.setCustomValidity('');
        const result = resolve(boundary);
        boundary.hint.textContent = result.error ? 'Invalid local time' : result.offsetLabel;
        error.hidden = true;
      }));
      bind(form, 'submit', (event) => {
        if (!committed) return;
        event.preventDefault();
        try {
          const target = new URL(form.action, window.location.href);
          target.search = '';
          for (const boundary of boundaries) {
            const result = resolve(boundary);
            boundary.input.setCustomValidity(result.error || '');
            if (result.error) {
              error.textContent = result.error;
              error.hidden = false;
              boundary.input.focus();
              boundary.input.reportValidity();
              return;
            }
            target.searchParams.set(boundary.name, result.canonical);
          }
          for (const name of ['client_id', 'model_pool_id', 'usage_available', 'limit']) {
            const value = form.elements.namedItem(name)?.value;
            if (value) target.searchParams.set(name, value);
          }
          // Ordering is validated by the server at full RFC3339Nano precision.
          window.location.assign(target.href);
        } catch {
          error.textContent = 'Unable to convert this range. Reload the page and try again.';
          error.hidden = false;
        }
      });
      // All formatting/conversion must succeed before changing the UTC fallback.
      changes.forEach(({element, property, value}) => { element[property] = value; });
      committed = true;
      return timeZone;
    } catch {
      bindings.forEach(({element, type, handler}) => element.removeEventListener(type, handler));
      changes.forEach(({element, property, previous}) => { element[property] = previous; });
      return null;
    }
  }

  const svgNamespace = 'http://www.w3.org/2000/svg';
  const palette = {grid: '#34475a', text: '#9aa8ba', request: '#ffb32c', input: '#38d6a2', output: '#b883f4', cache: '#78b8ff', ratio: '#ffb32c'};
  const chartContainers = [...document.querySelectorAll('[data-analytics-chart]')];
  if (chartContainers.length === 0) return;
  const series = [...document.querySelectorAll('[data-analytics-point]')].map((point) => ({
    bucketStart: point.dataset.bucketStart,
    requestCount: numeric(point.dataset.requestCount),
    inputTokens: numeric(point.dataset.inputTokens),
    outputTokens: numeric(point.dataset.outputTokens),
    cacheReadTokens: nullableDatasetNumber(point.dataset, 'cacheReadTokens'),
    cacheHitRatio: nullableDatasetNumber(point.dataset, 'cacheHitRatio'),
  }));
  chartContainers.forEach((container) => renderChart(container, series));

  function renderChart(container, series) {
    if (series.length === 0) {
      setChartMessage(container, 'No series data is available for this range.');
      return;
    }
    if (container.dataset.analyticsChart === 'requests') renderRequestChart(container, series);
    if (container.dataset.analyticsChart === 'input-tokens') renderTokenChart(container, series, 'inputTokens');
    if (container.dataset.analyticsChart === 'output-tokens') renderTokenChart(container, series, 'outputTokens');
    if (container.dataset.analyticsChart === 'cache') renderCacheChart(container, series);
  }

  function renderRequestChart(container, series) {
    const values = series.map((point) => numeric(point.requestCount));
    const frame = chartFrame(container, series, 280, 'Request volume chart', 'Requests per UTC bucket from a zero baseline.');
    drawGrid(frame, 48, 210, Math.max(1, ...values), (value) => compactNumber(value));
    drawSeries(frame, series, values, 48, 210, Math.max(1, ...values), palette.request, false, 'circle');
    drawTimeLabels(frame, series, 234);
    drawLegend(frame.svg, [{label: 'Requests', color: palette.request, marker: 'circle'}], 18);
  }

  function renderTokenChart(container, series, metric) {
    const values = series.map((point) => numeric(point[metric]));
    const maximum = Math.max(1, ...values);
    const output = metric === 'outputTokens';
    const label = output ? 'Output' : 'Input';
    const color = output ? palette.output : palette.input;
    const marker = output ? 'square' : 'circle';
    const frame = chartFrame(container, series, 280, `${label} token chart`, `${label} tokens per UTC bucket on an independent scale from a zero baseline. ${output ? 'Dashed line with squares.' : 'Solid line with circles.'}`);
    drawGrid(frame, 48, 210, maximum, (value) => compactNumber(value));
    drawSeries(frame, series, values, 48, 210, maximum, color, output, marker);
    drawTimeLabels(frame, series, 234);
    drawLegend(frame.svg, [{label, color, marker, dashed: output}], 18);
  }

  function renderCacheChart(container, series) {
    const cache = series.map((point) => nullableNumber(point.cacheReadTokens));
    const ratio = series.map((point) => nullableNumber(point.cacheHitRatio));
    if (!cache.some((value) => value !== null) && !ratio.some((value) => value !== null)) {
      setChartMessage(container, 'Cache usage is unavailable for every bucket in this range.');
      return;
    }
    const frame = chartFrame(container, series, 460, 'Cache-read tokens and cache-hit ratio charts', 'Two aligned plots for cache-known buckets. The top plot uses tokens; the bottom uses percent. Gaps represent unavailable cache data.');
    const cacheMaximum = Math.max(1, ...cache.filter((value) => value !== null));
    addText(frame.svg, 64, 36, 'Cache-read tokens — cache-known buckets', 'chart-subtitle');
    drawGrid(frame, 52, 184, cacheMaximum, (value) => compactNumber(value));
    drawSeries(frame, series, cache, 52, 184, cacheMaximum, palette.cache, false, 'circle');
    addText(frame.svg, 64, 236, 'Cache-hit ratio — cache-known buckets', 'chart-subtitle');
    drawGrid(frame, 252, 384, 1, (value) => `${Math.round(value * 100)}%`);
    drawSeries(frame, series, ratio, 252, 384, 1, palette.ratio, false, 'square');
    drawTimeLabels(frame, series, 410);
  }

  function chartFrame(container, series, height, titleText, descriptionText) {
    clearElement(container);
    const svg = svgElement('svg', {viewBox: `0 0 760 ${height}`, role: 'img', preserveAspectRatio: 'xMidYMid meet'});
    const sequence = document.querySelectorAll('svg.analytics-chart').length + 1;
    const titleID = `analytics-chart-title-${sequence}`;
    const descriptionID = `analytics-chart-description-${sequence}`;
    svg.setAttribute('class', 'analytics-chart');
    svg.setAttribute('aria-labelledby', `${titleID} ${descriptionID}`);
    const title = svgElement('title', {id: titleID});
    title.textContent = titleText;
    const description = svgElement('desc', {id: descriptionID});
    description.textContent = descriptionText;
    svg.append(title, description);
    container.append(svg);
    return {svg, series, left: 64, right: 742};
  }

  function drawGrid(frame, top, bottom, maximum, formatter) {
    for (let index = 0; index <= 4; index += 1) {
      const fraction = index / 4;
      const y = bottom - (bottom - top) * fraction;
      frame.svg.append(svgElement('line', {x1: frame.left, y1: y, x2: frame.right, y2: y, class: index === 0 ? 'chart-baseline' : 'chart-gridline'}));
      addText(frame.svg, frame.left - 9, y + 4, formatter(maximum * fraction), 'chart-axis-label chart-axis-label-y', 'end');
    }
  }

  function drawSeries(frame, series, values, top, bottom, maximum, color, dashed, marker) {
    const coordinates = values.map((value, index) => value === null ? null : {
      x: xPosition(series, index, frame.left, frame.right),
      y: bottom - (bottom - top) * Math.max(0, value) / maximum,
    });
    const pathData = coordinates.reduce((path, point, index) => {
      if (point === null) return path;
      const previousKnown = index > 0 && coordinates[index - 1] !== null;
      return `${path}${previousKnown ? ' L' : ' M'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`;
    }, '');
    const known = coordinates.filter((point) => point !== null);
    if (known.length > 1) {
      const path = svgElement('path', {d: pathData.trim(), fill: 'none', stroke: color, 'stroke-width': 3, 'vector-effect': 'non-scaling-stroke'});
      if (dashed) path.setAttribute('stroke-dasharray', '8 7');
      frame.svg.append(path);
    }
    known.forEach((point) => {
      if (marker === 'square') frame.svg.append(svgElement('rect', {x: point.x - 4, y: point.y - 4, width: 8, height: 8, fill: color}));
      else frame.svg.append(svgElement('circle', {cx: point.x, cy: point.y, r: 4.5, fill: color}));
    });
    if (known.length === 1) addText(frame.svg, known[0].x + 9, known[0].y - 9, 'One reported bucket', 'chart-sparse-label');
  }

  function drawTimeLabels(frame, series, y) {
    const draw = (point, x, anchor) => {
      let label = utcLabel(point.bucketStart);
      let accessible = label;
      if (analyticsTimeZone) {
        try {
          label = window.LLMGWAnalyticsTime.formatAxisTimestamp(point.bucketStart, analyticsTimeZone);
          accessible = window.LLMGWAnalyticsTime.formatTimestamp(point.bucketStart, {timeZone: analyticsTimeZone, precision: 'minute'});
        } catch { /* Keep an explicitly UTC label if formatting fails. */ }
      }
      const text = addText(frame.svg, x, y, label, 'chart-axis-label', anchor);
      text.setAttribute('aria-label', accessible);
      const title = svgElement('title', {});
      title.textContent = accessible;
      text.append(title);
    };
    draw(series[0], frame.left, 'start');
    if (series.length > 1) draw(series[series.length - 1], frame.right, 'end');
  }

  function drawLegend(svg, items, y) {
    let x = 520;
    items.forEach((item) => {
      const line = svgElement('line', {x1: x, y1: y, x2: x + 25, y2: y, stroke: item.color, 'stroke-width': 3});
      if (item.dashed) line.setAttribute('stroke-dasharray', '7 6');
      svg.append(line);
      if (item.marker === 'square') svg.append(svgElement('rect', {x: x + 9, y: y - 4, width: 8, height: 8, fill: item.color}));
      else svg.append(svgElement('circle', {cx: x + 13, cy: y, r: 4, fill: item.color}));
      addText(svg, x + 33, y + 4, item.label, 'chart-legend');
      x += 108;
    });
  }

  function xPosition(series, index, left, right) {
    if (series.length === 1) return (left + right) / 2;
    const first = Date.parse(series[0].bucketStart);
    const last = Date.parse(series[series.length - 1].bucketStart);
    const current = Date.parse(series[index].bucketStart);
    if (!Number.isFinite(first) || !Number.isFinite(last) || last === first) return left + (right - left) * index / (series.length - 1);
    return left + (right - left) * (current - first) / (last - first);
  }

  function svgElement(name, attributes) {
    const element = document.createElementNS(svgNamespace, name);
    Object.entries(attributes).forEach(([key, value]) => element.setAttribute(key, String(value)));
    return element;
  }

  function addText(svg, x, y, value, className, anchor) {
    const label = svgElement('text', {x, y, class: className});
    if (anchor) label.setAttribute('text-anchor', anchor);
    label.textContent = value;
    svg.append(label);
    return label;
  }

  function setChartMessage(container, message) {
    clearElement(container);
    const status = document.createElement('p');
    status.className = 'chart-message';
    status.textContent = message;
    container.append(status);
  }

  function clearElement(element) {
    while (element.firstChild) element.removeChild(element.firstChild);
  }

  function numeric(value) {
    return Number.isFinite(Number(value)) ? Number(value) : 0;
  }

  function nullableNumber(value) {
    if (value === null || value === undefined || !Number.isFinite(Number(value))) return null;
    return Number(value);
  }

  function nullableDatasetNumber(dataset, name) {
    if (!Object.prototype.hasOwnProperty.call(dataset, name)) return null;
    return nullableNumber(dataset[name]);
  }

  function compactNumber(value) {
    return new Intl.NumberFormat(undefined, {notation: 'compact', maximumFractionDigits: 1}).format(value);
  }

  function utcLabel(value) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'UTC';
    return date.toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  }
})();
