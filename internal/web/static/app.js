(() => {
  const refresh = document.querySelector('[data-refresh]');
  if (refresh) refresh.addEventListener('click', () => window.location.reload());
  document.querySelectorAll('[data-confirm]').forEach((control) => {
    control.addEventListener('click', (event) => {
      if (!window.confirm(control.dataset.confirm)) event.preventDefault();
    });
  });
  const copy = document.querySelector('[data-copy]');
  if (copy) copy.addEventListener('click', async () => {
    const secret = document.querySelector('[data-secret]');
    if (!secret || !navigator.clipboard) return;
    await navigator.clipboard.writeText(secret.textContent.trim());
    copy.textContent = 'Copied';
  });
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
})();
