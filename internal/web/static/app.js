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
  if (document.body.hasAttribute('data-dashboard')) window.setTimeout(() => window.location.reload(), 15000);
})();
